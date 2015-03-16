
package de.uniduesseldorf.dxram.core.lookup.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;


/**
 * Organizes OIDs in a table-based structure
 * @author Kevin Beineke
 *         24.07.2013
 */
public final class OIDTableTest {

	// Attributes
	private ArrayList<Entry> m_table;
	private short m_creator;

	private Entry m_changedEntry;

	// Constructors
	/**
	 * Creates an instance of OIDTable
	 * @param p_creator
	 *            the creator
	 */
	public OIDTableTest(final short p_creator) {
		m_table = new ArrayList<Entry>();
		m_table.add(new Entry((long)(Math.pow(2, 32) * 1000), p_creator));

		m_creator = p_creator;
	}

	/**
	 * Returns the tablelist
	 * @return the tablelist
	 */
	public ArrayList<Entry> getTable() {
		return m_table;
	}

	// Methods
	/**
	 * Get the NodeID of the node which hosts the object of the given ObjectID
	 * @param p_oid
	 *            the ObjectID
	 * @return the NodeID
	 */
	public short get(final long p_oid) {
		short ret;
		int index;
		Entry successor;

		if (0 == m_table.size()) {
			ret = m_creator;
		} else {
			index = Collections.binarySearch(m_table, new Entry(p_oid & 0x0000FFFFFFFFFFFFL, (short)-1));
			if (0 <= index) {
				ret = m_table.get(index).getNodeID();
			} else {
				index = (index * -1) - 1;
				successor = m_table.get(index);
				ret = successor.getNodeID();
			}
		}

		return ret;
	}

	/**
	 * Migrates an object
	 * @param p_oid
	 *            the ObjectID
	 * @param p_nid
	 *            the NodeID
	 */
	public void migrateObject(final long p_oid, final short p_nid) {
		long lid;
		int index;

		lid = p_oid & 0x0000FFFFFFFFFFFFL;

		index = createOrReplaceEntry(lid, p_nid);

		index = mergeWithPredecessorOrBound(index, lid, p_nid);

		mergeWithSuccessor(index, lid, p_nid);
	}

	/**
	 * Migrates a range
	 * @param p_start
	 *            the range start
	 * @param p_end
	 *            the range end
	 * @param p_nid
	 *            the NodeID
	 */
	public void migrateRange(final long p_start, final long p_end, final short p_nid) {
		long startLid;
		long endLid;
		int index;
		int endIndex;
		int count;

		startLid = p_start & 0x0000FFFFFFFFFFFFL;
		endLid = p_end & 0x0000FFFFFFFFFFFFL;

		index = createOrReplaceEntry(startLid, p_nid);

		index = mergeWithPredecessorOrBound(index, startLid, p_nid);

		endIndex = createOrReplaceEntry(endLid, p_nid);

		mergeWithSuccessor(endIndex, endLid, p_nid);

		// Remove entries between startValue and endValue
		count = index;
		while (count++ < endIndex) {
			m_table.remove(index);
		}
	}

	/**
	 * Creates/replaces an entry
	 * @param p_oid
	 *            the ObjectID
	 * @param p_nid
	 *            the NodeID
	 * @return the index of the entry
	 */
	private int createOrReplaceEntry(final long p_oid, final short p_nid) {
		int ret;
		int index;
		Entry newEntry;

		newEntry = new Entry(p_oid, p_nid);

		index = Collections.binarySearch(m_table, newEntry);
		if ((0 < m_table.size()) && (index < m_table.size()) && (0 <= index)) {
			// Replace entry
			m_changedEntry = m_table.get(index);
			m_table.set(index, newEntry);
			ret = index;
		} else {
			// Create new entry
			m_changedEntry = null;
			index = (index * -1) - 1;
			m_table.add(index, newEntry);
			ret = index;
		}

		return ret;
	}

	/**
	 * Merges the entry of the given index with its predecessor or bound
	 * @param p_index
	 *            the index of the entry
	 * @param p_oid
	 *            the ObjectID
	 * @param p_nid
	 *            the NodeID
	 * @return the index of the new entry
	 */
	private int mergeWithPredecessorOrBound(final int p_index, final long p_oid, final short p_nid) {
		int ret = p_index;
		Entry predecessor;
		Entry successor;

		if (0 < p_index) {
			predecessor = m_table.get(p_index - 1);
			if ((p_oid - 1) != predecessor.getLid()) {
				if ((p_index + 1) < m_table.size()) {
					successor = m_table.get(p_index + 1);
					if ((p_oid + 1) == successor.getLid()) {
						if (null == m_changedEntry) {
							if (p_nid != successor.getNodeID()) {
								createOrReplaceEntry(p_oid - 1, successor.getNodeID());
								ret++;
							} else {
								m_table.remove(p_index);
								ret--;
							}
						} else {
							createOrReplaceEntry(p_oid - 1, m_creator);
							ret++;
						}
					} else {
						if ((null != m_changedEntry) && ((p_oid - 1) != predecessor.getLid())) {
							if (p_nid != m_changedEntry.getNodeID()) {
								// New entry overwrote range ending
								createOrReplaceEntry(p_oid - 1, m_changedEntry.getNodeID());
								ret++;
							}
						} else {
							// Successor is end of range
							if (p_nid != successor.getNodeID()) {
								createOrReplaceEntry(p_oid - 1, successor.getNodeID());
								ret++;
							} else {
								// New Object is in range that already was migrated to the same destination
								m_table.remove(p_index);
							}
						}
					}
				}
			} else if (((p_oid - 1) == predecessor.getLid()) && (p_nid == predecessor.getNodeID())) {
				m_table.remove(p_index - 1);
				ret--;
			}
		} else {
			createOrReplaceEntry(p_oid - 1, m_creator);
			ret++;
		}

		return ret;
	}

	/**
	 * Merges the entry of the given index with its successor
	 * @param p_index
	 *            the index of the entry
	 * @param p_oid
	 *            the ObjectID
	 * @param p_nid
	 *            the NodeID
	 */
	private void mergeWithSuccessor(final int p_index, final long p_oid, final short p_nid) {
		Entry successor;

		if (p_index < (m_table.size() - 2)) {
			successor = m_table.get(p_index + 1);
			if (p_nid == successor.getNodeID()) {
				m_table.remove(p_index + 1);
			}
		}
	}

	@Override
	public String toString() {
		String ret;
		Entry entry;
		Iterator<Entry> iter;
		int i;

		ret = "Table: \n";
		i = 0;
		iter = m_table.iterator();
		while (iter.hasNext()) {
			entry = iter.next();
			ret +=
					i + ". [lid: " + entry.getLid() + ", nid: "	+ entry.getNodeID() + "]  ";
			if ((++i % 6) == 0) {
				ret += "\n";
			}
		}
		return ret;
	}

	/**
	 * Auxiliary object to store lid and NodeID
	 * @author Kevin Beineke
	 *         13.06.2013
	 */
	private static final class Entry implements Comparable<Entry> {

		// Attributes
		private long m_lid;
		private long m_nodeID;

		// Constructors
		/**
		 * Creates an instance of Entry
		 * @param p_lid
		 *            the lid
		 * @param p_nodeID
		 *            the NodeID
		 */
		public Entry(final long p_lid, final short p_nodeID) {
			m_lid = p_lid;
			m_nodeID = p_nodeID;
		}

		/**
		 * Compares two nodes
		 * @param p_cmp
		 *            the node to compare with
		 * @return 0 if the nodes are equal, (-1) if p_cmp is larger, 1 otherwise
		 */
		@Override
		public int compareTo(final Entry p_cmp) {
			int ret;

			if (this.getLid() < p_cmp.getLid()) {
				ret = -1;
			} else if (this.getLid() > p_cmp.getLid()) {
				ret = 1;
			} else {
				ret = 0;
			}

			return ret;
		}

		/**
		 * Returns the lid
		 * @return the lid
		 */
		public long getLid() {
			return m_lid;
		}

		/**
		 * Returns the location
		 * @return the location
		 */
		public short getNodeID() {
			return (short)m_nodeID;
		}

		/**
		 * Prints the entry
		 * @return String interpretation of the entry
		 */
		@Override
		public String toString() {
			return "(lid: " + m_lid + ", location: " + m_nodeID + ")";
		}
	}
}

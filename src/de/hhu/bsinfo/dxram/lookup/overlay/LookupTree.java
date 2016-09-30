
package de.hhu.bsinfo.dxram.lookup.overlay;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Btree to store ranges. Backup nodes are stored in an ArrayList<Long> to improve access times.
 *
 * @author Kevin Beineke
 *         13.06.2013
 */
public final class LookupTree implements Serializable, Importable, Exportable {

	private static final long serialVersionUID = -3992560499375457216L;

	// Attributes
	private short m_minEntries;
	private short m_minChildren;
	private short m_maxEntries;
	private short m_maxChildren;

	private Node m_root;
	private int m_size;
	private short m_creator;
	private short m_restorer;
	private boolean m_status;

	private ArrayList<long[]> m_backupRanges;
	private ArrayList<Long> m_migrationBackupRanges;

	private Entry m_changedEntry;

	// Constructors

	/**
	 * Creates an instance of LookupTree
	 *
	 * @param p_order order of the btree
	 */
	public LookupTree(final short p_order) {
		// too small order for BTree
		assert p_order > 1;

		m_minEntries = p_order;
		m_minChildren = (short) (m_minEntries + 1);
		m_maxEntries = (short) (2 * m_minEntries);
		m_maxChildren = (short) (m_maxEntries + 1);

		m_root = null;
		m_size = -1;
		m_creator = -1;
		m_restorer = -1;
		m_status = true;

		m_backupRanges = new ArrayList<long[]>();
		m_migrationBackupRanges = new ArrayList<Long>();

		m_changedEntry = null;
	}

	/**
	 * Reads an CIDTree from ByteBuffer
	 *
	 * @param p_buffer the ByteBuffer
	 * @return the CIDTree
	 */
	// TODO stefan: delete this when done implementing importer/exporter interface
	public static LookupTree readCIDTree(final ByteBuffer p_buffer) {
		LookupTree ret = null;
		byte[] data;

		if (p_buffer.get() != 0) {
			data = new byte[p_buffer.getInt()];
			p_buffer.get(data);
			ret = parseCIDTree(data);
		}

		return ret;
	}

	/**
	 * Parses binary data into an CIDTree
	 *
	 * @param p_data the binary data
	 * @return the CIDTree
	 */
	// TODO stefan: delete this when done implementing importer/exporter interface
	public static LookupTree parseCIDTree(final byte[] p_data) {
		LookupTree ret = null;
		ByteArrayInputStream byteArrayInputStream;
		ObjectInput objectInput = null;

		if (p_data != null && p_data.length > 0) {
			byteArrayInputStream = new ByteArrayInputStream(p_data);
			try {
				objectInput = new ObjectInputStream(byteArrayInputStream);
				ret = (LookupTree) objectInput.readObject();
			} catch (final Exception e) {
			} finally {
				try {
					if (objectInput != null) {
						objectInput.close();
					}
					byteArrayInputStream.close();
				} catch (final IOException e) {
				}
			}
		}

		return ret;
	}

	/**
	 * Writes an CIDTree
	 *
	 * @param p_buffer the buffer
	 * @param p_tree   the CIDTree
	 */
	// TODO stefan: delete this when done implementing importer/exporter interface
	public static void writeCIDTree(final ByteBuffer p_buffer, final LookupTree p_tree) {
		byte[] data;

		assert p_buffer != null;

		if (p_tree == null) {
			p_buffer.put((byte) 0);
		} else {
			data = parseCIDTree(p_tree);
			if (data == null) {
				p_buffer.put((byte) 0);
			} else {
				p_buffer.put((byte) 1);
			}
			if (data != null) {
				p_buffer.putInt(data.length);
				p_buffer.put(data);
			}
		}
	}

	/**
	 * Parses an CIDTree to a byte array
	 *
	 * @param p_tree the CIDTree
	 * @return the byte array
	 */
	// TODO stefan: delete this when done implementing importer/exporter interface
	private static byte[] parseCIDTree(final LookupTree p_tree) {
		byte[] ret = null;
		ByteArrayOutputStream byteArrayOutputStream;
		ObjectOutput objectOutput = null;

		byteArrayOutputStream = new ByteArrayOutputStream();
		try {
			objectOutput = new ObjectOutputStream(byteArrayOutputStream);
			objectOutput.writeObject(p_tree);
			ret = byteArrayOutputStream.toByteArray();
		} catch (final IOException e) {
		} finally {
			try {
				if (objectOutput != null) {
					objectOutput.close();
				}
				byteArrayOutputStream.close();
			} catch (final IOException e) {
			}
		}

		return ret;
	}

	/**
	 * Get the lenth of a CIDTree
	 *
	 * @param p_tree the CIDTree
	 * @return the lenght of the CIDTree
	 */
	// TODO stefan: delete this when done implementing importer/exporter interface
	public static int getCIDTreeWriteLength(final LookupTree p_tree) {
		int ret;
		byte[] data;

		if (p_tree == null) {
			ret = Byte.BYTES;
		} else {
			data = parseCIDTree(p_tree);
			ret = Byte.BYTES;
			if (data != null) {
				ret += Integer.BYTES + data.length;
			}
		}

		return ret;
	}

	@Override
	public void importObject(final Importer p_importer) {
		// TODO stefan: replace java serializable interface with importer/exporter interface
		throw new RuntimeException("Not implemented.");
	}

	@Override
	public void exportObject(final Exporter p_exporter) {
		// TODO stefan: replace java serializable interface with importer/exporter interface
		throw new RuntimeException("Not implemented.");
	}

	@Override
	public int sizeofObject() {
		// TODO stefan: replace java serializable interface with importer/exporter interface
		throw new RuntimeException("Not implemented.");
	}

	// Getters

	/**
	 * Returns the creator
	 *
	 * @return the creator
	 */
	public short getCreator() {
		return m_creator;
	}

	/**
	 * Get the status of this peer
	 *
	 * @return whether this node is online or not
	 */
	public boolean getStatus() {
		return m_status;
	}

	// Setters

	/**
	 * Set the status of this peer
	 *
	 * @param p_status whether this node is online or not
	 */
	public void setStatus(final boolean p_status) {
		m_status = p_status;
	}

	/**
	 * Set the restorer
	 *
	 * @param p_nodeID the NodeID of the peer that restored this peer
	 */
	public void setRestorer(final short p_nodeID) {
		m_restorer = p_nodeID;
	}

	// Methods

	/**
	 * Stores the migration for a single object
	 *
	 * @param p_chunkID ChunkID of migrated object
	 * @param p_nodeID  new primary peer
	 * @return true if insertion was successful
	 */
	public boolean migrateObject(final long p_chunkID, final short p_nodeID) {
		long localID;
		Node node;

		localID = p_chunkID & 0x0000FFFFFFFFFFFFL;

		assert localID >= 0;

		node = createOrReplaceEntry(localID, p_nodeID);

		mergeWithPredecessorOrBound(localID, p_nodeID, node);

		mergeWithSuccessor(localID, p_nodeID);

		return true;
	}

	/**
	 * Stores the migration for a range
	 *
	 * @param p_startCID ChunkID of first migrated object
	 * @param p_endCID   ChunkID of last migrated object
	 * @param p_nodeID   new primary peer
	 * @return true if insertion was successful
	 */
	public boolean migrateRange(final long p_startCID, final long p_endCID, final short p_nodeID) {
		long startLID;
		long endLID;
		Node startNode;

		startLID = p_startCID & 0x0000FFFFFFFFFFFFL;
		endLID = p_endCID & 0x0000FFFFFFFFFFFFL;
		// end larger than start or start smaller than 1
		assert startLID <= endLID && 0 < startLID;

		if (startLID == endLID) {
			migrateObject(p_startCID, p_nodeID);
		} else {
			startNode = createOrReplaceEntry(startLID, p_nodeID);

			mergeWithPredecessorOrBound(startLID, p_nodeID, startNode);

			createOrReplaceEntry(endLID, p_nodeID);

			removeEntriesWithinRange(startLID, endLID);

			mergeWithSuccessor(endLID, p_nodeID);
		}
		return true;
	}

	/**
	 * Initializes a range
	 *
	 * @param p_startID     ChunkID of first chunk
	 * @param p_creator     the creator
	 * @param p_backupPeers the backup peers
	 * @return true if insertion was successful
	 */
	public boolean initRange(final long p_startID, final short p_creator, final short[] p_backupPeers) {
		long backupPeers;

		if (0 == p_startID) {
			m_creator = p_creator;
		} else {
			if (null == m_root) {
				createOrReplaceEntry((long) Math.pow(2, 48), p_creator);
			}
			backupPeers = ((p_backupPeers[2] & 0x000000000000FFFFL) << 32)
					+ ((p_backupPeers[1] & 0x000000000000FFFFL) << 16) + (p_backupPeers[0] & 0x0000FFFF);
			m_backupRanges.add(new long[] {p_startID, backupPeers});
		}
		return true;
	}

	/**
	 * Initializes a range for migrated chunks
	 *
	 * @param p_rangeID     the RangeID
	 * @param p_backupPeers the backup peers
	 * @return true if insertion was successful
	 */
	public boolean initMigrationRange(final int p_rangeID, final short[] p_backupPeers) {

		m_migrationBackupRanges.add(p_rangeID,
				((p_backupPeers[2] & 0x000000000000FFFFL) << 32) + ((p_backupPeers[1] & 0x000000000000FFFFL) << 16)
						+ (p_backupPeers[0] & 0x0000FFFF));

		return true;
	}

	/**
	 * Returns the primary peer for given object
	 *
	 * @param p_chunkID ChunkID of requested object
	 * @return the NodeID of the primary peer for given object
	 */
	public short getPrimaryPeer(final long p_chunkID) {
		if (m_root != null) {
			return getNodeIDOrSuccessorsNodeID(p_chunkID & 0x0000FFFFFFFFFFFFL);
		} else {
			return -1;
		}
	}

	/**
	 * Returns the range given ChunkID is in
	 *
	 * @param p_chunkID ChunkID of requested object
	 * @return the first and last ChunkID of the range
	 */
	public LookupRange getMetadata(final long p_chunkID) {
		LookupRange ret = null;
		long[] range;
		short nodeID;
		int index;
		long localID;
		Node node;
		Entry predecessorEntry;

		if (m_root != null) {
			localID = p_chunkID & 0x0000FFFFFFFFFFFFL;
			node = getNodeOrSuccessorsNode(localID);
			if (node != null) {
				index = node.indexOf(localID);
				if (0 <= index) {
					// LocalID was found: Store NodeID and determine successor
					range = new long[2];
					nodeID = node.getNodeID(index);
					range[1] = localID;
					// range[1] = getSuccessorsEntry(localID, node).getLocalID();
				} else {
					// LocalID was not found, but successor: Store NodeID and LocalID of successor
					range = new long[2];
					nodeID = node.getNodeID(index * -1 - 1);
					range[1] = node.getLocalID(index * -1 - 1);
				}
				// Determine LocalID of predecessor
				predecessorEntry = getPredecessorsEntry(range[1], node);
				if (predecessorEntry != null) {
					range[0] = predecessorEntry.getLocalID() + 1;
				} else {
					range[0] = 0;
				}
				ret = new LookupRange(nodeID, range);
			}
		}

		return ret;
	}

	/**
	 * Returns all the backup peers for given object
	 *
	 * @param p_chunkID     ChunkID of requested object
	 * @param p_wasMigrated whether this Chunk was migrated or not
	 * @return the NodeIDs of all backup peers for given object
	 */
	public short[] getBackupPeers(final long p_chunkID, final boolean p_wasMigrated) {
		short[] ret = null;
		short backupPeer;
		Long tempResult = null;
		long result;

		if (m_root != null) {
			if (!p_wasMigrated) {
				for (int i = m_backupRanges.size() - 1; i >= 0; i--) {
					if (m_backupRanges.get(i)[0] <= p_chunkID) {
						tempResult = m_backupRanges.get(i)[1];
					}
				}

				ret = new short[] {-1, -1, -1};
				if (tempResult != null) {
					result = tempResult;
					for (int i = 0; i < ret.length; i++) {
						backupPeer = (short) (result >> i * 16);
						if (backupPeer != 0) {
							ret[i] = backupPeer;
						}
					}
				}
			} else {
				ret = new short[] {-1, -1, -1};
			}

		}
		return ret;
	}

	/**
	 * Removes given object from btree
	 *
	 * @param p_chunkID ChunkID of deleted object
	 * @note should always be called if an object is deleted
	 */
	public void removeObject(final long p_chunkID) {
		short creatorOrRestorer;
		int index;
		Node node;
		long localID;
		long currentLID;
		Entry currentEntry;
		Entry predecessor;
		Entry successor;

		localID = p_chunkID & 0x0000FFFFFFFFFFFFL;
		if (null != m_root) {
			if (m_restorer == -1) {
				creatorOrRestorer = m_creator;
			} else {
				creatorOrRestorer = m_restorer;
			}
			node = getNodeOrSuccessorsNode(localID);
			if (null != node) {
				currentLID = -1;

				index = node.indexOf(localID);
				if (0 <= index) {
					// Entry was found
					currentLID = node.getLocalID(index);
					predecessor = getPredecessorsEntry(localID, node);
					currentEntry = new Entry(currentLID, node.getNodeID(index));
					successor = getSuccessorsEntry(localID, node);
					if (creatorOrRestorer != currentEntry.getNodeID() && null != predecessor) {
						if (localID - 1 == predecessor.getLocalID()) {
							// Predecessor is direct neighbor: AB
							// Successor might be direct neighbor or not: ABC or AB___C
							if (creatorOrRestorer == successor.getNodeID()) {
								// Successor is barrier: ABC -> A_C or AB___C -> A___C
								remove(localID);
							} else {
								// Successor is no barrier: ABC -> AXC or AB___C -> AX___C
								node.changeEntry(localID, creatorOrRestorer, index);
							}
							if (creatorOrRestorer == predecessor.getNodeID()) {
								// Predecessor is barrier: A_C -> ___C or AXC -> ___XC
								// or A___C -> ___C or AX___C -> ___X___C
								remove(predecessor.getLocalID());
							}
						} else {
							// Predecessor is no direct neighbor: A___B
							if (creatorOrRestorer == successor.getNodeID()) {
								// Successor is barrier: A___BC -> A___C or A___B___C -> A___'___C
								remove(localID);
							} else {
								// Successor is no barrier: A___BC -> A___XC or A___B___C -> A___X___C
								node.changeEntry(localID, creatorOrRestorer, index);
							}
							// Predecessor is barrier: A___C -> A___(B-1)_C or A___XC -> ___(B-1)XC
							// or A___'___C -> A___(B-1)___C or A___X___C -> A___(B-1)X___C
							createOrReplaceEntry(localID - 1, currentEntry.getNodeID());
						}
					}
				} else {
					// Entry was not found
					index = index * -1 - 1;
					successor = new Entry(node.getLocalID(index), node.getNodeID(index));
					predecessor = getPredecessorsEntry(successor.getLocalID(), node);
					if (creatorOrRestorer != successor.getNodeID() && null != predecessor) {
						// Entry is in range
						if (localID - 1 == predecessor.getLocalID()) {
							// Predecessor is direct neighbor: A'B'
							// Successor might be direct neighbor or not: A'B'C -> AXC or A'B'___C -> AX___C
							createOrReplaceEntry(localID, creatorOrRestorer);
							if (creatorOrRestorer == predecessor.getNodeID()) {
								// Predecessor is barrier: AXC -> ___XC or AX___C -> ___X___C
								remove(localID - 1);
							}
						} else {
							// Predecessor is no direct neighbor: A___'B'
							// Successor might be direct neighbor or not: A___'B'C -> A___(B-1)XC
							// or A___'B'___C -> A___(B-1)X___C
							createOrReplaceEntry(localID, creatorOrRestorer);
							createOrReplaceEntry(localID - 1, successor.getNodeID());
						}
					}
				}
			}
		}
	}

	/**
	 * Returns the backup peers for every range
	 *
	 * @return an ArrayList with all backup peers
	 */
	public ArrayList<long[]> getAllBackupRanges() {
		return m_backupRanges;
	}

	/**
	 * Returns the backup peers for migrated chunks
	 *
	 * @return an ArrayList with all backup peers for migrated
	 */
	public ArrayList<Long> getAllMigratedBackupRanges() {
		return m_migrationBackupRanges;
	}

	/**
	 * Removes given peer from all backups
	 *
	 * @param p_failedPeer  NodeID of failed peer
	 * @param p_replacement NodeID of new backup peer
	 */
	public void removeBackupPeer(final short p_failedPeer, final short p_replacement) {
		long backupNodes;
		short[] backupPeers;
		long[] element;

		for (int i = 0; i < m_backupRanges.size(); i++) {
			element = m_backupRanges.get(i);
			backupNodes = element[1];
			backupPeers =
					new short[] {(short) backupNodes, (short) ((backupNodes & 0x00000000FFFF0000L) >> 16),
							(short) ((backupNodes & 0x0000FFFF00000000L) >> 32)};
			if (p_failedPeer == backupPeers[0]) {
				backupNodes = ((p_replacement & 0x000000000000FFFFL) << 32)
						+ ((backupPeers[2] & 0x000000000000FFFFL) << 16) + (backupPeers[1] & 0x0000FFFF);
				element[1] = backupNodes;
			} else if (p_failedPeer == backupPeers[1]) {
				backupNodes = ((p_replacement & 0x000000000000FFFFL) << 32)
						+ ((backupPeers[2] & 0x000000000000FFFFL) << 16) + (backupPeers[0] & 0x0000FFFF);
				element[1] = backupNodes;
			} else if (p_failedPeer == backupPeers[2]) {
				backupNodes = ((p_replacement & 0x000000000000FFFFL) << 32)
						+ ((backupPeers[1] & 0x000000000000FFFFL) << 16) + (backupPeers[0] & 0x0000FFFF);
				element[1] = backupNodes;
			}
		}
	}

	/**
	 * Creates a new entry or replaces the old one
	 *
	 * @param p_localID the LocalID
	 * @param p_nodeID  the NodeID
	 * @return the node in which the entry is stored
	 */
	private Node createOrReplaceEntry(final long p_localID, final short p_nodeID) {
		Node ret = null;
		Node node;
		int index;
		int size;

		if (null == m_root) {
			m_root = new Node(null, m_maxEntries, m_maxChildren);
			m_root.addEntry(p_localID, p_nodeID);
			ret = m_root;
		} else {
			node = m_root;
			while (true) {
				if (0 == node.getNumberOfChildren()) {
					index = node.indexOf(p_localID);
					if (0 <= index) {
						m_changedEntry = new Entry(node.getLocalID(index), node.getNodeID(index));
						node.changeEntry(p_localID, p_nodeID, index);
					} else {
						m_changedEntry = null;
						node.addEntry(p_localID, p_nodeID, index * -1 - 1);
						if (m_maxEntries < node.getNumberOfEntries()) {
							// Need to split up
							node = split(p_localID, node);
						}
					}
					break;
				} else {
					if (p_localID < node.getLocalID(0)) {
						node = node.getChild(0);
						continue;
					}

					size = node.getNumberOfEntries();
					if (p_localID > node.getLocalID(size - 1)) {
						node = node.getChild(size);
						continue;
					}

					index = node.indexOf(p_localID);
					if (0 <= index) {
						m_changedEntry = new Entry(node.getLocalID(index), node.getNodeID(index));
						node.changeEntry(p_localID, p_nodeID, index);
						break;
					} else {
						node = node.getChild(index * -1 - 1);
					}
				}
			}

			ret = node;
		}
		if (m_changedEntry == null) {
			m_size++;
		}

		return ret;
	}

	/**
	 * Merges the object or range with predecessor
	 *
	 * @param p_localID the LocalID
	 * @param p_nodeID  the NodeID
	 * @param p_node    anchor node
	 */
	private void mergeWithPredecessorOrBound(final long p_localID, final short p_nodeID, final Node p_node) {
		Entry predecessor;
		Entry successor;

		predecessor = getPredecessorsEntry(p_localID, p_node);
		if (null == predecessor) {
			createOrReplaceEntry(p_localID - 1, m_creator);
		} else {
			if (p_localID - 1 == predecessor.getLocalID()) {
				if (p_nodeID == predecessor.getNodeID()) {
					remove(predecessor.getLocalID(), getPredecessorsNode(p_localID, p_node));
				}
			} else {
				successor = getSuccessorsEntry(p_localID, p_node);
				if (null == m_changedEntry) {
					// Successor is end of range
					if (p_nodeID != successor.getNodeID()) {
						createOrReplaceEntry(p_localID - 1, successor.getNodeID());
					} else {
						// New Object is in range that already was migrated to the same destination
						remove(p_localID, p_node);
					}
				} else {
					if (p_nodeID != m_changedEntry.getNodeID()) {
						createOrReplaceEntry(p_localID - 1, m_changedEntry.getNodeID());
					}
				}
			}
		}
	}

	/**
	 * Merges the object or range with successor
	 *
	 * @param p_localID the LocalID
	 * @param p_nodeID  the NodeID
	 */
	private void mergeWithSuccessor(final long p_localID, final short p_nodeID) {
		Node node;
		Entry successor;

		node = getNodeOrSuccessorsNode(p_localID);
		successor = getSuccessorsEntry(p_localID, node);
		if (null != successor && p_nodeID == successor.getNodeID()) {
			remove(p_localID, node);
		}
	}

	/**
	 * Removes all entries between start (inclusive) and end
	 *
	 * @param p_start the first object in range
	 * @param p_end   the last object in range
	 */
	private void removeEntriesWithinRange(final long p_start, final long p_end) {
		long successor;

		remove(p_start, getNodeOrSuccessorsNode(p_start));

		successor = getLIDOrSuccessorsLID(p_start);
		while (-1 != successor && successor < p_end) {
			remove(successor);
			successor = getLIDOrSuccessorsLID(p_start);
		}
	}

	/**
	 * Returns the node in which the next entry to given LocalID (could be the LocalID itself) is stored
	 *
	 * @param p_localID LocalID whose node is searched
	 * @return node in which LocalID is stored if LocalID is in tree or successors node, null if there is no successor
	 */
	private Node getNodeOrSuccessorsNode(final long p_localID) {
		Node ret;
		int size;
		int index;
		long greater;

		ret = m_root;

		while (true) {
			if (p_localID < ret.getLocalID(0)) {
				if (0 < ret.getNumberOfChildren()) {
					ret = ret.getChild(0);
					continue;
				} else {
					break;
				}
			}

			size = ret.getNumberOfEntries();
			greater = ret.getLocalID(size - 1);
			if (p_localID > greater) {
				if (size < ret.getNumberOfChildren()) {
					ret = ret.getChild(size);
					continue;
				} else {
					ret = getSuccessorsNode(greater, ret);
					break;
				}
			}

			index = ret.indexOf(p_localID);
			if (0 <= index) {
				break;
			} else {
				index = index * -1 - 1;
				if (index < ret.getNumberOfChildren()) {
					ret = ret.getChild(index);
				} else {
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Returns next LocalID to given LocalID (could be the LocalID itself)
	 *
	 * @param p_localID the LocalID
	 * @return p_localID if p_localID is in btree or successor of p_localID, (-1) if there is no successor
	 */
	private long getLIDOrSuccessorsLID(final long p_localID) {
		long ret = -1;
		int index;
		Node node;

		node = getNodeOrSuccessorsNode(p_localID);
		if (node != null) {
			index = node.indexOf(p_localID);
			if (0 <= index) {
				ret = node.getLocalID(index);
			} else {
				ret = node.getLocalID(index * -1 - 1);
			}
		}

		return ret;
	}

	/**
	 * Returns the location and backup nodes of next LocalID to given LocalID (could be the LocalID itself)
	 *
	 * @param p_localID the LocalID whose corresponding NodeID is searched
	 * @return NodeID for p_localID if p_localID is in btree or successors NodeID
	 */
	private short getNodeIDOrSuccessorsNodeID(final long p_localID) {
		short ret = -1;
		int index;
		Node node;

		node = getNodeOrSuccessorsNode(p_localID);
		if (node != null) {
			index = node.indexOf(p_localID);
			if (0 <= index) {
				ret = node.getNodeID(index);
			} else {
				ret = node.getNodeID(index * -1 - 1);
			}
		}

		return ret;
	}

	/**
	 * Returns the node in which the predecessor is
	 *
	 * @param p_localID LocalID whose predecessor's node is searched
	 * @param p_node    anchor node
	 * @return the node in which the predecessor of p_localID is or null if there is no predecessor
	 */
	private Node getPredecessorsNode(final long p_localID, final Node p_node) {
		int index;
		Node ret = null;
		Node node;
		Node parent;

		assert p_node != null;

		node = p_node;

		if (p_localID == node.getLocalID(0)) {
			if (0 < node.getNumberOfChildren()) {
				// Get maximum in child tree
				node = node.getChild(0);
				while (node.getNumberOfEntries() < node.getNumberOfChildren()) {
					node = node.getChild(node.getNumberOfChildren() - 1);
				}
				ret = node;
			} else {
				parent = node.getParent();
				if (parent != null) {
					while (parent != null && p_localID < parent.getLocalID(0)) {
						parent = parent.getParent();
					}
					ret = parent;
				}
			}
		} else {
			index = node.indexOf(p_localID);
			if (0 <= index) {
				if (index <= node.getNumberOfChildren()) {
					// Get maximum in child tree
					node = node.getChild(index);
					while (node.getNumberOfEntries() < node.getNumberOfChildren()) {
						node = node.getChild(node.getNumberOfChildren() - 1);
					}
				}
				ret = node;
			}
		}

		return ret;
	}

	/**
	 * Returns the entry of the predecessor
	 *
	 * @param p_localID the LocalID whose predecessor is searched
	 * @param p_node    anchor node
	 * @return the entry of p_localID's predecessor or null if there is no predecessor
	 */
	private Entry getPredecessorsEntry(final long p_localID, final Node p_node) {
		Entry ret = null;
		Node predecessorsNode;
		long predecessorsLID;

		predecessorsNode = getPredecessorsNode(p_localID, p_node);
		if (predecessorsNode != null) {
			for (int i = predecessorsNode.getNumberOfEntries() - 1; i >= 0; i--) {
				predecessorsLID = predecessorsNode.getLocalID(i);
				if (p_localID > predecessorsLID) {
					ret = new Entry(predecessorsLID, predecessorsNode.getNodeID(i));
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Returns the node in which the successor is
	 *
	 * @param p_localID LocalID whose successor's node is searched
	 * @param p_node    anchor node
	 * @return the node in which the successor of p_localID is or null if there is no successor
	 */
	private Node getSuccessorsNode(final long p_localID, final Node p_node) {
		int index;
		Node ret = null;
		Node node;
		Node parent;

		assert p_node != null;

		node = p_node;

		if (p_localID == node.getLocalID(node.getNumberOfEntries() - 1)) {
			if (node.getNumberOfEntries() < node.getNumberOfChildren()) {
				// Get minimum in child tree
				node = node.getChild(node.getNumberOfEntries());
				while (0 < node.getNumberOfChildren()) {
					node = node.getChild(0);
				}
				ret = node;
			} else {
				parent = node.getParent();
				if (parent != null) {
					while (parent != null && p_localID > parent.getLocalID(parent.getNumberOfEntries() - 1)) {
						parent = parent.getParent();
					}
					ret = parent;
				}
			}
		} else {
			index = node.indexOf(p_localID);
			if (0 <= index) {
				if (index < node.getNumberOfChildren()) {
					// Get minimum in child tree
					node = node.getChild(index + 1);
					while (0 < node.getNumberOfChildren()) {
						node = node.getChild(0);
					}
				}
				ret = node;
			}
		}

		return ret;
	}

	/**
	 * Returns the entry of the successor
	 *
	 * @param p_localID the LocalID whose successor is searched
	 * @param p_node    anchor node
	 * @return the entry of p_localID's successor or null if there is no successor
	 */
	private Entry getSuccessorsEntry(final long p_localID, final Node p_node) {
		Entry ret = null;
		Node successorsNode;
		long successorsLID;

		successorsNode = getSuccessorsNode(p_localID, p_node);
		if (successorsNode != null) {
			for (int i = 0; i < successorsNode.getNumberOfEntries(); i++) {
				successorsLID = successorsNode.getLocalID(i);
				if (p_localID < successorsLID) {
					ret = new Entry(successorsLID, successorsNode.getNodeID(i));
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Splits down the middle if node is greater than maxEntries
	 *
	 * @param p_localID the new LocalID that causes the splitting
	 * @param p_node    the node that has to be split
	 * @return the node in which p_localID must be inserted
	 */
	private Node split(final long p_localID, final Node p_node) {
		Node ret;
		Node node;

		int size;
		int medianIndex;
		long medianLID;
		short medianNodeID;

		Node left;
		Node right;
		Node parent;
		Node newRoot;

		node = p_node;

		size = node.getNumberOfEntries();
		medianIndex = size / 2;
		medianLID = node.getLocalID(medianIndex);
		medianNodeID = node.getNodeID(medianIndex);

		left = new Node(null, m_maxEntries, m_maxChildren);
		left.addEntries(node, 0, medianIndex, 0);
		if (0 < node.getNumberOfChildren()) {
			left.addChildren(node, 0, medianIndex + 1, 0);
		}

		right = new Node(null, m_maxEntries, m_maxChildren);
		right.addEntries(node, medianIndex + 1, size, 0);
		if (0 < node.getNumberOfChildren()) {
			right.addChildren(node, medianIndex + 1, node.getNumberOfChildren(), 0);
		}
		if (null == node.getParent()) {
			// New root, height of tree is increased
			newRoot = new Node(null, m_maxEntries, m_maxChildren);
			newRoot.addEntry(medianLID, medianNodeID, 0);
			node.setParent(newRoot);
			m_root = newRoot;
			node = m_root;
			node.addChild(left);
			node.addChild(right);
			parent = newRoot;
		} else {
			// Move the median LocalID up to the parent
			parent = node.getParent();
			parent.addEntry(medianLID, medianNodeID);
			parent.removeChild(node);
			parent.addChild(left);
			parent.addChild(right);

			if (parent.getNumberOfEntries() > m_maxEntries) {
				split(p_localID, parent);
			}
		}

		if (p_localID < medianLID) {
			ret = left;
		} else if (p_localID > medianLID) {
			ret = right;
		} else {
			ret = parent;
		}

		return ret;
	}

	/**
	 * Removes given p_localID
	 *
	 * @param p_localID the LocalID
	 * @return p_localID or (-1) if there is no entry for p_localID
	 */
	private long remove(final long p_localID) {
		long ret;
		Node node;

		node = getNodeOrSuccessorsNode(p_localID);
		ret = remove(p_localID, node);

		return ret;
	}

	/**
	 * Removes the p_localID from given node and checks invariants
	 *
	 * @param p_localID the LocalID
	 * @param p_node    the node in which p_localID should be stored
	 * @return p_localID or (-1) if there is no entry for p_localID
	 */
	private long remove(final long p_localID, final Node p_node) {
		long ret = -1;
		int index;
		Node greatest;
		long replaceLID;
		short replaceNodeID;

		assert p_node != null;

		index = p_node.indexOf(p_localID);
		if (0 <= index) {
			ret = p_node.removeEntry(p_localID);
			if (0 == p_node.getNumberOfChildren()) {
				// Leaf node
				if (null != p_node.getParent() && p_node.getNumberOfEntries() < m_minEntries) {
					combined(p_node);
				} else if (null == p_node.getParent() && 0 == p_node.getNumberOfEntries()) {
					// Removing root node with no keys or children
					m_root = null;
				}
			} else {
				// Internal node
				greatest = p_node.getChild(index);
				while (0 < greatest.getNumberOfChildren()) {
					greatest = greatest.getChild(greatest.getNumberOfChildren() - 1);
				}
				replaceLID = -1;
				replaceNodeID = -1;
				if (0 < greatest.getNumberOfEntries()) {
					replaceNodeID = greatest.getNodeID(greatest.getNumberOfEntries() - 1);
					replaceLID = greatest.removeEntry(greatest.getNumberOfEntries() - 1);
				}
				p_node.addEntry(replaceLID, replaceNodeID);
				if (null != greatest.getParent() && greatest.getNumberOfEntries() < m_minEntries) {
					combined(greatest);
				}
				if (greatest.getNumberOfChildren() > m_maxChildren) {
					split(p_localID, greatest);
				}
			}
			m_size--;
		}

		return ret;
	}

	/**
	 * Combines children entries with parent when size is less than minEntries
	 *
	 * @param p_node the node
	 */
	private void combined(final Node p_node) {
		Node parent;
		int index;
		int indexOfLeftNeighbor;
		int indexOfRightNeighbor;
		Node rightNeighbor;
		int rightNeighborSize;
		Node leftNeighbor;
		int leftNeighborSize;

		long removeLID;
		int prev;
		short parentNodeID;
		long parentLID;

		short neighborNodeID;
		long neighborLID;

		parent = p_node.getParent();
		index = parent.indexOf(p_node);
		indexOfLeftNeighbor = index - 1;
		indexOfRightNeighbor = index + 1;

		rightNeighbor = null;
		rightNeighborSize = -m_minChildren;
		if (indexOfRightNeighbor < parent.getNumberOfChildren()) {
			rightNeighbor = parent.getChild(indexOfRightNeighbor);
			rightNeighborSize = rightNeighbor.getNumberOfEntries();
		}

		// Try to borrow neighbor
		if (null != rightNeighbor && rightNeighborSize > m_minEntries) {
			// Try to borrow from right neighbor
			removeLID = rightNeighbor.getLocalID(0);
			prev = parent.indexOf(removeLID) * -1 - 2;
			parentNodeID = parent.getNodeID(prev);
			parentLID = parent.removeEntry(prev);

			neighborNodeID = rightNeighbor.getNodeID(0);
			neighborLID = rightNeighbor.removeEntry(0);

			p_node.addEntry(parentLID, parentNodeID);
			parent.addEntry(neighborLID, neighborNodeID);
			if (0 < rightNeighbor.getNumberOfChildren()) {
				p_node.addChild(rightNeighbor.removeChild(0));
			}
		} else {
			leftNeighbor = null;
			leftNeighborSize = -m_minChildren;
			if (0 <= indexOfLeftNeighbor) {
				leftNeighbor = parent.getChild(indexOfLeftNeighbor);
				leftNeighborSize = leftNeighbor.getNumberOfEntries();
			}

			if (null != leftNeighbor && leftNeighborSize > m_minEntries) {
				// Try to borrow from left neighbor
				removeLID = leftNeighbor.getLocalID(leftNeighbor.getNumberOfEntries() - 1);
				prev = parent.indexOf(removeLID) * -1 - 1;
				parentNodeID = parent.getNodeID(prev);
				parentLID = parent.removeEntry(prev);

				neighborNodeID = leftNeighbor.getNodeID(leftNeighbor.getNumberOfEntries() - 1);
				neighborLID = leftNeighbor.removeEntry(leftNeighbor.getNumberOfEntries() - 1);

				p_node.addEntry(parentLID, parentNodeID);
				parent.addEntry(neighborLID, neighborNodeID);
				if (0 < leftNeighbor.getNumberOfChildren()) {
					p_node.addChild(leftNeighbor.removeChild(leftNeighbor.getNumberOfChildren() - 1));
				}
			} else if (null != rightNeighbor && 0 < parent.getNumberOfEntries()) {
				// Cannot borrow from neighbors, try to combined with right neighbor
				removeLID = rightNeighbor.getLocalID(0);
				prev = parent.indexOf(removeLID) * -1 - 2;
				parentNodeID = parent.getNodeID(prev);
				parentLID = parent.removeEntry(prev);
				parent.removeChild(rightNeighbor);
				p_node.addEntry(parentLID, parentNodeID);

				p_node.addEntries(rightNeighbor, 0, rightNeighbor.getNumberOfEntries(), p_node.getNumberOfEntries());
				p_node.addChildren(rightNeighbor, 0, rightNeighbor.getNumberOfChildren(), p_node.getNumberOfChildren());

				if (null != parent.getParent() && parent.getNumberOfEntries() < m_minEntries) {
					// Removing key made parent too small, combined up tree
					combined(parent);
				} else if (0 == parent.getNumberOfEntries()) {
					// Parent no longer has keys, make this node the new root which decreases the height of the tree
					p_node.setParent(null);
					m_root = p_node;
				}
			} else if (null != leftNeighbor && 0 < parent.getNumberOfEntries()) {
				// Cannot borrow from neighbors, try to combined with left neighbor
				removeLID = leftNeighbor.getLocalID(leftNeighbor.getNumberOfEntries() - 1);
				prev = parent.indexOf(removeLID) * -1 - 1;
				parentNodeID = parent.getNodeID(prev);
				parentLID = parent.removeEntry(prev);
				parent.removeChild(leftNeighbor);
				p_node.addEntry(parentLID, parentNodeID);
				p_node.addEntries(leftNeighbor, 0, leftNeighbor.getNumberOfEntries(), -1);
				p_node.addChildren(leftNeighbor, 0, leftNeighbor.getNumberOfChildren(), -1);

				if (null != parent.getParent() && parent.getNumberOfEntries() < m_minEntries) {
					// Removing key made parent too small, combined up tree
					combined(parent);
				} else if (0 == parent.getNumberOfEntries()) {
					// Parent no longer has keys, make this node the new root which decreases the height of the tree
					p_node.setParent(null);
					m_root = p_node;
				}
			}
		}
	}

	/**
	 * Returns the number of entries in btree
	 *
	 * @return the number of entries in btree
	 */
	public int size() {
		return m_size;
	}

	/**
	 * Validates the btree
	 *
	 * @return whether the tree is valid or not
	 */
	public boolean validate() {
		boolean ret = true;

		if (m_root != null) {
			ret = validateNode(m_root);
		}

		return ret;
	}

	/**
	 * Validates the node according to the btree invariants
	 *
	 * @param p_node the node
	 * @return whether the node is valid or not
	 */
	private boolean validateNode(final Node p_node) {
		boolean ret = true;
		int numberOfEntries;
		long prev;
		long next;
		int childrenSize;
		Node first;
		Node last;
		Node child;

		numberOfEntries = p_node.getNumberOfEntries();

		if (1 < numberOfEntries) {
			// Make sure the keys are sorted
			for (int i = 1; i < numberOfEntries; i++) {
				prev = p_node.getLocalID(i - 1);
				next = p_node.getLocalID(i);
				if (prev > next) {
					ret = false;
					break;
				}
			}
		} else {
			childrenSize = p_node.getNumberOfChildren();
			if (null == p_node.getParent()) {
				// Root
				if (numberOfEntries > m_maxEntries) {
					// Check max key size. Root does not have a minimum key size
					ret = false;
				} else if (0 == childrenSize) {
					// If root, no children, and keys are valid
					ret = true;
				} else if (2 > childrenSize) {
					// Root should have zero or at least two children
					ret = false;
				} else if (childrenSize > m_maxChildren) {
					ret = false;
				}
			} else {
				// Non-root
				if (numberOfEntries < m_minEntries) {
					ret = false;
				} else if (numberOfEntries > m_maxEntries) {
					ret = false;
				} else if (0 == childrenSize) {
					ret = true;
				} else if (numberOfEntries != childrenSize - 1) {
					// If there are children, there should be one more child then keys
					ret = false;
				} else if (childrenSize < m_minChildren) {
					ret = false;
				} else if (childrenSize > m_maxChildren) {
					ret = false;
				}
			}

			first = p_node.getChild(0);
			// The first child's last key should be less than the node's first key
			if (first.getLocalID(first.getNumberOfEntries() - 1) > p_node.getLocalID(0)) {
				ret = false;
			}

			last = p_node.getChild(p_node.getNumberOfChildren() - 1);
			// The last child's first key should be greater than the node's last key
			if (last.getLocalID(0) < p_node.getLocalID(p_node.getNumberOfEntries() - 1)) {
				ret = false;
			}

			// Check that each node's first and last key holds it's invariance
			for (int i = 1; i < p_node.getNumberOfEntries(); i++) {
				prev = p_node.getLocalID(i - 1);
				next = p_node.getLocalID(i);
				child = p_node.getChild(i);
				if (prev > child.getLocalID(0)) {
					ret = false;
					break;
				}
				if (next < child.getLocalID(child.getNumberOfEntries() - 1)) {
					ret = false;
					break;
				}
			}

			for (int i = 0; i < p_node.getNumberOfChildren(); i++) {
				child = p_node.getChild(i);
				if (!validateNode(child)) {
					ret = false;
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Prints the btree
	 *
	 * @return String interpretation of the tree
	 */
	@Override
	public String toString() {
		String ret;

		if (null == m_root) {
			ret = "Btree has no nodes";
		} else {
			ret = "Size: " + m_size + "\n" + getString(m_root, "", true);
		}

		return ret;
	}

	/**
	 * Prints one node of the btree and walks down the btree recursively
	 *
	 * @param p_node   the current node
	 * @param p_prefix the prefix to use
	 * @param p_isTail defines wheter the node is the tail
	 * @return String interpretation of the tree
	 */
	private String getString(final Node p_node, final String p_prefix, final boolean p_isTail) {
		StringBuilder ret;
		Node obj;

		ret = new StringBuilder();

		ret.append(p_prefix);
		if (p_isTail) {
			ret.append("└── ");
		} else {
			ret.append("├── ");
		}
		ret.append("[" + p_node.getNumberOfEntries() + ", " + p_node.getNumberOfChildren() + "] ");
		for (int i = 0; i < p_node.getNumberOfEntries(); i++) {
			ret.append("(LocalID: " + ChunkID.toHexString(p_node.getLocalID(i)) + " NodeID: "
					+ NodeID.toHexString(p_node.getNodeID(i)) + ")");
			if (i < p_node.getNumberOfEntries() - 1) {
				ret.append(", ");
			}
		}
		ret.append("\n");

		if (null != p_node.getChild(0)) {
			for (int i = 0; i < p_node.getNumberOfChildren() - 1; i++) {
				obj = p_node.getChild(i);
				if (p_isTail) {
					ret.append(getString(obj, p_prefix + "    ", false));
				} else {
					ret.append(getString(obj, p_prefix + "│   ", false));
				}
			}
			if (1 <= p_node.getNumberOfChildren()) {
				obj = p_node.getChild(p_node.getNumberOfChildren() - 1);
				if (p_isTail) {
					ret.append(getString(obj, p_prefix + "    ", true));
				} else {
					ret.append(getString(obj, p_prefix + "│   ", true));
				}
			}
		}
		return ret.toString();
	}

	/**
	 * A single node of the btree
	 *
	 * @author Kevin Beineke
	 *         13.06.2013
	 */
	private final class Node implements Comparable<Node>, Serializable {

		private static final long serialVersionUID = 7768073624509268941L;

		private Node m_parent;

		private long[] m_keys;
		private short[] m_dataLeafs;
		private short m_numberOfEntries;

		private Node[] m_children;
		private short m_numberOfChildren;

		// Constructors

		/**
		 * Creates an instance of Node
		 *
		 * @param p_parent      the parent
		 * @param p_maxEntries  the number of entries that can be stored
		 * @param p_maxChildren the number of children that can be stored
		 */
		private Node(final Node p_parent, final short p_maxEntries, final int p_maxChildren) {
			m_parent = p_parent;
			m_keys = new long[p_maxEntries + 1];
			m_dataLeafs = new short[p_maxEntries + 1];
			m_numberOfEntries = 0;
			m_children = new Node[p_maxChildren + 1];
			m_numberOfChildren = 0;
		}

		/**
		 * Compares two nodes
		 *
		 * @param p_cmp the node to compare with
		 * @return 0 if the nodes are equal, (-1) if p_cmp is larger, 1 otherwise
		 */
		@Override
		public int compareTo(final Node p_cmp) {
			int ret;

			if (getLocalID(0) < p_cmp.getLocalID(0)) {
				ret = -1;
			} else if (getLocalID(0) > p_cmp.getLocalID(0)) {
				ret = 1;
			} else {
				ret = 0;
			}

			return ret;
		}

		/**
		 * Returns the parent node
		 *
		 * @return the parent node
		 */
		private Node getParent() {
			return m_parent;
		}

		/**
		 * Returns the parent node
		 *
		 * @param p_parent the parent node
		 */
		private void setParent(final Node p_parent) {
			m_parent = p_parent;
		}

		/**
		 * Returns the LocalID to given index
		 *
		 * @param p_index the index
		 * @return the LocalID to given index
		 */
		private long getLocalID(final int p_index) {
			return m_keys[p_index];
		}

		/**
		 * Returns the data leaf to given index
		 *
		 * @param p_index the index
		 * @return the data leaf to given index
		 */
		private short getNodeID(final int p_index) {
			short ret;

			ret = m_dataLeafs[p_index];
			if (m_restorer != -1 && ret == m_creator) {
				m_dataLeafs[p_index] = m_restorer;
				ret = m_restorer;
			}

			return ret;
		}

		/**
		 * Returns the index for given LocalID. Uses the binary search algorithm from
		 * java.util.Arrays adapted to our needs
		 *
		 * @param p_localID the LocalID
		 * @return the index for given LocalID, if it is contained in the array, (-(insertion point) - 1) otherwise
		 */
		private int indexOf(final long p_localID) {
			int ret = -1;
			int low;
			int high;
			int mid;
			long midVal;

			low = 0;
			high = m_numberOfEntries - 1;

			while (low <= high) {
				mid = low + high >>> 1;
				midVal = m_keys[mid];

				if (midVal < p_localID) {
					low = mid + 1;
				} else if (midVal > p_localID) {
					high = mid - 1;
				} else {
					ret = mid;
					break;
				}
			}
			if (-1 == ret) {
				ret = -(low + 1);
			}

			return ret;
		}

		/**
		 * Adds an entry
		 *
		 * @param p_localID the LocalID
		 * @param p_nodeID  the NodeID
		 */
		private void addEntry(final long p_localID, final short p_nodeID) {
			int index;

			index = this.indexOf(p_localID) * -1 - 1;

			System.arraycopy(m_keys, index, m_keys, index + 1, m_numberOfEntries - index);
			System.arraycopy(m_dataLeafs, index, m_dataLeafs, index + 1, m_numberOfEntries - index);

			m_keys[index] = p_localID;
			m_dataLeafs[index] = p_nodeID;

			m_numberOfEntries++;
		}

		/**
		 * Adds an entry
		 *
		 * @param p_localID the LocalID
		 * @param p_nodeID  the NodeID
		 * @param p_index   the index to store the element at
		 */
		private void addEntry(final long p_localID, final short p_nodeID, final int p_index) {
			System.arraycopy(m_keys, p_index, m_keys, p_index + 1, m_numberOfEntries - p_index);
			System.arraycopy(m_dataLeafs, p_index, m_dataLeafs, p_index + 1, m_numberOfEntries - p_index);

			m_keys[p_index] = p_localID;
			m_dataLeafs[p_index] = p_nodeID;

			m_numberOfEntries++;
		}

		/**
		 * Adds entries from another node
		 *
		 * @param p_node      the other node
		 * @param p_offsetSrc the offset in source array
		 * @param p_endSrc    the end of source array
		 * @param p_offsetDst the offset in destination array or -1 if the source array has to be prepended
		 */
		private void addEntries(final Node p_node, final int p_offsetSrc, final int p_endSrc, final int p_offsetDst) {
			long[] aux1;
			short[] aux2;

			if (-1 != p_offsetDst) {
				System.arraycopy(p_node.m_keys, p_offsetSrc, m_keys, p_offsetDst, p_endSrc - p_offsetSrc);
				System.arraycopy(p_node.m_dataLeafs, p_offsetSrc, m_dataLeafs, p_offsetDst, p_endSrc - p_offsetSrc);
				m_numberOfEntries = (short) (p_offsetDst + p_endSrc - p_offsetSrc);
			} else {
				aux1 = new long[m_keys.length];
				System.arraycopy(p_node.m_keys, 0, aux1, 0, p_node.m_numberOfEntries);
				System.arraycopy(m_keys, 0, aux1, p_node.m_numberOfEntries, m_numberOfEntries);
				m_keys = aux1;

				aux2 = new short[m_dataLeafs.length];
				System.arraycopy(p_node.m_dataLeafs, 0, aux2, 0, p_node.m_numberOfEntries);
				System.arraycopy(m_dataLeafs, 0, aux2, p_node.m_numberOfEntries, m_numberOfEntries);
				m_dataLeafs = aux2;

				m_numberOfEntries += p_node.m_numberOfEntries;
			}
		}

		/**
		 * Changes an entry
		 *
		 * @param p_localID the LocalID
		 * @param p_nodeID  the NodeID
		 * @param p_index   the index of given entry in this node
		 */
		private void changeEntry(final long p_localID, final short p_nodeID, final int p_index) {

			if (p_localID == getLocalID(p_index)) {
				m_keys[p_index] = p_localID;
				m_dataLeafs[p_index] = p_nodeID;
			}
		}

		/**
		 * Removes the entry with given LocalID
		 *
		 * @param p_localID LocalID of the entry that has to be deleted
		 * @return p_localID or (-1) if there is no entry for p_localID in this node
		 */
		private long removeEntry(final long p_localID) {
			long ret = -1;
			int index;

			index = this.indexOf(p_localID);
			if (0 <= index) {
				ret = getLocalID(index);

				System.arraycopy(m_keys, index + 1, m_keys, index, m_numberOfEntries - index - 1);
				System.arraycopy(m_dataLeafs, index + 1, m_dataLeafs, index, m_numberOfEntries - index - 1);

				m_numberOfEntries--;
			}

			return ret;
		}

		/**
		 * Removes the entry with given index
		 *
		 * @param p_index the index of the entry that has to be deleted
		 * @return p_localID or (-1) if p_index is to large
		 */
		private long removeEntry(final int p_index) {
			long ret = -1;

			if (p_index < m_numberOfEntries) {
				ret = getLocalID(p_index);

				System.arraycopy(m_keys, p_index + 1, m_keys, p_index, m_numberOfEntries - p_index);
				System.arraycopy(m_dataLeafs, p_index + 1, m_dataLeafs, p_index, m_numberOfEntries - p_index);
				m_numberOfEntries--;
			}

			return ret;
		}

		/**
		 * Returns the number of entries
		 *
		 * @return the number of entries
		 */
		private int getNumberOfEntries() {
			return m_numberOfEntries;
		}

		/**
		 * Returns the child with given index
		 *
		 * @param p_index the index
		 * @return the child with given index
		 */
		private Node getChild(final int p_index) {
			Node ret;

			if (p_index >= m_numberOfChildren) {
				ret = null;
			} else {
				ret = m_children[p_index];
			}

			return ret;
		}

		/**
		 * Returns the index of the given child. Uses the binary search algorithm from
		 * java.util.Arrays adapted to our needs
		 *
		 * @param p_child the child
		 * @return the index of the given child, if it is contained in the array, (-(insertion point) - 1) otherwise
		 */
		private int indexOf(final Node p_child) {
			int ret = -1;
			int low;
			int high;
			int mid;
			long localID;
			long midVal;

			localID = p_child.getLocalID(0);
			low = 0;
			high = m_numberOfChildren - 1;

			while (low <= high) {
				mid = low + high >>> 1;
				midVal = m_children[mid].getLocalID(0);

				if (midVal < localID) {
					low = mid + 1;
				} else if (midVal > localID) {
					high = mid - 1;
				} else {
					ret = mid;
					break;
				}
			}
			if (-1 == ret) {
				ret = -(low + 1);
			}

			return ret;
		}

		/**
		 * Adds a child
		 *
		 * @param p_child the child
		 */
		private void addChild(final Node p_child) {
			int index;

			index = this.indexOf(p_child) * -1 - 1;

			System.arraycopy(m_children, index, m_children, index + 1, m_numberOfChildren - index);
			m_children[index] = p_child;
			p_child.setParent(this);

			m_numberOfChildren++;
		}

		/**
		 * Adds children of another node
		 *
		 * @param p_node      the other node
		 * @param p_offsetSrc the offset in source array
		 * @param p_endSrc    the end of source array
		 * @param p_offsetDst the offset in destination array or -1 if the source array has to be prepended
		 */
		private void addChildren(final Node p_node, final int p_offsetSrc, final int p_endSrc, final int p_offsetDst) {
			Node[] aux;

			if (-1 != p_offsetDst) {
				System.arraycopy(p_node.m_children, p_offsetSrc, m_children, p_offsetDst, p_endSrc - p_offsetSrc);

				for (final Node child : m_children) {
					if (null == child) {
						break;
					}
					child.setParent(this);
				}
				m_numberOfChildren = (short) (p_offsetDst + p_endSrc - p_offsetSrc);
			} else {
				aux = new Node[m_children.length];
				System.arraycopy(p_node.m_children, 0, aux, 0, p_node.m_numberOfChildren);

				for (final Node child : aux) {
					if (null == child) {
						break;
					}
					child.setParent(this);
				}

				System.arraycopy(m_children, 0, aux, p_node.m_numberOfChildren, m_numberOfChildren);
				m_children = aux;

				m_numberOfChildren += p_node.m_numberOfChildren;
			}
		}

		/**
		 * Removes the given child
		 *
		 * @param p_child the child
		 * @return true if the child was found and deleted, false otherwise
		 */
		private boolean removeChild(final Node p_child) {
			boolean ret = false;
			int index;

			index = this.indexOf(p_child);
			if (0 <= index) {
				System.arraycopy(m_children, index + 1, m_children, index, m_numberOfChildren - index);

				m_numberOfChildren--;
				ret = true;
			}

			return ret;
		}

		/**
		 * Removes the child with given index
		 *
		 * @param p_index the index
		 * @return the deleted child
		 */
		private Node removeChild(final int p_index) {
			Node ret = null;

			if (p_index < m_numberOfChildren) {
				ret = m_children[p_index];
				System.arraycopy(m_children, p_index + 1, m_children, p_index, m_numberOfChildren - p_index);

				m_numberOfChildren--;
			}

			return ret;
		}

		/**
		 * Returns the number of children
		 *
		 * @return the number of children
		 */
		private int getNumberOfChildren() {
			return m_numberOfChildren;
		}

		/**
		 * Prints the node
		 *
		 * @return String interpretation of the node
		 */
		@Override
		public String toString() {
			StringBuilder ret;

			ret = new StringBuilder();

			ret.append("entries=[");
			for (int i = 0; i < getNumberOfEntries(); i++) {
				ret.append("(LocalID: " + getLocalID(i) + " location: " + getNodeID(i) + ")");
				if (i < getNumberOfEntries() - 1) {
					ret.append(", ");
				}
			}
			ret.append("]\n");

			if (null != m_parent) {
				ret.append("parent=[");
				for (int i = 0; i < m_parent.getNumberOfEntries(); i++) {
					ret.append("(LocalID: " + getLocalID(i) + " location: " + getNodeID(i) + ")");
					if (i < m_parent.getNumberOfEntries() - 1) {
						ret.append(", ");
					}
				}
				ret.append("]\n");
			}

			if (null != m_children) {
				ret.append("numberOfEntries=");
				ret.append(getNumberOfEntries());
				ret.append(" children=");
				ret.append(getNumberOfChildren());
				ret.append("\n");
			}

			return ret.toString();
		}
	}

	/**
	 * Auxiliary object to return LocalID and NodeID at once
	 *
	 * @author Kevin Beineke
	 *         13.06.2013
	 */
	private final class Entry implements Serializable {

		private static final long serialVersionUID = -3247514337685593792L;

		// Attributes
		private long m_localID;
		private short m_nodeID;

		// Constructors

		/**
		 * Creates an instance of Entry
		 *
		 * @param p_localID the LocalID
		 * @param p_nodeID  the NodeID
		 */
		Entry(final long p_localID, final short p_nodeID) {
			m_localID = p_localID;
			m_nodeID = p_nodeID;
		}

		/**
		 * Returns the LocalID
		 *
		 * @return the LocalID
		 */
		public long getLocalID() {
			return m_localID;
		}

		/**
		 * Returns the location
		 *
		 * @return the location
		 */
		public short getNodeID() {
			return m_nodeID;
		}

		/**
		 * Prints the entry
		 *
		 * @return String interpretation of the entry
		 */
		@Override
		public String toString() {
			return "(LocalID: " + m_localID + ", location: " + m_nodeID + ")";
		}
	}
}

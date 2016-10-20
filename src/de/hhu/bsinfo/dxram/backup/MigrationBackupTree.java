
package de.hhu.bsinfo.dxram.backup;

import java.io.Serializable;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Btree to store backup ranges of migrated chunks.
 * @author Kevin Beineke
 *         03.06.2015
 */
public final class MigrationBackupTree implements Serializable {

	// Constants
	private static final long serialVersionUID = 7565597467331239020L;
	private static final byte INVALID = -1;

	// Attributes
	private short m_minEntries;
	private long m_backupRangeSize;
	private short m_minChildren;
	private short m_maxEntries;
	private short m_maxChildren;

	private Node m_root;
	private int m_entrySize;
	private long m_currentSecLogSize;

	private Entry m_changedEntry;

	private ReentrantLock m_lock;

	// Constructors
	/**
	 * Creates an instance of MigrationsTree
	 * @param p_order
	 *            order of the btree
	 * @param p_backupRangeSize
	 *            the backup range size
	 */
	public MigrationBackupTree(final short p_order, final long p_backupRangeSize) {
		// too small order for BTree
		assert 1 < p_order;

		m_minEntries = p_order;
		m_backupRangeSize = p_backupRangeSize;
		m_minChildren = (short) (m_minEntries + 1);
		m_maxEntries = (short) (2 * m_minEntries);
		m_maxChildren = (short) (m_maxEntries + 1);

		m_root = null;
		m_entrySize = -1;
		m_currentSecLogSize = 0;

		m_changedEntry = null;

		m_lock = new ReentrantLock(false);

		createOrReplaceEntry(Long.MAX_VALUE, INVALID);
	}

	// Methods
	/**
	 * Checks if the Chunk fits in current log
	 * @param p_size
	 *            the size of the chunk + log header size
	 * @return true if it fits
	 */
	public boolean fits(final long p_size) {
		boolean ret;

		m_lock.lock();
		ret = p_size + m_currentSecLogSize <= m_backupRangeSize;
		m_lock.unlock();

		return ret;
	}

	/**
	 * Resets the byte counter for current backup range
	 */
	public void initNewBackupRange() {
		m_currentSecLogSize = 0;
	}

	/**
	 * Stores the backup range ID for a single object
	 * @param p_chunkID
	 *            ChunkID of migrated object
	 * @param p_rangeID
	 *            the backup range ID
	 * @param p_size
	 *            the size of the chunk + log header size
	 * @return true if insertion was successful
	 */
	public boolean putObject(final long p_chunkID, final byte p_rangeID, final long p_size) {
		Node node;

		m_lock.lock();
		node = createOrReplaceEntry(p_chunkID, p_rangeID);

		mergeWithPredecessorOrBound(p_chunkID, p_rangeID, node);

		mergeWithSuccessor(p_chunkID, p_rangeID);

		m_currentSecLogSize += p_size;
		m_lock.unlock();

		return true;
	}

	/**
	 * Stores the backup range ID for a range
	 * @param p_startID
	 *            ChunkID of first migrated object
	 * @param p_endID
	 *            ChunkID of last migrated object
	 * @param p_rangeID
	 *            the backup range ID
	 * @param p_rangeSize
	 *            the size of the range + log header sizes
	 * @return true if insertion was successful
	 */
	public boolean putRange(final long p_startID, final long p_endID, final byte p_rangeID, final long p_rangeSize) {
		// end larger than start
		assert p_startID <= p_endID;

		Node startNode;

		if (p_startID == p_endID) {
			putObject(p_startID, p_rangeID, p_rangeSize);
		} else {
			m_lock.lock();
			startNode = createOrReplaceEntry(p_startID, p_rangeID);

			mergeWithPredecessorOrBound(p_startID, p_rangeID, startNode);

			createOrReplaceEntry(p_endID, p_rangeID);

			removeEntriesWithinRange(p_startID, p_endID);

			mergeWithSuccessor(p_endID, p_rangeID);

			m_currentSecLogSize += p_rangeSize;
			m_lock.unlock();
		}
		return true;
	}

	/**
	 * Returns the backup range ID for given object
	 * @param p_chunkID
	 *            ChunkID of requested object
	 * @return the backup range ID
	 */
	public byte getBackupRange(final long p_chunkID) {
		assert m_root != null;

		byte ret;

		m_lock.lock();
		ret = getRangeIDOrSuccessorsRangeID(p_chunkID);
		m_lock.unlock();

		return ret;
	}

	/**
	 * Removes given object from btree
	 * @param p_chunkID
	 *            ChunkID of deleted object
	 * @note should always be called if an object is deleted
	 */
	public void removeObject(final long p_chunkID) {
		int index;
		Node node;
		long currentCID;
		Entry currentEntry;
		Entry predecessor;
		Entry successor;

		m_lock.lock();
		if (null != m_root) {
			node = getNodeOrSuccessorsNode(p_chunkID);
			if (null != node) {
				currentCID = -1;

				index = node.indexOf(p_chunkID);
				if (0 <= index) {
					// Entry was found
					currentCID = node.getChunkID(index);
					predecessor = getPredecessorsEntry(p_chunkID, node);
					currentEntry = new Entry(currentCID, node.getRangeID(index));
					successor = getSuccessorsEntry(p_chunkID, node);
					if (INVALID != currentEntry.getRangeID() && null != predecessor) {
						if (p_chunkID - 1 == predecessor.getChunkID()) {
							// Predecessor is direct neighbor: AB
							// Successor might be direct neighbor or not: ABC or AB___C
							if (INVALID == successor.getRangeID()) {
								// Successor is barrier: ABC -> A_C or AB___C -> A___C
								remove(p_chunkID);
							} else {
								// Successor is no barrier: ABC -> AXC or AB___C -> AX___C
								node.changeEntry(p_chunkID, INVALID, index);
							}
							if (INVALID == predecessor.getRangeID()) {
								// Predecessor is barrier: A_C -> ___C or AXC -> ___XC
								// or A___C -> ___C or AX___C -> ___X___C
								remove(predecessor.getChunkID());
							}
						} else {
							// Predecessor is no direct neighbor: A___B
							if (INVALID == successor.getRangeID()) {
								// Successor is barrier: A___BC -> A___C or A___B___C -> A___'___C
								remove(p_chunkID);
							} else {
								// Successor is no barrier: A___BC -> A___XC or A___B___C -> A___X___C
								node.changeEntry(p_chunkID, INVALID, index);
							}
							// Predecessor is barrier: A___C -> A___(B-1)_C or A___XC -> ___(B-1)XC
							// or A___'___C -> A___(B-1)___C or A___X___C -> A___(B-1)X___C
							createOrReplaceEntry(p_chunkID - 1, currentEntry.getRangeID());
						}
					}
				} else {
					// Entry was not found
					index = index * -1 - 1;
					successor = new Entry(node.getChunkID(index), node.getRangeID(index));
					predecessor = getPredecessorsEntry(successor.getChunkID(), node);
					if (INVALID != successor.getRangeID() && null != predecessor) {
						// Entry is in range
						if (p_chunkID - 1 == predecessor.getChunkID()) {
							// Predecessor is direct neighbor: A'B'
							// Successor might be direct neighbor or not: A'B'C -> AXC or A'B'___C -> AX___C
							createOrReplaceEntry(p_chunkID, INVALID);
							if (INVALID == predecessor.getRangeID()) {
								// Predecessor is barrier: AXC -> ___XC or AX___C -> ___X___C
								remove(p_chunkID - 1);
							}
						} else {
							// Predecessor is no direct neighbor: A___'B'
							// Successor might be direct neighbor or not: A___'B'C -> A___(B-1)XC
							// or A___'B'___C -> A___(B-1)X___C
							createOrReplaceEntry(p_chunkID, INVALID);
							createOrReplaceEntry(p_chunkID - 1, successor.getRangeID());
						}
					}
				}
			}
		}
		m_lock.unlock();
	}

	/**
	 * Creates a new entry or replaces the old one
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the backup range ID
	 * @return the node in which the entry is stored
	 */
	private Node createOrReplaceEntry(final long p_chunkID, final byte p_rangeID) {
		Node ret = null;
		Node node;
		int index;
		int size;

		if (null == m_root) {
			m_root = new Node(null, m_maxEntries, m_maxChildren);
			m_root.addEntry(p_chunkID, p_rangeID);
			ret = m_root;
		} else {
			node = m_root;
			while (true) {
				if (0 == node.getNumberOfChildren()) {
					index = node.indexOf(p_chunkID);
					if (0 <= index) {
						m_changedEntry = new Entry(node.getChunkID(index), node.getRangeID(index));
						node.changeEntry(p_chunkID, p_rangeID, index);
					} else {
						m_changedEntry = null;
						node.addEntry(p_chunkID, p_rangeID, index * -1 - 1);
						if (m_maxEntries < node.getNumberOfEntries()) {
							// Need to split up
							node = split(p_chunkID, node);
						}
					}
					break;
				} else {
					if (p_chunkID < node.getChunkID(0)) {
						node = node.getChild(0);
						continue;
					}

					size = node.getNumberOfEntries();
					if (p_chunkID > node.getChunkID(size - 1)) {
						node = node.getChild(size);
						continue;
					}

					index = node.indexOf(p_chunkID);
					if (0 <= index) {
						m_changedEntry = new Entry(node.getChunkID(index), node.getRangeID(index));
						node.changeEntry(p_chunkID, p_rangeID, index);
						break;
					} else {
						node = node.getChild(index * -1 - 1);
					}
				}
			}

			ret = node;
		}
		if (m_changedEntry == null) {
			m_entrySize++;
		}

		return ret;
	}

	/**
	 * Merges the object or range with predecessor
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the backup range ID
	 * @param p_node
	 *            anchor node
	 */
	private void mergeWithPredecessorOrBound(final long p_chunkID, final byte p_rangeID, final Node p_node) {
		Entry predecessor;
		Entry successor;

		predecessor = getPredecessorsEntry(p_chunkID, p_node);
		if (null == predecessor) {
			createOrReplaceEntry(p_chunkID - 1, INVALID);
		} else {
			if (p_chunkID - 1 == predecessor.getChunkID()) {
				if (p_rangeID == predecessor.getRangeID()) {
					remove(predecessor.getChunkID(), getPredecessorsNode(p_chunkID, p_node));
				}
			} else {
				successor = getSuccessorsEntry(p_chunkID, p_node);
				if (null == m_changedEntry) {
					// Successor is end of range
					if (p_rangeID != successor.getRangeID()) {
						createOrReplaceEntry(p_chunkID - 1, successor.getRangeID());
					} else {
						// New Object is in range that already was migrated to the same destination
						remove(p_chunkID, p_node);
					}
				} else {
					if (p_rangeID != m_changedEntry.getRangeID()) {
						createOrReplaceEntry(p_chunkID - 1, m_changedEntry.getRangeID());
					}
				}
			}
		}
	}

	/**
	 * Merges the object or range with successor
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the backup range ID
	 */
	private void mergeWithSuccessor(final long p_chunkID, final byte p_rangeID) {
		Node node;
		Entry successor;

		node = getNodeOrSuccessorsNode(p_chunkID);
		successor = getSuccessorsEntry(p_chunkID, node);
		if (null != successor && p_rangeID == successor.getRangeID()) {
			remove(p_chunkID, node);
		}
	}

	/**
	 * Removes all entries between start (inclusive) and end
	 * @param p_start
	 *            the first object in range
	 * @param p_end
	 *            the last object in range
	 */
	private void removeEntriesWithinRange(final long p_start, final long p_end) {
		long successor;

		remove(p_start, getNodeOrSuccessorsNode(p_start));

		successor = getCIDOrSuccessorsCID(p_start);
		while (-1 != successor && successor < p_end) {
			remove(successor);
			successor = getCIDOrSuccessorsCID(p_start);
		}
	}

	/**
	 * Returns the node in which the next entry to given ChunkID (could be the ChunkID itself) is stored
	 * @param p_chunkID
	 *            ChunkID whose node is searched
	 * @return node in which ChunkID is stored if ChunkID is in tree or successors node, null if there is no successor
	 */
	private Node getNodeOrSuccessorsNode(final long p_chunkID) {
		Node ret;
		int size;
		int index;
		long greater;

		ret = m_root;

		while (true) {
			if (p_chunkID < ret.getChunkID(0)) {
				if (0 < ret.getNumberOfChildren()) {
					ret = ret.getChild(0);
					continue;
				} else {
					break;
				}
			}

			size = ret.getNumberOfEntries();
			greater = ret.getChunkID(size - 1);
			if (p_chunkID > greater) {
				if (size < ret.getNumberOfChildren()) {
					ret = ret.getChild(size);
					continue;
				} else {
					ret = getSuccessorsNode(greater, ret);
					break;
				}
			}

			index = ret.indexOf(p_chunkID);
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
	 * Returns next ChunkID to given ChunkID (could be the ChunkID itself)
	 * @param p_chunkID
	 *            the ChunkID
	 * @return p_chunkID if p_chunkID is in btree or successor of p_chunkID, (-1) if there is no successor
	 */
	private long getCIDOrSuccessorsCID(final long p_chunkID) {
		long ret = -1;
		int index;
		Node node;

		node = getNodeOrSuccessorsNode(p_chunkID);
		if (node != null) {
			index = node.indexOf(p_chunkID);
			if (0 <= index) {
				ret = node.getChunkID(index);
			} else {
				ret = node.getChunkID(index * -1 - 1);
			}
		}

		return ret;
	}

	/**
	 * Returns the backup range ID of next ChunkID to given ChunkID (could be the ChunkID itself)
	 * @param p_chunkID
	 *            the ChunkID whose corresponding RangeID is searched
	 * @return RangeID for p_chunkID if p_chunkID is in btree or successors NodeID
	 */
	private byte getRangeIDOrSuccessorsRangeID(final long p_chunkID) {
		byte ret = INVALID;
		int index;
		Node node;

		node = getNodeOrSuccessorsNode(p_chunkID);
		if (node != null) {
			index = node.indexOf(p_chunkID);
			if (0 <= index) {
				ret = node.getRangeID(index);
			} else {
				ret = node.getRangeID(index * -1 - 1);
			}
		}

		return ret;
	}

	/**
	 * Returns the node in which the predecessor is
	 * @param p_chunkID
	 *            ChunkID whose predecessor's node is searched
	 * @param p_node
	 *            anchor node
	 * @return the node in which the predecessor of p_chunkID is or null if there is no predecessor
	 */
	private Node getPredecessorsNode(final long p_chunkID, final Node p_node) {
		int index;
		Node ret = null;
		Node node;
		Node parent;

		assert p_node != null;

		node = p_node;

		if (p_chunkID == node.getChunkID(0)) {
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
					while (parent != null && p_chunkID < parent.getChunkID(0)) {
						parent = parent.getParent();
					}
					ret = parent;
				}
			}
		} else {
			index = node.indexOf(p_chunkID);
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
	 * @param p_chunkID
	 *            the ChunkID whose predecessor is searched
	 * @param p_node
	 *            anchor node
	 * @return the entry of p_chunkID's predecessor or null if there is no predecessor
	 */
	private Entry getPredecessorsEntry(final long p_chunkID, final Node p_node) {
		Entry ret = null;
		Node predecessorsNode;
		long predecessorsCID;

		predecessorsNode = getPredecessorsNode(p_chunkID, p_node);
		if (predecessorsNode != null) {
			for (int i = predecessorsNode.getNumberOfEntries() - 1; i >= 0; i--) {
				predecessorsCID = predecessorsNode.getChunkID(i);
				if (p_chunkID > predecessorsCID) {
					ret = new Entry(predecessorsCID, predecessorsNode.getRangeID(i));
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Returns the node in which the successor is
	 * @param p_chunkID
	 *            ChunkID whose successor's node is searched
	 * @param p_node
	 *            anchor node
	 * @return the node in which the successor of p_chunkID is or null if there is no successor
	 */
	private Node getSuccessorsNode(final long p_chunkID, final Node p_node) {
		int index;
		Node ret = null;
		Node node;
		Node parent;

		assert p_node != null;

		node = p_node;

		if (p_chunkID == node.getChunkID(node.getNumberOfEntries() - 1)) {
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
					while (parent != null && p_chunkID > parent.getChunkID(parent.getNumberOfEntries() - 1)) {
						parent = parent.getParent();
					}
					ret = parent;
				}
			}
		} else {
			index = node.indexOf(p_chunkID);
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
	 * @param p_chunkID
	 *            the ChunkID whose successor is searched
	 * @param p_node
	 *            anchor node
	 * @return the entry of p_chunkID's successor or null if there is no successor
	 */
	private Entry getSuccessorsEntry(final long p_chunkID, final Node p_node) {
		Entry ret = null;
		Node successorsNode;
		long successorsCID;

		successorsNode = getSuccessorsNode(p_chunkID, p_node);
		if (successorsNode != null) {
			for (int i = 0; i < successorsNode.getNumberOfEntries(); i++) {
				successorsCID = successorsNode.getChunkID(i);
				if (p_chunkID < successorsCID) {
					ret = new Entry(successorsCID, successorsNode.getRangeID(i));
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Splits down the middle if node is greater than maxEntries
	 * @param p_chunkID
	 *            the new ChunkID that causes the splitting
	 * @param p_node
	 *            the node that has to be split
	 * @return the node in which p_chunkID must be inserted
	 */
	private Node split(final long p_chunkID, final Node p_node) {
		Node ret;
		Node node;

		int size;
		int medianIndex;
		long medianCID;
		byte medianRangeID;

		Node left;
		Node right;
		Node parent;
		Node newRoot;

		node = p_node;

		size = node.getNumberOfEntries();
		medianIndex = size / 2;
		medianCID = node.getChunkID(medianIndex);
		medianRangeID = node.getRangeID(medianIndex);

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
			newRoot.addEntry(medianCID, medianRangeID, 0);
			node.setParent(newRoot);
			m_root = newRoot;
			node = m_root;
			node.addChild(left);
			node.addChild(right);
			parent = newRoot;
		} else {
			// Move the median ChunkID up to the parent
			parent = node.getParent();
			parent.addEntry(medianCID, medianRangeID);
			parent.removeChild(node);
			parent.addChild(left);
			parent.addChild(right);

			if (parent.getNumberOfEntries() > m_maxEntries) {
				split(p_chunkID, parent);
			}
		}

		if (p_chunkID < medianCID) {
			ret = left;
		} else if (p_chunkID > medianCID) {
			ret = right;
		} else {
			ret = parent;
		}

		return ret;
	}

	/**
	 * Removes given p_chunkID
	 * @param p_chunkID
	 *            the ChunkID
	 * @return p_chunkID or (-1) if there is no entry for p_chunkID
	 */
	private long remove(final long p_chunkID) {
		long ret;
		Node node;

		node = getNodeOrSuccessorsNode(p_chunkID);
		ret = remove(p_chunkID, node);

		return ret;
	}

	/**
	 * Removes the p_chunkID from given node and checks invariants
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_node
	 *            the node in which p_chunkID should be stored
	 * @return p_chunkID or (-1) if there is no entry for p_chunkID
	 */
	private long remove(final long p_chunkID, final Node p_node) {
		long ret = -1;
		int index;
		Node greatest;
		long replaceCID;
		byte replaceRangeID;

		assert p_node != null;

		index = p_node.indexOf(p_chunkID);
		if (0 <= index) {
			ret = p_node.removeEntry(p_chunkID);
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
				replaceCID = -1;
				replaceRangeID = INVALID;
				if (0 < greatest.getNumberOfEntries()) {
					replaceRangeID = greatest.getRangeID(greatest.getNumberOfEntries() - 1);
					replaceCID = greatest.removeEntry(greatest.getNumberOfEntries() - 1);
				}
				p_node.addEntry(replaceCID, replaceRangeID);
				if (null != greatest.getParent() && greatest.getNumberOfEntries() < m_minEntries) {
					combined(greatest);
				}
				if (greatest.getNumberOfChildren() > m_maxChildren) {
					split(p_chunkID, greatest);
				}
			}
			m_entrySize--;
		}

		return ret;
	}

	/**
	 * Combines children entries with parent when size is less than minEntries
	 * @param p_node
	 *            the node
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

		long removeCID;
		int prev;
		byte parentRangeID;
		long parentCID;

		byte neighborRangeID;
		long neighborCID;

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
			removeCID = rightNeighbor.getChunkID(0);
			prev = parent.indexOf(removeCID) * -1 - 2;
			parentRangeID = parent.getRangeID(prev);
			parentCID = parent.removeEntry(prev);

			neighborRangeID = rightNeighbor.getRangeID(0);
			neighborCID = rightNeighbor.removeEntry(0);

			p_node.addEntry(parentCID, parentRangeID);
			parent.addEntry(neighborCID, neighborRangeID);
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
				removeCID = leftNeighbor.getChunkID(leftNeighbor.getNumberOfEntries() - 1);
				prev = parent.indexOf(removeCID) * -1 - 1;
				parentRangeID = parent.getRangeID(prev);
				parentCID = parent.removeEntry(prev);

				neighborRangeID = leftNeighbor.getRangeID(leftNeighbor.getNumberOfEntries() - 1);
				neighborCID = leftNeighbor.removeEntry(leftNeighbor.getNumberOfEntries() - 1);

				p_node.addEntry(parentCID, parentRangeID);
				parent.addEntry(neighborCID, neighborRangeID);
				if (0 < leftNeighbor.getNumberOfChildren()) {
					p_node.addChild(leftNeighbor.removeChild(leftNeighbor.getNumberOfChildren() - 1));
				}
			} else if (null != rightNeighbor && 0 < parent.getNumberOfEntries()) {
				// Cannot borrow from neighbors, try to combined with right neighbor
				removeCID = rightNeighbor.getChunkID(0);
				prev = parent.indexOf(removeCID) * -1 - 2;
				parentRangeID = parent.getRangeID(prev);
				parentCID = parent.removeEntry(prev);
				parent.removeChild(rightNeighbor);
				p_node.addEntry(parentCID, parentRangeID);

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
				removeCID = leftNeighbor.getChunkID(leftNeighbor.getNumberOfEntries() - 1);
				prev = parent.indexOf(removeCID) * -1 - 1;
				parentRangeID = parent.getRangeID(prev);
				parentCID = parent.removeEntry(prev);
				parent.removeChild(leftNeighbor);
				p_node.addEntry(parentCID, parentRangeID);
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
	 * @return the number of entries in btree
	 */
	public int size() {
		return m_entrySize;
	}

	/**
	 * Validates the btree
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
	 * @param p_node
	 *            the node
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
				prev = p_node.getChunkID(i - 1);
				next = p_node.getChunkID(i);
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
			if (first.getChunkID(first.getNumberOfEntries() - 1) > p_node.getChunkID(0)) {
				ret = false;
			}

			last = p_node.getChild(p_node.getNumberOfChildren() - 1);
			// The last child's first key should be greater than the node's last key
			if (last.getChunkID(0) < p_node.getChunkID(p_node.getNumberOfEntries() - 1)) {
				ret = false;
			}

			// Check that each node's first and last key holds it's invariance
			for (int i = 1; i < p_node.getNumberOfEntries(); i++) {
				prev = p_node.getChunkID(i - 1);
				next = p_node.getChunkID(i);
				child = p_node.getChild(i);
				if (prev > child.getChunkID(0)) {
					ret = false;
					break;
				}
				if (next < child.getChunkID(child.getNumberOfEntries() - 1)) {
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
	 * @return String interpretation of the tree
	 */
	@Override
	public String toString() {
		String ret;

		if (null == m_root) {
			ret = "Btree has no nodes";
		} else {
			ret = "Size: " + m_entrySize + "\n" + getString(m_root, "", true);
		}

		return ret;
	}

	/**
	 * Prints one node of the btree and walks down the btree recursively
	 * @param p_node
	 *            the current node
	 * @param p_prefix
	 *            the prefix to use
	 * @param p_isTail
	 *            defines wheter the node is the tail
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
			ret.append("(ChunkID: " + p_node.getChunkID(i) + " NodeID: " + p_node.getRangeID(i) + ")");
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
	 * @author Kevin Beineke
	 *         13.06.2013
	 */
	private static final class Node implements Comparable<Node>, Serializable {

		// Attributes
		private static final long serialVersionUID = 8096853988906422021L;

		private Node m_parent;

		private long[] m_keys;
		private byte[] m_dataLeafs;
		private short m_numberOfEntries;

		private Node[] m_children;
		private short m_numberOfChildren;

		// Constructors
		/**
		 * Creates an instance of Node
		 * @param p_parent
		 *            the parent
		 * @param p_maxEntries
		 *            the number of entries that can be stored
		 * @param p_maxChildren
		 *            the number of children that can be stored
		 */
		private Node(final Node p_parent, final short p_maxEntries, final int p_maxChildren) {
			m_parent = p_parent;
			m_keys = new long[p_maxEntries + 1];
			m_dataLeafs = new byte[p_maxEntries + 1];
			m_numberOfEntries = 0;
			m_children = new Node[p_maxChildren + 1];
			m_numberOfChildren = 0;
		}

		/**
		 * Compares two nodes
		 * @param p_cmp
		 *            the node to compare with
		 * @return 0 if the nodes are equal, (-1) if p_cmp is larger, 1 otherwise
		 */
		@Override
		public int compareTo(final Node p_cmp) {
			int ret;

			if (getChunkID(0) < p_cmp.getChunkID(0)) {
				ret = -1;
			} else if (getChunkID(0) > p_cmp.getChunkID(0)) {
				ret = 1;
			} else {
				ret = 0;
			}

			return ret;
		}

		/**
		 * Returns the parent node
		 * @return the parent node
		 */
		private Node getParent() {
			return m_parent;
		}

		/**
		 * Returns the parent node
		 * @param p_parent
		 *            the parent node
		 */
		private void setParent(final Node p_parent) {
			m_parent = p_parent;
		}

		/**
		 * Returns the ChunkID to given index
		 * @param p_index
		 *            the index
		 * @return the ChunkID to given index
		 */
		private long getChunkID(final int p_index) {
			return m_keys[p_index];
		}

		/**
		 * Returns the data leaf to given index
		 * @param p_index
		 *            the index
		 * @return the data leaf to given index
		 */
		private byte getRangeID(final int p_index) {
			return m_dataLeafs[p_index];
		}

		/**
		 * Returns the index for given ChunkID. Uses the binary search algorithm from
		 * java.util.Arrays adapted to our needs
		 * @param p_chunkID
		 *            the ChunkID
		 * @return the index for given ChunkID, if it is contained in the array, (-(insertion point) - 1) otherwise
		 */
		private int indexOf(final long p_chunkID) {
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

			if (midVal < p_chunkID) {
				low = mid + 1;
			} else if (midVal > p_chunkID) {
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
		 * @param p_chunkID
		 *            the ChunkID
		 * @param p_rangeID
		 *            the backup range ID
		 */
		private void addEntry(final long p_chunkID, final byte p_rangeID) {
			int index;

			index = this.indexOf(p_chunkID) * -1 - 1;

			System.arraycopy(m_keys, index, m_keys, index + 1, m_numberOfEntries - index);
			System.arraycopy(m_dataLeafs, index, m_dataLeafs, index + 1, m_numberOfEntries - index);

			m_keys[index] = p_chunkID;
			m_dataLeafs[index] = p_rangeID;

			m_numberOfEntries++;
		}

		/**
		 * Adds an entry
		 * @param p_chunkID
		 *            the ChunkID
		 * @param p_rangeID
		 *            the backup range ID
		 * @param p_index
		 *            the index to store the element at
		 */
		private void addEntry(final long p_chunkID, final byte p_rangeID, final int p_index) {
			System.arraycopy(m_keys, p_index, m_keys, p_index + 1, m_numberOfEntries - p_index);
			System.arraycopy(m_dataLeafs, p_index, m_dataLeafs, p_index + 1, m_numberOfEntries - p_index);

			m_keys[p_index] = p_chunkID;
			m_dataLeafs[p_index] = p_rangeID;

			m_numberOfEntries++;
		}

		/**
		 * Adds entries from another node
		 * @param p_node
		 *            the other node
		 * @param p_offsetSrc
		 *            the offset in source array
		 * @param p_endSrc
		 *            the end of source array
		 * @param p_offsetDst
		 *            the offset in destination array or -1 if the source array has to be prepended
		 */
		private void addEntries(final Node p_node, final int p_offsetSrc, final int p_endSrc, final int p_offsetDst) {
			long[] aux1;
			byte[] aux2;

			if (-1 != p_offsetDst) {
				System.arraycopy(p_node.m_keys, p_offsetSrc, m_keys, p_offsetDst, p_endSrc - p_offsetSrc);
				System.arraycopy(p_node.m_dataLeafs, p_offsetSrc, m_dataLeafs, p_offsetDst, p_endSrc - p_offsetSrc);
				m_numberOfEntries = (short) (p_offsetDst + p_endSrc - p_offsetSrc);
			} else {
				aux1 = new long[m_keys.length];
				System.arraycopy(p_node.m_keys, 0, aux1, 0, p_node.m_numberOfEntries);
				System.arraycopy(m_keys, 0, aux1, p_node.m_numberOfEntries, m_numberOfEntries);
				m_keys = aux1;

				aux2 = new byte[m_dataLeafs.length];
				System.arraycopy(p_node.m_dataLeafs, 0, aux2, 0, p_node.m_numberOfEntries);
				System.arraycopy(m_dataLeafs, 0, aux2, p_node.m_numberOfEntries, m_numberOfEntries);
				m_dataLeafs = aux2;

				m_numberOfEntries += p_node.m_numberOfEntries;
			}
		}

		/**
		 * Changes an entry
		 * @param p_chunkID
		 *            the ChunkID
		 * @param p_rangeID
		 *            the backup range ID
		 * @param p_index
		 *            the index of given entry in this node
		 */
		private void changeEntry(final long p_chunkID, final byte p_rangeID, final int p_index) {

			if (p_chunkID == getChunkID(p_index)) {
				m_keys[p_index] = p_chunkID;
				m_dataLeafs[p_index] = p_rangeID;
			}
		}

		/**
		 * Removes the entry with given ChunkID
		 * @param p_chunkID
		 *            ChunkID of the entry that has to be deleted
		 * @return p_chunkID or (-1) if there is no entry for p_chunkID in this node
		 */
		private long removeEntry(final long p_chunkID) {
			long ret = -1;
			int index;

			index = this.indexOf(p_chunkID);
			if (0 <= index) {
				ret = getChunkID(index);

				System.arraycopy(m_keys, index + 1, m_keys, index, m_numberOfEntries - index - 1);
				System.arraycopy(m_dataLeafs, index + 1, m_dataLeafs, index, m_numberOfEntries - index - 1);

				m_numberOfEntries--;
			}

			return ret;
		}

		/**
		 * Removes the entry with given index
		 * @param p_index
		 *            the index of the entry that has to be deleted
		 * @return ChunkID or (-1) if p_index is to large
		 */
		private long removeEntry(final int p_index) {
			long ret = -1;

			if (p_index < m_numberOfEntries) {
				ret = getChunkID(p_index);

				System.arraycopy(m_keys, p_index + 1, m_keys, p_index, m_numberOfEntries - p_index);
				System.arraycopy(m_dataLeafs, p_index + 1, m_dataLeafs, p_index, m_numberOfEntries - p_index);
				m_numberOfEntries--;
			}

			return ret;
		}

		/**
		 * Returns the number of entries
		 * @return the number of entries
		 */
		private int getNumberOfEntries() {
			return m_numberOfEntries;
		}

		/**
		 * Returns the child with given index
		 * @param p_index
		 *            the index
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
		 * @param p_child
		 *            the child
		 * @return the index of the given child, if it is contained in the array, (-(insertion point) - 1) otherwise
		 */
		private int indexOf(final Node p_child) {
			int ret = -1;
			int low;
			int high;
			int mid;
			long chunkID;
			long midVal;

			chunkID = p_child.getChunkID(0);
			low = 0;
			high = m_numberOfChildren - 1;

			while (low <= high) {
				mid = low + high >>> 1;
			midVal = m_children[mid].getChunkID(0);

			if (midVal < chunkID) {
				low = mid + 1;
			} else if (midVal > chunkID) {
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
		 * @param p_child
		 *            the child
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
		 * @param p_node
		 *            the other node
		 * @param p_offsetSrc
		 *            the offset in source array
		 * @param p_endSrc
		 *            the end of source array
		 * @param p_offsetDst
		 *            the offset in destination array or -1 if the source array has to be prepended
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
		 * @param p_child
		 *            the child
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
		 * @param p_index
		 *            the index
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
		 * @return the number of children
		 */
		private int getNumberOfChildren() {
			return m_numberOfChildren;
		}

		/**
		 * Prints the node
		 * @return String interpretation of the node
		 */
		@Override
		public String toString() {
			StringBuilder ret;

			ret = new StringBuilder();

			ret.append("entries=[");
			for (int i = 0; i < getNumberOfEntries(); i++) {
				ret.append("(ChunkID: " + getChunkID(i) + " location: " + getRangeID(i) + ")");
				if (i < getNumberOfEntries() - 1) {
					ret.append(", ");
				}
			}
			ret.append("]\n");

			if (null != m_parent) {
				ret.append("parent=[");
				for (int i = 0; i < m_parent.getNumberOfEntries(); i++) {
					ret.append("(ChunkID: " + getChunkID(i) + " location: " + getRangeID(i) + ")");
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
	 * Auxiliary object to return ChunkID and backup range ID at once
	 * @author Kevin Beineke
	 *         13.06.2013
	 */
	private static final class Entry implements Serializable {

		// Constants
		private static final long serialVersionUID = -7000053901808777917L;

		// Attributes
		private long m_chunkID;
		private byte m_rangeID;

		// Constructors
		/**
		 * Creates an instance of Entry
		 * @param p_chunkID
		 *            the ChunkID
		 * @param p_rangeID
		 *            the backup range ID
		 */
		Entry(final long p_chunkID, final byte p_rangeID) {
			m_chunkID = p_chunkID;
			m_rangeID = p_rangeID;
		}

		/**
		 * Returns the ChunkID
		 * @return the ChunkID
		 */
		public long getChunkID() {
			return m_chunkID;
		}

		/**
		 * Returns the backup range ID
		 * @return the backup range ID
		 */
		public byte getRangeID() {
			return m_rangeID;
		}

		/**
		 * Prints the entry
		 * @return String interpretation of the entry
		 */
		@Override
		public String toString() {
			return "(ChunkID: " + m_chunkID + ", location: " + m_rangeID + ")";
		}
	}
}

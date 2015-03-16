
package de.uniduesseldorf.dxram.core.lookup.storage;

import de.uniduesseldorf.dxram.utils.Contract;


/**
 * Btree to store ranges, experimental version for evaluation
 *         13.06.2013
 */
public final class OIDTreeTest {

	// Attributes
	private short m_minEntries;
	private short m_minChildren;
	private short m_maxEntries;
	private short m_maxChildren;

	private Node m_root;
	private int m_size;
	private short m_creator;

	private Entry m_changedEntry;

	// Constructors
	/**
	 * Creates an instance of OIDTree
	 * @param p_order
	 *            order of the btree
	 * @param p_creator
	 *            the creator
	 */
	public OIDTreeTest(final short p_order, final short p_creator) {

		m_minEntries = p_order;
		m_minChildren = (short)(m_minEntries + 1);
		m_maxEntries = (short)(2 * m_minEntries);
		m_maxChildren = (short)(m_maxEntries + 1);

		m_root = null;
		m_size = -1;
		m_creator = p_creator;

		m_changedEntry = null;

		createOrReplaceEntry((long)(Math.pow(2, 32) * 1000), p_creator);
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

		if (null == m_root) {
			ret = m_creator;
		} else {
			ret = getNodeIDOrSuccessorsNodeID(p_oid & 0x0000FFFFFFFFFFFFL);
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
		Node node;

		lid = p_oid & 0x0000FFFFFFFFFFFFL;

		node = createOrReplaceEntry(lid, p_nid);

		mergeWithPredecessorOrBound(lid, p_nid, node);

		mergeWithSuccessor(lid, p_nid);
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
		Node startNode;

		startLid = p_start & 0x0000FFFFFFFFFFFFL;
		endLid = p_end & 0x0000FFFFFFFFFFFFL;

		startNode = createOrReplaceEntry(startLid, p_nid);

		mergeWithPredecessorOrBound(startLid, p_nid, startNode);

		createOrReplaceEntry(endLid, p_nid);

		removeEntriesWithinRange(startLid, endLid);

		mergeWithSuccessor(endLid, p_nid);
	}

	/**
	 * Creates a new entry or replaces the old one
	 * @param p_lid
	 *            the lid
	 * @param p_nodeID
	 *            the nodeID
	 * @return the node in which the entry is stored
	 */
	private Node createOrReplaceEntry(final long p_lid, final short p_nodeID) {
		Node ret = null;
		Node node;
		int index;
		int size;

		if (null == m_root) {
			m_root = new Node(null, m_maxEntries, m_maxChildren);
			m_root.addEntry(p_lid, p_nodeID, 0);
			ret = m_root;
		} else {
			node = m_root;
			while (true) {
				if (0 == node.getNumberOfChildren()) {
					index = node.indexOf(p_lid);
					if (0 <= index) {
						m_changedEntry = new Entry(node.getLid(index), node.getNodeID(index));
						node.changeEntry(p_lid, p_nodeID, index);
					} else {
						m_changedEntry = null;
						node.addEntry(p_lid, p_nodeID, (index * -1) - 1);
						if (m_maxEntries < node.getNumberOfEntries()) {
							// Need to split up
							node = split(p_lid, node);
						}
					}
					break;
					/* Sequential search
					for (int i = 0;i < node.getNumberOfEntries();i++) {
						currentLid = node.getLid(i);
						if (p_lid == currentLid) {
							m_changedEntry = new Entry(node.getLid(i), node.getNodeID(i), node.isEndOfRange(i));
							node.changeEntry(p_lid, p_nodeID, p_isEndOfRange, i);
							ready = true;
							break;
						}
					}
					if (!ready) {
						node.addEntry(p_lid, p_nodeID, p_isEndOfRange);
						if (node.getNumberOfEntries() > m_maxEntries) {
							// Need to split up
							node = split(p_lid, node);
						}
						break;
					}*/
				} else {
					if (p_lid < node.getLid(0)) {
						node = node.getChild(0);
						continue;
					}

					size = node.getNumberOfEntries();
					if (p_lid > node.getLid(size - 1)) {
						node = node.getChild(size);
						continue;
					}

					index = node.indexOf(p_lid);
					if (0 <= index) {
						m_changedEntry = new Entry(node.getLid(index), node.getNodeID(index));
						node.changeEntry(p_lid, p_nodeID, index);
						break;
					} else {
						node = node.getChild((index * -1) - 1);
					}
					/* Sequential search
					for (int i = 1;i < node.getNumberOfChildren();i++) {
						prev = node.getLid(i - 1);
						if (p_lid == prev) {
							m_changedEntry =
									new Entry(node.getLid(i - 1), node.getNodeID(i - 1), node.isEndOfRange(i - 1));
							node.changeEntry(p_lid, p_nodeID, p_isEndOfRange, i - 1);
							ready = true;
							break;
						}
						next = node.getLid(i);
						if ((p_lid > prev) && (p_lid < next)) {
							node = node.getChild(i);
							break;
						}
					}*/
				}
			}

			ret = node;
		}
		m_size++;
		m_changedEntry = null;

		return ret;
	}

	/**
	 * Merges the object or range with predecessor
	 * @param p_lid
	 *            the lid
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_node
	 *            anchor node
	 */
	private void mergeWithPredecessorOrBound(final long p_lid, final short p_nodeID, final Node p_node) {
		Entry predecessor;
		Entry successor;

		predecessor = getPredecessorsEntry(p_lid, p_node);
		if (null == predecessor) {
			createOrReplaceEntry(p_lid - 1, m_creator);
		} else {
			if ((p_lid - 1) == predecessor.getLid()) {
				if (p_nodeID == predecessor.getNodeID()) {
					remove(predecessor.getLid(), getPredecessorsNode(p_lid, p_node));
				}
			} else {
				successor = getSuccessorsEntry(p_lid, p_node);
				if (null == m_changedEntry) {
					// Successor is end of range
					if (p_nodeID != successor.getNodeID()) {
						createOrReplaceEntry(p_lid - 1, successor.getNodeID());
					} else {
						// New Object is in range that already was migrated to the same destination
						remove(p_lid, p_node);
					}
				} else {
					if (p_nodeID != m_changedEntry.getNodeID()) {
						createOrReplaceEntry(p_lid - 1, m_changedEntry.getNodeID());
					}
				}
			}
		}
	}

	/**
	 * Merges the object or range with successor
	 * @param p_lid
	 *            the lid
	 * @param p_nodeID
	 *            the NodeID
	 */
	private void mergeWithSuccessor(final long p_lid, final short p_nodeID) {
		Node node;
		Entry successor;

		node = getNodeOrSuccessorsNode(p_lid);
		successor = getSuccessorsEntry(p_lid, node);
		if ((null != successor) && (p_nodeID == successor.getNodeID())) {
			remove(p_lid, node);
		}
	}


	/**
	 * Returns the node in which the next entry to given lid (could be the lid itself) is stored
	 * @param p_lid
	 *            lid whose node is searched
	 * @return node in which lid is stored if lid is in tree or successors node, null if there is no successor
	 */
	private Node getNodeOrSuccessorsNode(final long p_lid) {
		Node ret;
		int size;
		int index;
		long greater;

		ret = m_root;

		while (true) {
			if (p_lid < ret.getLid(0)) {
				if (0 < ret.getNumberOfChildren()) {
					ret = ret.getChild(0);
					continue;
				} else {
					break;
				}
			}

			size = ret.getNumberOfEntries();
			greater = ret.getLid(size - 1);
			if (p_lid > greater) {
				if (size < ret.getNumberOfChildren()) {
					ret = ret.getChild(size);
					continue;
				} else {
					ret = getSuccessorsNode(greater, ret);
					break;
				}
			}

			index = ret.indexOf(p_lid);
			if (0 <= index) {
				break;
			} else {
				index = (index * -1) - 1;
				if (index < ret.getNumberOfChildren()) {
					ret = ret.getChild(index);
				} else {
					break;
				}
			}
			/* Sequential search
			for (int i = 0;i < size;i++) {
				currentLid = ret.getLid(i);
				if (currentLid == p_lid) {
					ready = true;
					break;
				}

				next = i + 1;
				if (next <= (size - 1)) {
					nextLid = ret.getLid(next);
					if ((currentLid < p_lid) && (nextLid > p_lid)) {
						if (next < ret.getNumberOfChildren()) {
							ret = ret.getChild(next);
							break;
						} else {
							ready = true;
							break;
						}
					}
				}
			}*/
		}

		return ret;
	}

	/**
	 * Returns next lid to given lid (could be the lid itself)
	 * @param p_lid
	 *            the lid
	 * @return p_lid if p_lid is in btree or successor of p_lid, (-1) if there is no successor
	 */
	private long getLidOrSuccessorsLid(final long p_lid) {
		long ret = -1;
		int index;
		Node node;

		node = getNodeOrSuccessorsNode(p_lid);
		if (node != null) {
			index = node.indexOf(p_lid);
			if (0 <= index) {
				ret = node.getLid(index);
			} else {
				ret = node.getLid((index * -1) - 1);
			}
			/*for (int i = 0;i < node.getNumberOfEntries();i++) {
				currentLid = node.getLid(i);
				if (p_lid <= currentLid) {
					ret = currentLid;
					break;
				}
			}*/
		}

		return ret;
	}

	/**
	 * Returns the location and backup nodes of next lid to given lid (could be the lid itself)
	 * @param p_lid
	 *            the lid whose NodeID are searched
	 * @return NodeID of p_lid if p_lid is in btree or successors NodeID
	 */
	private short getNodeIDOrSuccessorsNodeID(final long p_lid) {
		short ret = -1;
		int index;
		Node node;

		node = getNodeOrSuccessorsNode(p_lid);
		if (node != null) {
			index = node.indexOf(p_lid);
			if (0 <= index) {
				ret = node.getNodeID(index);
			} else {
				ret = node.getNodeID((index * -1) - 1);
			}
			/*for (int i = 0;i < node.getNumberOfEntries();i++) {
				currentLid = node.getLid(i);
				if (p_lid <= currentLid) {
					ret = node.getNodeID(i);
					break;
				}
			}*/
		}

		return ret;
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

		successor = getLidOrSuccessorsLid(p_start);
		while ((-1 != successor) && (successor < p_end)) {
			remove(successor);
			successor = getLidOrSuccessorsLid(p_start);
		}
	}

	/**
	 * Returns the node in which the predecessor is
	 * @param p_lid
	 *            lid whose predecessor's node is searched
	 * @param p_node
	 *            anchor node
	 * @return the node in which the predecessor of p_lid is or null if there is no predecessor
	 */
	private Node getPredecessorsNode(final long p_lid, final Node p_node) {
		int index;
		Node ret = null;
		Node node;
		Node parent;

		Contract.checkNotNull(p_node, "Node null");

		node = p_node;

		if (p_lid == node.getLid(0)) {
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
					while ((parent != null) && (p_lid < parent.getLid(0))) {
						parent = parent.getParent();
					}
					ret = parent;
				}
			}
		} else {
			index = node.indexOf(p_lid);
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
			/* Sequential search
			for (int i = 1;i < node.getNumberOfEntries();i++) {
				if (p_lid == node.getLid(i)) {
					if (i <= node.getNumberOfChildren()) {
						// Get maximum in child tree
						node = node.getChild(i);
						while (node.getNumberOfEntries() < node.getNumberOfChildren()) {
							node = node.getChild(node.getNumberOfChildren() - 1);
						}
					}

					ret = node;
					break;
				}
			}*/
		}

		return ret;
	}

	/**
	 * Returns the entry of the predecessor
	 * @param p_lid
	 *            the lid whose predecessor is searched
	 * @param p_node
	 *            anchor node
	 * @return the entry of p_lid's predecessor or null if there is no predecessor
	 */
	private Entry getPredecessorsEntry(final long p_lid, final Node p_node) {
		Entry ret = null;
		Node predecessorsNode;
		long predecessorsLid;

		predecessorsNode = getPredecessorsNode(p_lid, p_node);
		if (predecessorsNode != null) {
			for (int i = predecessorsNode.getNumberOfEntries() - 1;i >= 0;i--) {
				predecessorsLid = predecessorsNode.getLid(i);
				if (p_lid > predecessorsLid) {
					ret = new Entry(predecessorsLid, predecessorsNode.getNodeID(i));
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Returns the node in which the successor is
	 * @param p_lid
	 *            lid whose successor's node is searched
	 * @param p_node
	 *            anchor node
	 * @return the node in which the successor of p_lid is or null if there is no successor
	 */
	private Node getSuccessorsNode(final long p_lid, final Node p_node) {
		int index;
		Node ret = null;
		Node node;
		Node parent;

		Contract.checkNotNull(p_node, "Node null");

		node = p_node;

		if (p_lid == node.getLid(node.getNumberOfEntries() - 1)) {
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
					while ((parent != null) && (p_lid > parent.getLid(parent.getNumberOfEntries() - 1))) {
						parent = parent.getParent();
					}
					ret = parent;
				}
			}
		} else {
			index = node.indexOf(p_lid);
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
			/* Sequential search
			for (int i = 0;i < (node.getNumberOfEntries() - 1);i++) {
				if (p_lid == node.getLid(i)) {
					if (i < node.getNumberOfChildren()) {
						// Get minimum in child tree
						node = node.getChild(i + 1);
						while (0 < node.getNumberOfChildren()) {
							node = node.getChild(0);
						}
					}

					ret = node;
					break;
				}
			}*/
		}

		return ret;
	}

	/**
	 * Returns the entry of the successor
	 * @param p_lid
	 *            the lid whose successor is searched
	 * @param p_node
	 *            anchor node
	 * @return the entry of p_lid's successor or null if there is no successor
	 */
	private Entry getSuccessorsEntry(final long p_lid, final Node p_node) {
		Entry ret = null;
		Node successorsNode;
		long successorsLid;

		successorsNode = getSuccessorsNode(p_lid, p_node);
		if (successorsNode != null) {
			for (int i = 0;i < successorsNode.getNumberOfEntries();i++) {
				successorsLid = successorsNode.getLid(i);
				if (p_lid < successorsLid) {
					ret = new Entry(successorsLid, successorsNode.getNodeID(i));
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Splits down the middle if node is greater than maxEntries
	 * @param p_lid
	 *            the new lid that causes the splitting
	 * @param p_node
	 *            the node that has to be split
	 * @return the node in which p_lid must be inserted
	 */
	private Node split(final long p_lid, final Node p_node) {
		Node ret;
		Node node;

		int size;
		int medianIndex;
		long medianLid;
		short medianNodeID;

		Node left;
		Node right;
		Node parent;
		Node newRoot;

		node = p_node;

		size = node.getNumberOfEntries();
		medianIndex = size / 2;
		medianLid = node.getLid(medianIndex);
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
			// new root, height of tree is increased
			newRoot = new Node(null, m_maxEntries, m_maxChildren);
			newRoot.addEntry(medianLid, medianNodeID, 0);
			node.setParent(newRoot);
			m_root = newRoot;
			node = m_root;
			node.addChild(left);
			node.addChild(right);
			parent = newRoot;
		} else {
			// Move the median lid up to the parent
			parent = node.getParent();
			parent.addEntry(medianLid, medianNodeID);
			parent.removeChild(node);
			parent.addChild(left);
			parent.addChild(right);

			if (parent.getNumberOfEntries() > m_maxEntries) {
				split(p_lid, parent);
			}
		}

		if (p_lid < medianLid) {
			ret = left;
		} else if (p_lid > medianLid) {
			ret = right;
		} else {
			ret = parent;
		}

		return ret;
	}

	/**
	 * Removes given p_lid
	 * @param p_lid
	 *            the lid
	 * @return p_lid or (-1) if there is no entry for p_lid
	 */
	private long remove(final long p_lid) {
		long ret;
		Node node;

		node = getNodeOrSuccessorsNode(p_lid);
		ret = remove(p_lid, node);

		return ret;
	}

	/**
	 * Removes the p_lid from given node and checks invariants
	 * @param p_lid
	 *            the lid
	 * @param p_node
	 *            the node in which p_lid should be stored
	 * @return p_lid or (-1) if there is no entry for p_lid
	 */
	private long remove(final long p_lid, final Node p_node) {
		long ret = -1;
		int index;
		Node greatest;
		long replaceLid;
		short replaceNodeID;

		Contract.checkNotNull(p_node, "Node null");

		index = p_node.indexOf(p_lid);
		if (0 <= index) {
			ret = p_node.removeEntry(p_lid);
			if (0 == p_node.getNumberOfChildren()) {
				// Leaf node
				if ((null != p_node.getParent()) && (p_node.getNumberOfEntries() < m_minEntries)) {
					combined(p_node);
				} else if ((null == p_node.getParent()) && (0 == p_node.getNumberOfEntries())) {
					// Removing root node with no keys or children
					m_root = null;
				}
			} else {
				// Internal node
				greatest = p_node.getChild(index);
				while (0 < greatest.getNumberOfChildren()) {
					greatest = greatest.getChild(greatest.getNumberOfChildren() - 1);
				}
				replaceLid = -1;
				replaceNodeID = -1;
				if (0 < greatest.getNumberOfEntries()) {
					replaceNodeID = greatest.getNodeID(greatest.getNumberOfEntries() - 1);
					replaceLid = greatest.removeEntry(greatest.getNumberOfEntries() - 1);
				}
				p_node.addEntry(replaceLid, replaceNodeID);
				if ((null != greatest.getParent()) && (greatest.getNumberOfEntries() < m_minEntries)) {
					combined(greatest);
				}
				if (greatest.getNumberOfChildren() > m_maxChildren) {
					split(p_lid, greatest);
				}
			}
			m_size--;
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

		long removeLid;
		int prev;
		short parentNodeID;
		long parentLid;

		short neighborNodeID;
		long neighborLid;

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
		if ((null != rightNeighbor) && (rightNeighborSize > m_minEntries)) {
			// Try to borrow from right neighbor
			removeLid = rightNeighbor.getLid(0);
			prev = (parent.indexOf(removeLid) * -1) - 2;
			parentNodeID = parent.getNodeID(prev);
			parentLid = parent.removeEntry(prev);

			neighborNodeID = rightNeighbor.getNodeID(0);
			neighborLid = rightNeighbor.removeEntry(0);

			p_node.addEntry(parentLid, parentNodeID);
			parent.addEntry(neighborLid, neighborNodeID);
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

			if ((null != leftNeighbor) && (leftNeighborSize > m_minEntries)) {
				// Try to borrow from left neighbor
				removeLid = leftNeighbor.getLid(leftNeighbor.getNumberOfEntries() - 1);
				prev = (parent.indexOf(removeLid) * -1) - 1;
				parentNodeID = parent.getNodeID(prev);
				parentLid = parent.removeEntry(prev);

				neighborNodeID = leftNeighbor.getNodeID(leftNeighbor.getNumberOfEntries() - 1);
				neighborLid = leftNeighbor.removeEntry(leftNeighbor.getNumberOfEntries() - 1);

				p_node.addEntry(parentLid, parentNodeID);
				parent.addEntry(neighborLid, neighborNodeID);
				if (0 < leftNeighbor.getNumberOfChildren()) {
					p_node.addChild(leftNeighbor.removeChild(leftNeighbor.getNumberOfChildren() - 1));
				}
			} else if ((null != rightNeighbor) && (0 < parent.getNumberOfEntries())) {
				// Cannot borrow from neighbors, try to combined with right neighbor
				removeLid = rightNeighbor.getLid(0);
				prev = (parent.indexOf(removeLid) * -1) - 2;
				parentNodeID = parent.getNodeID(prev);
				parentLid = parent.removeEntry(prev);
				parent.removeChild(rightNeighbor);
				p_node.addEntry(parentLid, parentNodeID);

				p_node.addEntries(rightNeighbor, 0, rightNeighbor.getNumberOfEntries(), p_node.getNumberOfEntries());
				p_node.addChildren(rightNeighbor, 0, rightNeighbor.getNumberOfChildren(), p_node.getNumberOfChildren());

				if ((null != parent.getParent()) && (parent.getNumberOfEntries() < m_minEntries)) {
					// Removing key made parent too small, combined up tree
					combined(parent);
				} else if (0 == parent.getNumberOfEntries()) {
					// Parent no longer has keys, make this node the new root which decreases the height of the tree
					p_node.setParent(null);
					m_root = p_node;
				}
			} else if ((null != leftNeighbor) && (0 < parent.getNumberOfEntries())) {
				// Cannot borrow from neighbors, try to combined with left neighbor
				removeLid = leftNeighbor.getLid(leftNeighbor.getNumberOfEntries() - 1);
				prev = (parent.indexOf(removeLid) * -1) - 1;
				parentNodeID = parent.getNodeID(prev);
				parentLid = parent.removeEntry(prev);
				parent.removeChild(leftNeighbor);
				p_node.addEntry(parentLid, parentNodeID);
				p_node.addEntries(leftNeighbor, 0, leftNeighbor.getNumberOfEntries(), -1);
				p_node.addChildren(leftNeighbor, 0, leftNeighbor.getNumberOfChildren(), -1);

				if ((null != parent.getParent()) && (parent.getNumberOfEntries() < m_minEntries)) {
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
		return m_size;
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
		long p;
		long n;
		int childrenSize;
		Node first;
		Node last;
		Node c;

		numberOfEntries = p_node.getNumberOfEntries();

		if (1 < numberOfEntries) {
			// Make sure the keys are sorted
			for (int i = 1;i < numberOfEntries;i++) {
				p = p_node.getLid(i - 1);
				n = p_node.getLid(i);
				if (p > n) {
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
				} else if (numberOfEntries != (childrenSize - 1)) {
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
			if (first.getLid(first.getNumberOfEntries() - 1) > p_node.getLid(0)) {
				ret = false;
			}

			last = p_node.getChild(p_node.getNumberOfChildren() - 1);
			// The last child's first key should be greater than the node's last key
			if (last.getLid(0) < p_node.getLid(p_node.getNumberOfEntries() - 1)) {
				ret = false;
			}

			// Check that each node's first and last key holds it's invariance
			for (int i = 1;i < p_node.getNumberOfEntries();i++) {
				p = p_node.getLid(i - 1);
				n = p_node.getLid(i);
				c = p_node.getChild(i);
				if (p > c.getLid(0)) {
					ret = false;
					break;
				}
				if (n < c.getLid(c.getNumberOfEntries() - 1)) {
					ret = false;
					break;
				}
			}

			for (int i = 0;i < p_node.getNumberOfChildren();i++) {
				c = p_node.getChild(i);
				if (!validateNode(c)) {
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
			ret = getString(m_root, "", true);
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
		for (int i = 0;i < p_node.getNumberOfEntries();i++) {
			ret.append("(lid: " + p_node.getLid(i) + " nid: " + p_node.getNodeID(i) + ")");
			if (i < (p_node.getNumberOfEntries() - 1)) {
				ret.append(", ");
			}
		}
		ret.append("\n");

		if (null != p_node.getChild(0)) {
			for (int i = 0;i < (p_node.getNumberOfChildren() - 1);i++) {
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
	private static final class Node implements Comparable<Node> {

		// Attributes
		private Node m_parent;

		private long[] m_keys;
		private short[] m_dataLeafs;
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
			m_dataLeafs = new short[p_maxEntries + 1];
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

			if (this.getLid(0) < p_cmp.getLid(0)) {
				ret = -1;
			} else if (this.getLid(0) > p_cmp.getLid(0)) {
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
		 * Returns the lid to given index
		 * @param p_index
		 *            the index
		 * @return the lid to given index
		 */
		private long getLid(final int p_index) {
			return m_keys[p_index] & 0x0000FFFFFFFFFFFFL;
		}

		/**
		 * Returns the data leaf to given index
		 * @param p_index
		 *            the index
		 * @return the data leaf to given index
		 */
		private short getNodeID(final int p_index) {
			return m_dataLeafs[p_index];
		}

		/**
		 * Returns the index for given lid. Uses the binary search algorithm from
		 * java.util.Arrays adapted to our needs
		 * @param p_lid
		 *            the lid
		 * @return the index for given lid, if it is contained in the array, (-(insertion point) - 1) otherwise
		 */
		private int indexOf(final long p_lid) {
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

				if (midVal < p_lid) {
					low = mid + 1;
				} else if (midVal > p_lid) {
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
		 * @param p_lid
		 *            the lid
		 * @param p_nodeID
		 *            the NodeID
		 */
		private void addEntry(final long p_lid, final short p_nodeID) {
			int index;

			index = (this.indexOf(p_lid) * -1) - 1;

			System.arraycopy(m_keys, index, m_keys, index + 1, m_numberOfEntries - index);
			System.arraycopy(m_dataLeafs, index, m_dataLeafs, index + 1, m_numberOfEntries - index);

			m_keys[index] = p_lid;
			m_dataLeafs[index] = p_nodeID;

			m_numberOfEntries++;
		}

		/**
		 * Adds an entry
		 * @param p_lid
		 *            the lid
		 * @param p_nodeID
		 *            the NodeID
		 * @param p_index
		 *            the index to store the element at
		 */
		private void addEntry(final long p_lid, final short p_nodeID, final int p_index) {
			System.arraycopy(m_keys, p_index, m_keys, p_index + 1, m_numberOfEntries - p_index);
			System.arraycopy(m_dataLeafs, p_index, m_dataLeafs, p_index + 1, m_numberOfEntries - p_index);

			m_keys[p_index] = p_lid;
			m_dataLeafs[p_index] = p_nodeID;

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
			short[] aux2;

			if (-1 != p_offsetDst) {
				System.arraycopy(p_node.m_keys, p_offsetSrc, m_keys, p_offsetDst, p_endSrc - p_offsetSrc);
				System.arraycopy(p_node.m_dataLeafs, p_offsetSrc, m_dataLeafs, p_offsetDst, p_endSrc - p_offsetSrc);
				m_numberOfEntries = (short)((p_offsetDst + p_endSrc) - p_offsetSrc);
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
		 * @param p_lid
		 *            the lid
		 * @param p_nodeID
		 *            the NodeID
		 * @param p_index
		 *            the index of given entry in this node
		 */
		private void changeEntry(final long p_lid, final short p_nodeID, final int p_index) {

			if (p_lid == this.getLid(p_index)) {
				m_keys[p_index] = p_lid;
				m_dataLeafs[p_index] = p_nodeID;
			}
		}

		/**
		 * Removes the entry with given lid
		 * @param p_lid
		 *            lid of the entry that has to be deleted
		 * @return p_lid or (-1) if there is no entry for p_lid in this node
		 */
		private long removeEntry(final long p_lid) {
			long ret = -1;
			int index;

			index = this.indexOf(p_lid);
			if (0 <= index) {
				ret = this.getLid(index);

				System.arraycopy(m_keys, index + 1, m_keys, index, m_numberOfEntries - index);
				System.arraycopy(m_dataLeafs, index + 1, m_dataLeafs, index, m_numberOfEntries - index);
				m_numberOfEntries--;
			}

			return ret;
		}

		/**
		 * Removes the entry with given index
		 * @param p_index
		 *            the index of the entry that has to be deleted
		 * @return p_lid or (-1) if p_index is to large
		 */
		private long removeEntry(final int p_index) {
			long ret = -1;

			if (p_index < m_numberOfEntries) {
				ret = this.getLid(p_index);

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
			long lid;
			long midVal;

			lid = p_child.getLid(0);
			low = 0;
			high = m_numberOfChildren - 1;

			while (low <= high) {
				mid = low + high >>> 1;
				midVal = m_children[mid].getLid(0);

				if (midVal < lid) {
					low = mid + 1;
				} else if (midVal > lid) {
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

			index = (this.indexOf(p_child) * -1) - 1;

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

				for (Node child : m_children) {
					if (null == child) {
						break;
					}
					child.setParent(this);
				}
				m_numberOfChildren = (short)((p_offsetDst + p_endSrc) - p_offsetSrc);
			} else {
				aux = new Node[m_children.length];
				System.arraycopy(p_node.m_children, 0, aux, 0, p_node.m_numberOfChildren);

				for (Node child : aux) {
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
			for (int i = 0;i < this.getNumberOfEntries();i++) {
				ret.append("(lid: " + this.getLid(i) + " location: " + this.getNodeID(i) + ")");
				if (i < (this.getNumberOfEntries() - 1)) {
					ret.append(", ");
				}
			}
			ret.append("]\n");

			if (null != m_parent) {
				ret.append("parent=[");
				for (int i = 0;i < m_parent.getNumberOfEntries();i++) {
					ret.append("(lid: " + this.getLid(i) + " location: " + this.getNodeID(i) + ")");
					if (i < (m_parent.getNumberOfEntries() - 1)) {
						ret.append(", ");
					}
				}
				ret.append("]\n");
			}

			if (null != m_children) {
				ret.append("numberOfEntries=");
				ret.append(this.getNumberOfEntries());
				ret.append(" children=");
				ret.append(this.getNumberOfChildren());
				ret.append("\n");
			}

			return ret.toString();
		}
	}

	/**
	 * Auxiliary object to return lid and NodeID at once
	 * @author Kevin Beineke
	 *         13.06.2013
	 */
	private static final class Entry {

		// Attributes
		private long m_lid;
		private short m_nodeID;

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
			return m_nodeID;
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

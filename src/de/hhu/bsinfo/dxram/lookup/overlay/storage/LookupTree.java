/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.lookup.overlay.storage;

import java.io.Serializable;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.utils.NodeID;
import de.hhu.bsinfo.dxram.lookup.LookupState;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Btree to store ranges. Backup nodes are stored in an ArrayList to improve access times.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 13.06.2013
 * @author Michael Birkhoff, michael.birkhoff@hhu.de
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

    private Entry m_changedEntry;

    // Constructors

    /**
     * Creates an instance of LookupTree
     */
    public LookupTree() {
        m_root = null;
        m_size = 0;

        m_changedEntry = null;
    }

    /**
     * Creates an instance of LookupTree
     *
     * @param p_order
     *         order of the btree
     */
    LookupTree(final short p_order, final short p_creator) {
        // too small order for BTree
        assert p_order > 1;

        m_minEntries = p_order;
        m_minChildren = (short) (m_minEntries + 1);
        m_maxEntries = (short) (2 * m_minEntries);
        m_maxChildren = (short) (m_maxEntries + 1);

        m_root = null;
        m_size = 0;

        m_creator = p_creator;

        m_changedEntry = null;
    }

    /**
     * Prints one node of the btree and walks down the btree recursively
     *
     * @param p_node
     *         the current node
     * @param p_prefix
     *         the prefix to use
     * @param p_isTail
     *         defines wheter the node is the tail
     * @return String interpretation of the tree
     */
    private static String getString(final Node p_node, final String p_prefix, final boolean p_isTail) {
        StringBuilder ret;
        Node obj;

        ret = new StringBuilder();

        ret.append(p_prefix);
        if (p_isTail) {
            ret.append("└── ");
        } else {
            ret.append("├── ");
        }
        ret.append('[');
        ret.append(p_node.getNumberOfEntries());
        ret.append(", ");
        ret.append(p_node.getNumberOfChildren());
        ret.append("] ");
        for (int i = 0; i < p_node.getNumberOfEntries(); i++) {
            ret.append("(LocalID: ");
            ret.append(ChunkID.toHexString(p_node.getLocalID(i)));
            ret.append(" NodeID: ");
            ret.append(NodeID.toHexString(p_node.getNodeID(i)));
            ret.append(')');
            if (i < p_node.getNumberOfEntries() - 1) {
                ret.append(", ");
            }
        }
        ret.append('\n');

        if (p_node.getChild(0) != null) {
            for (int i = 0; i < p_node.getNumberOfChildren() - 1; i++) {
                obj = p_node.getChild(i);
                if (p_isTail) {
                    ret.append(getString(obj, p_prefix + "    ", false));
                } else {
                    ret.append(getString(obj, p_prefix + "│   ", false));
                }
            }
            if (p_node.getNumberOfChildren() >= 1) {
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
     * Adds one node as pair of long and short from the btree to the byte buffer and walks down the btree recursively
     *
     * @param p_node
     *         the current node
     * @param p_exporter
     *         bytebuffer to write into
     */
    private static void serialize(final Node p_node, final Exporter p_exporter) {
        Node obj;

        for (int i = 0; i < p_node.getNumberOfEntries(); i++) {

            p_exporter.writeLong(p_node.getLocalID(i));
            p_exporter.writeShort(p_node.getNodeID(i));

        }

        for (int i = 0; i < p_node.getNumberOfChildren() - 1; i++) {
            obj = p_node.getChild(i);

            if (obj != null) {
                serialize(obj, p_exporter);
            }

        }
        if (p_node.getNumberOfChildren() >= 1) {
            obj = p_node.getChild(p_node.getNumberOfChildren() - 1);

            serialize(obj, p_exporter);
        }
    }

    /**
     * Returns the node in which the predecessor is
     *
     * @param p_localID
     *         LocalID whose predecessor's node is searched
     * @param p_node
     *         anchor node
     * @return the node in which the predecessor of p_localID is or null if there is no predecessor
     */
    private static Node getPredecessorsNode(final long p_localID, final Node p_node) {
        int index;
        Node ret = null;
        Node node;
        Node parent;

        assert p_node != null;

        node = p_node;

        if (p_localID == node.getLocalID(0)) {
            if (node.getNumberOfChildren() > 0) {
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
            if (index >= 0) {
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
     * @param p_localID
     *         the LocalID whose predecessor is searched
     * @param p_node
     *         anchor node
     * @return the entry of p_localID's predecessor or null if there is no predecessor
     */
    private static Entry getPredecessorsEntry(final long p_localID, final Node p_node) {
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
     * @param p_localID
     *         LocalID whose successor's node is searched
     * @param p_node
     *         anchor node
     * @return the node in which the successor of p_localID is or null if there is no successor
     */
    private static Node getSuccessorsNode(final long p_localID, final Node p_node) {
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
                while (node.getNumberOfChildren() > 0) {
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
            if (index >= 0) {
                if (index < node.getNumberOfChildren()) {
                    // Get minimum in child tree
                    node = node.getChild(index + 1);
                    while (node.getNumberOfChildren() > 0) {
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
     * @param p_localID
     *         the LocalID whose successor is searched
     * @param p_node
     *         anchor node
     * @return the entry of p_localID's successor or null if there is no successor
     */
    private static Entry getSuccessorsEntry(final long p_localID, final Node p_node) {
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

    @Override
    public void importObject(final Importer p_importer) {
        m_creator = p_importer.readShort();
        m_minEntries = p_importer.readShort();
        m_minChildren = (short) (m_minEntries + 1);
        m_maxEntries = (short) (2 * m_minEntries);
        m_maxChildren = (short) (m_maxEntries + 1);

        int elementsInBTree = p_importer.readInt();
        if (elementsInBTree > 0) {
            for (int i = 0; i < elementsInBTree; i++) {
                long lid = p_importer.readLong();
                short nid = p_importer.readShort();

                createOrReplaceEntry(lid, nid);
            }
        }
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeShort(m_creator);
        p_exporter.writeShort(m_minEntries);

        if (m_root != null) {
            // Push Size
            int numberOfTreeElements = m_size;
            p_exporter.writeInt(numberOfTreeElements);

            // Push elements
            serialize(m_root, p_exporter);
        } else {
            p_exporter.writeInt(-1);
        }
    }

    @Override
    public int sizeofObject() {
        int numberOfBytesWritten = 2 * Short.BYTES;

        numberOfBytesWritten += Integer.BYTES;

        // Size of the b tree list
        // Integer represents the bytes where the size of the list is stored, m_size + 1 for number of entries including the
        // default (LocalID: 0x1000000000000 NodeID: 0xNID), long and short for key and value
        if (m_root != null) {
            numberOfBytesWritten += m_size * (Long.BYTES + Short.BYTES);
        }

        return numberOfBytesWritten;
    }

    /**
     * Validates the btree
     *
     * @return whether the tree is valid or not
     */
    @SuppressWarnings("unused")
    public boolean validate() {
        boolean ret = true;

        if (m_root != null) {
            ret = validateNode(m_root);
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

        if (m_root == null) {
            ret = "Btree has no nodes";
        } else {
            ret = "Size: " + m_size + '\n' + getString(m_root, "", true);
        }

        return ret;
    }

    /**
     * Stores the migration for a single chunk
     *
     * @param p_chunkID
     *         ChunkID of migrated object
     * @param p_nodeID
     *         new primary peer
     * @return true if insertion was successful
     */
    boolean migrate(final long p_chunkID, final short p_nodeID) {
        long localID;
        Node node;

        localID = p_chunkID & 0x0000FFFFFFFFFFFFL;

        assert localID >= 0;

        if (m_root == null) {
            createOrReplaceEntry(ChunkID.MAX_LOCALID, m_creator);
        }

        node = createOrReplaceEntry(localID, p_nodeID);

        mergeWithPredecessorOrBound(localID, p_nodeID, node);

        mergeWithSuccessor(localID, p_nodeID);

        return true;
    }

    /**
     * Stores the migration for a range
     *
     * @param p_startCID
     *         ChunkID of first migrated object
     * @param p_endCID
     *         ChunkID of last migrated object
     * @param p_nodeID
     *         new primary peer
     * @return true if insertion was successful
     */
    boolean migrateRange(final long p_startCID, final long p_endCID, final short p_nodeID) {
        long startLID;
        long endLID;
        Node startNode;

        startLID = p_startCID & 0x0000FFFFFFFFFFFFL;
        endLID = p_endCID & 0x0000FFFFFFFFFFFFL;
        // end larger than start or start smaller than 1
        assert startLID <= endLID && startLID > 0;

        if (m_root == null) {
            createOrReplaceEntry(ChunkID.MAX_LOCALID, m_creator);
        }

        if (startLID == endLID) {
            migrate(p_startCID, p_nodeID);
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
     * Returns the range given ChunkID is in
     *
     * @param p_chunkID
     *         ChunkID of requested object
     * @return the first and last ChunkID of the range
     */
    LookupRange getMetadata(final long p_chunkID) {
        LookupRange ret;
        long[] range;
        short nodeID;
        int index;
        long localID;
        Node node;
        Entry predecessorEntry;

        if (m_root != null) {
            localID = p_chunkID & 0x0000FFFFFFFFFFFFL;
            node = getNodeOrSuccessorsNode(localID);
            index = node.indexOf(localID);
            if (index >= 0) {
                // LocalID was found: Store NodeID and determine successor
                range = new long[2];
                nodeID = node.getNodeID(index);
                Entry successor = getSuccessorsEntry(localID, node);
                if (successor != null) {
                    range[1] = successor.getLocalID();
                } else {
                    range[1] = localID;
                }
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
            ret = new LookupRange(nodeID, range, LookupState.OK);
        } else {
            // Lookup tree is empty -> no migrations
            ret = new LookupRange(m_creator, new long[] {0, (long) (Math.pow(2, 48) - 1)}, LookupState.OK);
        }

        return ret;
    }

    /**
     * Removes multiple chunks from btree
     *
     * @param p_chunkIDs
     *         ChunkIDs of deleted objects
     * @note should always be called if an object is deleted
     */

    void removeObjects(final long... p_chunkIDs) {
        for (long chunkId : p_chunkIDs) {
            remove(chunkId);
        }
    }

    /**
     * Removes given chunk from btree
     *
     * @param p_chunkID
     *         ChunkID of deleted object
     * @note should always be called if an object is deleted
     */
    void remove(final long p_chunkID) {
        int index;
        Node node;
        long localID;
        long currentLID;
        Entry currentEntry;
        Entry predecessor;
        Entry successor;

        localID = p_chunkID & 0x0000FFFFFFFFFFFFL;
        if (m_root != null) {
            node = getNodeOrSuccessorsNode(localID);
            if (node != null) {
                index = node.indexOf(localID);
                if (index >= 0) {
                    // Entry was found
                    currentLID = node.getLocalID(index);
                    predecessor = getPredecessorsEntry(localID, node);
                    currentEntry = new Entry(currentLID, node.getNodeID(index));
                    successor = getSuccessorsEntry(localID, node);
                    if (m_creator != currentEntry.getNodeID() && predecessor != null) {
                        if (localID - 1 == predecessor.getLocalID()) {
                            // Predecessor is direct neighbor: AB
                            // Successor might be direct neighbor or not: ABC or AB___C
                            if (m_creator == successor.getNodeID()) {
                                // Successor is barrier: ABC -> A_C or AB___C -> A___C
                                removeInternal(localID);
                            } else {
                                // Successor is no barrier: ABC -> AXC or AB___C -> AX___C
                                node.changeEntry(localID, m_creator, index);
                            }
                            if (m_creator == predecessor.getNodeID()) {
                                // Predecessor is barrier: A_C -> ___C or AXC -> ___XC
                                // or A___C -> ___C or AX___C -> ___X___C
                                removeInternal(predecessor.getLocalID());
                            }
                        } else {
                            // Predecessor is no direct neighbor: A___B
                            if (m_creator == successor.getNodeID()) {
                                // Successor is barrier: A___BC -> A___C or A___B___C -> A___'___C
                                removeInternal(localID);
                            } else {
                                // Successor is no barrier: A___BC -> A___XC or A___B___C -> A___X___C
                                node.changeEntry(localID, m_creator, index);
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
                    if (m_creator != successor.getNodeID() && predecessor != null) {
                        // Entry is in range
                        if (localID - 1 == predecessor.getLocalID()) {
                            // Predecessor is direct neighbor: A'B'
                            // Successor might be direct neighbor or not: A'B'C -> AXC or A'B'___C -> AX___C
                            createOrReplaceEntry(localID, m_creator);
                            if (m_creator == predecessor.getNodeID()) {
                                // Predecessor is barrier: AXC -> ___XC or AX___C -> ___X___C
                                removeInternal(localID - 1);
                            }
                        } else {
                            // Predecessor is no direct neighbor: A___'B'
                            // Successor might be direct neighbor or not: A___'B'C -> A___(B-1)XC
                            // or A___'B'___C -> A___(B-1)X___C
                            createOrReplaceEntry(localID, m_creator);
                            createOrReplaceEntry(localID - 1, successor.getNodeID());
                        }
                    }
                }
            }
        }
    }

    /**
     * Creates a new entry or replaces the old one
     *
     * @param p_localID
     *         the LocalID
     * @param p_nodeID
     *         the NodeID
     * @return the node in which the entry is stored
     */
    private Node createOrReplaceEntry(final long p_localID, final short p_nodeID) {
        Node ret;
        Node node;
        int index;
        int size;

        if (m_root == null) {
            m_root = new Node(null, m_maxEntries, m_maxChildren);
            m_root.addEntry(p_localID, p_nodeID);
            ret = m_root;
        } else {
            node = m_root;
            while (true) {
                if (node.getNumberOfChildren() == 0) {
                    index = node.indexOf(p_localID);
                    if (index >= 0) {
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
                    if (index >= 0) {
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
     * @param p_localID
     *         the LocalID
     * @param p_nodeID
     *         the NodeID
     * @param p_node
     *         anchor node
     */
    private void mergeWithPredecessorOrBound(final long p_localID, final short p_nodeID, final Node p_node) {
        Entry predecessor;
        Entry successor;

        predecessor = getPredecessorsEntry(p_localID, p_node);
        if (predecessor == null) {
            createOrReplaceEntry(p_localID - 1, m_creator);
        } else {
            if (p_localID - 1 == predecessor.getLocalID()) {
                if (p_nodeID == predecessor.getNodeID()) {
                    removeInternal(predecessor.getLocalID(), getPredecessorsNode(p_localID, p_node));
                }
            } else {
                successor = getSuccessorsEntry(p_localID, p_node);
                if (m_changedEntry == null) {
                    // Successor is end of range
                    if (p_nodeID != successor.getNodeID()) {
                        createOrReplaceEntry(p_localID - 1, successor.getNodeID());
                    } else {
                        // New Object is in range that already was migrated to the same destination
                        removeInternal(p_localID, p_node);
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
     * @param p_localID
     *         the LocalID
     * @param p_nodeID
     *         the NodeID
     */
    private void mergeWithSuccessor(final long p_localID, final short p_nodeID) {
        Node node;
        Entry successor;

        node = getNodeOrSuccessorsNode(p_localID);
        successor = getSuccessorsEntry(p_localID, node);
        if (successor != null && p_nodeID == successor.getNodeID()) {
            removeInternal(p_localID, node);
        }
    }

    /**
     * Removes all entries between start (inclusive) and end
     *
     * @param p_start
     *         the first object in range
     * @param p_end
     *         the last object in range
     */
    private void removeEntriesWithinRange(final long p_start, final long p_end) {
        long successor;

        removeInternal(p_start, getNodeOrSuccessorsNode(p_start));

        successor = getLIDOrSuccessorsLID(p_start);
        while (successor != -1 && successor < p_end) {
            removeInternal(successor);
            successor = getLIDOrSuccessorsLID(p_start);
        }
    }

    /**
     * Returns the node in which the next entry to given LocalID (could be the LocalID itself) is stored
     *
     * @param p_localID
     *         LocalID whose node is searched
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
                if (ret.getNumberOfChildren() > 0) {
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
            if (index >= 0) {
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
     * @param p_localID
     *         the LocalID
     * @return p_localID if p_localID is in btree or successor of p_localID, (-1) if there is no successor
     */
    private long getLIDOrSuccessorsLID(final long p_localID) {
        long ret = -1;
        int index;
        Node node;

        node = getNodeOrSuccessorsNode(p_localID);
        if (node != null) {
            index = node.indexOf(p_localID);
            if (index >= 0) {
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
     * @param p_localID
     *         the LocalID whose corresponding NodeID is searched
     * @return NodeID for p_localID if p_localID is in btree or successors NodeID
     */
    private short getNodeIDOrSuccessorsNodeID(final long p_localID) {
        short ret = m_creator;
        int index;
        Node node;

        node = getNodeOrSuccessorsNode(p_localID);
        if (node != null) {
            index = node.indexOf(p_localID);
            if (index >= 0) {
                ret = node.getNodeID(index);
            } else {
                ret = node.getNodeID(index * -1 - 1);
            }
        }

        return ret;
    }

    /**
     * Splits down the middle if node is greater than maxEntries
     *
     * @param p_localID
     *         the new LocalID that causes the splitting
     * @param p_node
     *         the node that has to be split
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
        if (node.getNumberOfChildren() > 0) {
            left.addChildren(node, 0, medianIndex + 1, 0);
        }

        right = new Node(null, m_maxEntries, m_maxChildren);
        right.addEntries(node, medianIndex + 1, size, 0);
        if (node.getNumberOfChildren() > 0) {
            right.addChildren(node, medianIndex + 1, node.getNumberOfChildren(), 0);
        }
        if (node.getParent() == null) {
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
     * @param p_localID
     *         the LocalID
     * @return p_localID or (-1) if there is no entry for p_localID
     */
    private long removeInternal(final long p_localID) {
        long ret;
        Node node;

        node = getNodeOrSuccessorsNode(p_localID);
        ret = removeInternal(p_localID, node);

        return ret;
    }

    /**
     * Removes the p_localID from given node and checks invariants
     *
     * @param p_localID
     *         the LocalID
     * @param p_node
     *         the node in which p_localID should be stored
     * @return p_localID or (-1) if there is no entry for p_localID
     */
    private long removeInternal(final long p_localID, final Node p_node) {
        long ret = -1;
        int index;
        Node greatest;
        long replaceLID;
        short replaceNodeID;

        if (p_node != null) {
            index = p_node.indexOf(p_localID);
            if (index >= 0) {
                ret = p_node.removeEntry(p_localID);
                if (p_node.getNumberOfChildren() == 0) {
                    // Leaf node
                    if (p_node.getParent() != null && p_node.getNumberOfEntries() < m_minEntries) {
                        combined(p_node);
                    } else if (p_node.getParent() == null && p_node.getNumberOfEntries() == 0) {
                        // Removing root node with no keys or children
                        m_root = null;
                    }
                } else {
                    // Internal node
                    greatest = p_node.getChild(index);
                    while (greatest.getNumberOfChildren() > 0) {
                        greatest = greatest.getChild(greatest.getNumberOfChildren() - 1);
                    }
                    replaceLID = -1;
                    replaceNodeID = m_creator;
                    if (greatest.getNumberOfEntries() > 0) {
                        replaceNodeID = greatest.getNodeID(greatest.getNumberOfEntries() - 1);
                        replaceLID = greatest.removeEntry(greatest.getNumberOfEntries() - 1);
                    }
                    p_node.addEntry(replaceLID, replaceNodeID);
                    if (greatest.getParent() != null && greatest.getNumberOfEntries() < m_minEntries) {
                        combined(greatest);
                    }
                    if (greatest.getNumberOfChildren() > m_maxChildren) {
                        split(p_localID, greatest);
                    }
                }
                m_size--;
            }
        }

        return ret;
    }

    /**
     * Combines children entries with parent when size is less than minEntries
     *
     * @param p_node
     *         the node
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
        if (rightNeighbor != null && rightNeighborSize > m_minEntries) {
            // Try to borrow from right neighbor
            removeLID = rightNeighbor.getLocalID(0);
            prev = parent.indexOf(removeLID) * -1 - 2;
            parentNodeID = parent.getNodeID(prev);
            parentLID = parent.removeEntry(prev);

            neighborNodeID = rightNeighbor.getNodeID(0);
            neighborLID = rightNeighbor.removeEntry(0);

            p_node.addEntry(parentLID, parentNodeID);
            parent.addEntry(neighborLID, neighborNodeID);
            if (rightNeighbor.getNumberOfChildren() > 0) {
                p_node.addChild(rightNeighbor.removeChild(0));
            }
        } else {
            leftNeighbor = null;
            leftNeighborSize = -m_minChildren;
            if (indexOfLeftNeighbor >= 0) {
                leftNeighbor = parent.getChild(indexOfLeftNeighbor);
                leftNeighborSize = leftNeighbor.getNumberOfEntries();
            }

            if (leftNeighbor != null && leftNeighborSize > m_minEntries) {
                // Try to borrow from left neighbor
                removeLID = leftNeighbor.getLocalID(leftNeighbor.getNumberOfEntries() - 1);
                prev = parent.indexOf(removeLID) * -1 - 1;
                parentNodeID = parent.getNodeID(prev);
                parentLID = parent.removeEntry(prev);

                neighborNodeID = leftNeighbor.getNodeID(leftNeighbor.getNumberOfEntries() - 1);
                neighborLID = leftNeighbor.removeEntry(leftNeighbor.getNumberOfEntries() - 1);

                p_node.addEntry(parentLID, parentNodeID);
                parent.addEntry(neighborLID, neighborNodeID);
                if (leftNeighbor.getNumberOfChildren() > 0) {
                    p_node.addChild(leftNeighbor.removeChild(leftNeighbor.getNumberOfChildren() - 1));
                }
            } else if (rightNeighbor != null && parent.getNumberOfEntries() > 0) {
                // Cannot borrow from neighbors, try to combined with right neighbor
                removeLID = rightNeighbor.getLocalID(0);
                prev = parent.indexOf(removeLID) * -1 - 2;
                parentNodeID = parent.getNodeID(prev);
                parentLID = parent.removeEntry(prev);
                parent.removeChild(rightNeighbor);
                p_node.addEntry(parentLID, parentNodeID);

                p_node.addEntries(rightNeighbor, 0, rightNeighbor.getNumberOfEntries(), p_node.getNumberOfEntries());
                p_node.addChildren(rightNeighbor, 0, rightNeighbor.getNumberOfChildren(), p_node.getNumberOfChildren());

                if (parent.getParent() != null && parent.getNumberOfEntries() < m_minEntries) {
                    // Removing key made parent too small, combined up tree
                    combined(parent);
                } else if (parent.getNumberOfEntries() == 0) {
                    // Parent no longer has keys, make this node the new root which decreases the height of the tree
                    p_node.setParent(null);
                    m_root = p_node;
                }
            } else if (leftNeighbor != null && parent.getNumberOfEntries() > 0) {
                // Cannot borrow from neighbors, try to combined with left neighbor
                removeLID = leftNeighbor.getLocalID(leftNeighbor.getNumberOfEntries() - 1);
                prev = parent.indexOf(removeLID) * -1 - 1;
                parentNodeID = parent.getNodeID(prev);
                parentLID = parent.removeEntry(prev);
                parent.removeChild(leftNeighbor);
                p_node.addEntry(parentLID, parentNodeID);
                p_node.addEntries(leftNeighbor, 0, leftNeighbor.getNumberOfEntries(), -1);
                p_node.addChildren(leftNeighbor, 0, leftNeighbor.getNumberOfChildren(), -1);

                if (parent.getParent() != null && parent.getNumberOfEntries() < m_minEntries) {
                    // Removing key made parent too small, combined up tree
                    combined(parent);
                } else if (parent.getNumberOfEntries() == 0) {
                    // Parent no longer has keys, make this node the new root which decreases the height of the tree
                    p_node.setParent(null);
                    m_root = p_node;
                }
            }
        }
    }

    /**
     * Validates the node according to the btree invariants
     *
     * @param p_node
     *         the node
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

        if (numberOfEntries > 1) {
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
            if (p_node.getParent() == null) {
                // Root
                if (numberOfEntries > m_maxEntries) {
                    // Check max key size. Root does not have a minimum key size
                    ret = false;
                } else if (childrenSize == 0) {
                    // If root, no children, and keys are valid
                    ret = true;
                } else if (childrenSize < 2) {
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
                } else if (childrenSize == 0) {
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
     * Auxiliary object to return LocalID and NodeID at once
     *
     * @author Kevin Beineke
     *         13.06.2013
     */
    private static final class Entry implements Serializable {

        private static final long serialVersionUID = -3247514337685593792L;

        // Attributes
        private long m_localID;
        private short m_nodeID;

        // Constructors

        /**
         * Creates an instance of Entry
         *
         * @param p_localID
         *         the LocalID
         * @param p_nodeID
         *         the NodeID
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
            return "(LocalID: " + m_localID + ", location: " + m_nodeID + ')';
        }
    }

    /**
     * A single node of the btree
     *
     * @author Kevin Beineke
     *         13.06.2013
     */
    private static final class Node implements Comparable<Node>, Serializable {

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
         * @param p_parent
         *         the parent
         * @param p_maxEntries
         *         the number of entries that can be stored
         * @param p_maxChildren
         *         the number of children that can be stored
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
         * @param p_cmp
         *         the node to compare with
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

        @Override
        public boolean equals(final Object p_cmp) {

            return p_cmp instanceof Node && compareTo((Node) p_cmp) == 0;
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
                ret.append("(LocalID: ");
                ret.append(getLocalID(i));
                ret.append(" location: ");
                ret.append(getNodeID(i));
                ret.append(')');
                if (i < getNumberOfEntries() - 1) {
                    ret.append(", ");
                }
            }
            ret.append("]\n");

            if (m_parent != null) {
                ret.append("parent=[");
                for (int i = 0; i < m_parent.getNumberOfEntries(); i++) {
                    ret.append("(LocalID: ");
                    ret.append(getLocalID(i));
                    ret.append(" location: ");
                    ret.append(getNodeID(i));
                    ret.append(')');
                    if (i < m_parent.getNumberOfEntries() - 1) {
                        ret.append(", ");
                    }
                }
                ret.append("]\n");
            }

            if (m_children != null) {
                ret.append("numberOfEntries=");
                ret.append(getNumberOfEntries());
                ret.append(" children=");
                ret.append(getNumberOfChildren());
                ret.append('\n');
            }

            return ret.toString();
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
         * @param p_parent
         *         the parent node
         */
        private void setParent(final Node p_parent) {
            m_parent = p_parent;
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
         * Returns the number of children
         *
         * @return the number of children
         */
        private int getNumberOfChildren() {
            return m_numberOfChildren;
        }

        /**
         * Returns the LocalID to given index
         *
         * @param p_index
         *         the index
         * @return the LocalID to given index
         */
        private long getLocalID(final int p_index) {
            return m_keys[p_index];
        }

        /**
         * Returns the data leaf to given index
         *
         * @param p_index
         *         the index
         * @return the data leaf to given index
         */
        private short getNodeID(final int p_index) {
            return m_dataLeafs[p_index];
        }

        /**
         * Returns the index for given LocalID. Uses the binary search algorithm from
         * java.util.Arrays adapted to our needs
         *
         * @param p_localID
         *         the LocalID
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
            if (ret == -1) {
                ret = -(low + 1);
            }

            return ret;
        }

        /**
         * Adds an entry
         *
         * @param p_localID
         *         the LocalID
         * @param p_nodeID
         *         the NodeID
         */
        private void addEntry(final long p_localID, final short p_nodeID) {
            int index;

            index = indexOf(p_localID) * -1 - 1;

            System.arraycopy(m_keys, index, m_keys, index + 1, m_numberOfEntries - index);
            System.arraycopy(m_dataLeafs, index, m_dataLeafs, index + 1, m_numberOfEntries - index);

            m_keys[index] = p_localID;
            m_dataLeafs[index] = p_nodeID;

            m_numberOfEntries++;
        }

        /**
         * Adds an entry
         *
         * @param p_localID
         *         the LocalID
         * @param p_nodeID
         *         the NodeID
         * @param p_index
         *         the index to store the element at
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
         * @param p_node
         *         the other node
         * @param p_offsetSrc
         *         the offset in source array
         * @param p_endSrc
         *         the end of source array
         * @param p_offsetDst
         *         the offset in destination array or -1 if the source array has to be prepended
         */
        private void addEntries(final Node p_node, final int p_offsetSrc, final int p_endSrc, final int p_offsetDst) {
            long[] aux1;
            short[] aux2;

            if (p_offsetDst != -1) {
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
         * @param p_localID
         *         the LocalID
         * @param p_nodeID
         *         the NodeID
         * @param p_index
         *         the index of given entry in this node
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
         * @param p_localID
         *         LocalID of the entry that has to be deleted
         * @return p_localID or (-1) if there is no entry for p_localID in this node
         */
        private long removeEntry(final long p_localID) {
            long ret = -1;
            int index;

            index = indexOf(p_localID);
            if (index >= 0) {
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
         * @param p_index
         *         the index of the entry that has to be deleted
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
         * Returns the child with given index
         *
         * @param p_index
         *         the index
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
         * @param p_child
         *         the child
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
            if (ret == -1) {
                ret = -(low + 1);
            }

            return ret;
        }

        /**
         * Adds a child
         *
         * @param p_child
         *         the child
         */
        private void addChild(final Node p_child) {
            int index;

            index = indexOf(p_child) * -1 - 1;

            System.arraycopy(m_children, index, m_children, index + 1, m_numberOfChildren - index);
            m_children[index] = p_child;
            p_child.m_parent = this;

            m_numberOfChildren++;
        }

        /**
         * Adds children of another node
         *
         * @param p_node
         *         the other node
         * @param p_offsetSrc
         *         the offset in source array
         * @param p_endSrc
         *         the end of source array
         * @param p_offsetDst
         *         the offset in destination array or -1 if the source array has to be prepended
         */
        private void addChildren(final Node p_node, final int p_offsetSrc, final int p_endSrc, final int p_offsetDst) {
            Node[] aux;

            if (p_offsetDst != -1) {
                System.arraycopy(p_node.m_children, p_offsetSrc, m_children, p_offsetDst, p_endSrc - p_offsetSrc);

                for (final Node child : m_children) {
                    if (child == null) {
                        break;
                    }
                    child.m_parent = this;
                }
                m_numberOfChildren = (short) (p_offsetDst + p_endSrc - p_offsetSrc);
            } else {
                aux = new Node[m_children.length];
                System.arraycopy(p_node.m_children, 0, aux, 0, p_node.m_numberOfChildren);

                for (final Node child : aux) {
                    if (child == null) {
                        break;
                    }
                    child.m_parent = this;
                }

                System.arraycopy(m_children, 0, aux, p_node.m_numberOfChildren, m_numberOfChildren);
                m_children = aux;

                m_numberOfChildren += p_node.m_numberOfChildren;
            }
        }

        /**
         * Removes the given child
         *
         * @param p_child
         *         the child
         * @return true if the child was found and deleted, false otherwise
         */
        private boolean removeChild(final Node p_child) {
            boolean ret = false;
            int index;

            index = indexOf(p_child);
            if (index >= 0) {
                System.arraycopy(m_children, index + 1, m_children, index, m_numberOfChildren - index);

                m_numberOfChildren--;
                ret = true;
            }

            return ret;
        }

        /**
         * Removes the child with given index
         *
         * @param p_index
         *         the index
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
    }
}

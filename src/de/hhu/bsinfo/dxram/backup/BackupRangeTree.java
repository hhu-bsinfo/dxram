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

package de.hhu.bsinfo.dxram.backup;

import java.io.Serializable;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.NodeID;
import de.hhu.bsinfo.utils.ArrayListLong;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Btree to store backup ranges of migrated chunks.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.06.2015
 */
public final class BackupRangeTree implements Serializable, Importable, Exportable {

    // Constants
    private static final short INVALID = -1;
    private static final long LOCAL_MAXIMUM_ID = (long) (Math.pow(2, 48) - 1);

    // Attributes
    private short m_minEntries;
    private short m_minChildren;
    private short m_maxEntries;
    private short m_maxChildren;

    private Node m_root;
    private int m_entrySize;

    private short m_creator = NodeID.INVALID_ID;
    private boolean m_initNeeded = false;
    private short m_oldRangeID = RangeID.INVALID_ID;
    private short m_newRangeID = RangeID.INVALID_ID;

    private Entry m_changedEntry;

    // Constructors

    /**
     * Creates an instance of MigrationsTree
     *
     * @param p_order
     *     order of the btree
     * @param p_nodeID
     *     this node's NodeID
     */
    public BackupRangeTree(final short p_order, final short p_nodeID) {
        m_minEntries = p_order;
        m_minChildren = (short) (m_minEntries + 1);
        m_maxEntries = (short) (2 * m_minEntries);
        m_maxChildren = (short) (m_maxEntries + 1);

        m_root = null;
        m_entrySize = -1;

        m_creator = p_nodeID;

        m_changedEntry = null;

        createOrReplaceEntry(Long.MAX_VALUE, INVALID);
    }

    // Methods

    /**
     * Returns the node in which the predecessor is
     *
     * @param p_chunkID
     *     ChunkID whose predecessor's node is searched
     * @param p_node
     *     anchor node
     * @return the node in which the predecessor of p_chunkID is or null if there is no predecessor
     */
    private static Node getPredecessorsNode(final long p_chunkID, final Node p_node) {
        int index;
        Node ret = null;
        Node node;
        Node parent;

        assert p_node != null;

        node = p_node;

        if (p_chunkID == node.getChunkID(0)) {
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
                    while (parent != null && p_chunkID < parent.getChunkID(0)) {
                        parent = parent.getParent();
                    }
                    ret = parent;
                }
            }
        } else {
            index = node.indexOf(p_chunkID);
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
     * @param p_chunkID
     *     the ChunkID whose predecessor is searched
     * @param p_node
     *     anchor node
     * @return the entry of p_chunkID's predecessor or null if there is no predecessor
     */
    private static Entry getPredecessorsEntry(final long p_chunkID, final Node p_node) {
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
     *
     * @param p_chunkID
     *     ChunkID whose successor's node is searched
     * @param p_node
     *     anchor node
     * @return the node in which the successor of p_chunkID is or null if there is no successor
     */
    private static Node getSuccessorsNode(final long p_chunkID, final Node p_node) {
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
                while (node.getNumberOfChildren() > 0) {
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
     * @param p_chunkID
     *     the ChunkID whose successor is searched
     * @param p_node
     *     anchor node
     * @return the entry of p_chunkID's successor or null if there is no successor
     */
    private static Entry getSuccessorsEntry(final long p_chunkID, final Node p_node) {
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
     * Prints one node of the btree and walks down the btree recursively
     *
     * @param p_node
     *     the current node
     * @param p_prefix
     *     the prefix to use
     * @param p_isTail
     *     defines wheter the node is the tail
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
            ret.append("(ChunkID: ");
            ret.append(ChunkID.toHexString(p_node.getChunkID(i)));
            ret.append(" RangeID: ");
            ret.append(p_node.getRangeID(i));
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
     *     the current node
     * @param p_exporter
     *     bytebuffer to write into
     */
    private static void putTreeInByteBuffer(final Node p_node, final Exporter p_exporter) {
        Node obj;

        for (int i = 0; i < p_node.getNumberOfEntries(); i++) {

            p_exporter.writeLong(p_node.getChunkID(i));
            p_exporter.writeShort(p_node.getRangeID(i));

        }

        for (int i = 0; i < p_node.getNumberOfChildren() - 1; i++) {
            obj = p_node.getChild(i);

            if (obj != null) {
                putTreeInByteBuffer(obj, p_exporter);
            }

        }
        if (p_node.getNumberOfChildren() >= 1) {
            obj = p_node.getChild(p_node.getNumberOfChildren() - 1);

            putTreeInByteBuffer(obj, p_exporter);
        }

    }

    @Override
    public void importObject(final Importer p_importer) {
        // Size is read before!
        m_minEntries = p_importer.readShort();
        m_minChildren = (short) (m_minEntries + 1);
        m_maxEntries = (short) (2 * m_minEntries);
        m_maxChildren = (short) (m_maxEntries + 1);

        int elementsInBTree = p_importer.readInt();
        if (elementsInBTree > 0) {
            for (int i = 0; i < elementsInBTree; i++) {
                long cid = p_importer.readLong();
                short rid = p_importer.readShort();

                createOrReplaceEntry(cid, rid);
            }
        }
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeShort(m_minEntries);
        if (m_root != null) {
            // Push Size
            int numberOfTreeElements = m_entrySize + 1;
            p_exporter.writeInt(numberOfTreeElements);

            // Push elements
            putTreeInByteBuffer(m_root, p_exporter);
        } else {
            p_exporter.writeInt(-1);
        }
    }

    @Override
    public int sizeofObject() {
        int numberOfBytesWritten = Integer.BYTES;

        // Size of the b tree list
        // Integer represents the bytes where the size of the list is stored, m_size + 1 for number of entries including the
        // default (LocalID: 0x1000000000000 NodeID: 0xNID), long and short for key and value
        if (m_root != null) {
            numberOfBytesWritten += (m_entrySize + 1) * (Long.BYTES + Short.BYTES);
        }

        numberOfBytesWritten += Short.BYTES;

        numberOfBytesWritten += Short.BYTES;

        return numberOfBytesWritten;
    }

    /**
     * Removes given chunk from btree
     *
     * @param p_chunkID
     *     ChunkID of deleted object
     * @note should always be called if an object is deleted
     */
    public void remove(final long p_chunkID) {
        int index;
        Node node;
        long currentCID;
        Entry currentEntry;
        Entry predecessor;
        Entry successor;

        if (m_root != null) {
            node = getNodeOrSuccessorsNode(p_chunkID);
            if (node != null) {
                index = node.indexOf(p_chunkID);
                if (index >= 0) {
                    // Entry was found
                    currentCID = node.getChunkID(index);
                    predecessor = getPredecessorsEntry(p_chunkID, node);
                    currentEntry = new Entry(currentCID, node.getRangeID(index));
                    successor = getSuccessorsEntry(p_chunkID, node);
                    if (currentEntry.getRangeID() != INVALID && predecessor != null) {
                        if (p_chunkID - 1 == predecessor.getChunkID()) {
                            // Predecessor is direct neighbor: AB
                            // Successor might be direct neighbor or not: ABC or AB___C
                            if (successor.getRangeID() == INVALID) {
                                // Successor is barrier: ABC -> A_C or AB___C -> A___C
                                removeInternal(p_chunkID);
                            } else {
                                // Successor is no barrier: ABC -> AXC or AB___C -> AX___C
                                node.changeEntry(p_chunkID, INVALID, index);
                            }
                            if (predecessor.getRangeID() == INVALID) {
                                // Predecessor is barrier: A_C -> ___C or AXC -> ___XC
                                // or A___C -> ___C or AX___C -> ___X___C
                                removeInternal(predecessor.getChunkID());
                            }
                        } else {
                            // Predecessor is no direct neighbor: A___B
                            if (successor.getRangeID() == INVALID) {
                                // Successor is barrier: A___BC -> A___C or A___B___C -> A___'___C
                                removeInternal(p_chunkID);
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
                    if (successor.getRangeID() != INVALID && predecessor != null) {
                        // Entry is in range
                        if (p_chunkID - 1 == predecessor.getChunkID()) {
                            // Predecessor is direct neighbor: A'B'
                            // Successor might be direct neighbor or not: A'B'C -> AXC or A'B'___C -> AX___C
                            createOrReplaceEntry(p_chunkID, INVALID);
                            if (predecessor.getRangeID() == INVALID) {
                                // Predecessor is barrier: AXC -> ___XC or AX___C -> ___X___C
                                removeInternal(p_chunkID - 1);
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
    }

    /**
     * Returns the number of entries in btree
     *
     * @return the number of entries in btree
     */
    public int size() {
        return m_entrySize;
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
            ret = "Size: " + m_entrySize + '\n' + getString(m_root, "", true);
        }

        return ret;
    }

    /**
     * Stores the backup range ID for a single chunk
     *
     * @param p_chunkID
     *     ChunkIDs of created/migrated/recovered chunks
     * @param p_rangeID
     *     the backup range ID
     * @return true if insertion was successful
     */
    public boolean putChunkIDs(final long[] p_chunkID, final short p_rangeID) {
        boolean ret = true;

        for (long chunkID : p_chunkID) {
            if (!putChunkID(chunkID, p_rangeID)) {
                ret = false;
            }
        }

        return ret;
    }

    /**
     * Returns the backup range ID for given object
     *
     * @param p_chunkID
     *     ChunkID of requested object
     * @return the backup range ID
     */
    public short getBackupRange(final long p_chunkID) {
        long chunkID;
        // Store localID for local chunks, only
        if (ChunkID.getCreatorID(p_chunkID) == m_creator) {
            chunkID = ChunkID.getLocalID(p_chunkID);
        } else {
            chunkID = p_chunkID;
        }

        return getRangeIDOrSuccessorsRangeID(chunkID);
    }

    /**
     * Stores the backup range ID for a single chunk
     *
     * @param p_chunkID
     *     ChunkID of created/migrated/recovered chunk
     * @param p_rangeID
     *     the backup range ID
     * @return true if insertion was successful
     */
    boolean putChunkID(final long p_chunkID, final short p_rangeID) {
        long chunkID;
        Node node;

        // Store localID for local chunks, only
        if (ChunkID.getCreatorID(p_chunkID) == m_creator) {
            chunkID = ChunkID.getLocalID(p_chunkID);

            // Continue initialization of new backup range with first known localID of new range
            if (m_initNeeded) {
                // Init new range
                createOrReplaceEntry(LOCAL_MAXIMUM_ID, m_newRangeID);
                // Close old range
                createOrReplaceEntry(chunkID - 1, m_oldRangeID);
                m_initNeeded = false;
            }
        } else {
            chunkID = p_chunkID;
        }

        node = createOrReplaceEntry(chunkID, p_rangeID);

        // Do not merge with predecessor and successor if successor has same RangeID
        if (node != null) {

            mergeWithPredecessorOrBound(chunkID, p_rangeID, node);

            mergeWithSuccessor(chunkID, p_rangeID);
        }

        return true;
    }

    /**
     * Stores the backup range ID for a chunk range
     *
     * @param p_firstChunkID
     *     first ChunkID of migrated/recovered chunk range
     * @param p_lastChunkID
     *     last ChunkID of migrated/recovered chunk range
     * @param p_rangeID
     *     the backup range ID
     * @return true if insertion was successful
     */
    boolean putChunkIDRange(final long p_firstChunkID, final long p_lastChunkID, final short p_rangeID) {
        Node startNode;

        // end larger than start or start smaller than 1
        assert p_firstChunkID <= p_lastChunkID && p_firstChunkID > 0;

        if (p_firstChunkID == p_lastChunkID) {
            putChunkID(p_firstChunkID, p_rangeID);
        } else {
            startNode = createOrReplaceEntry(p_firstChunkID, p_rangeID);

            if (startNode != null) {
                mergeWithPredecessorOrBound(p_firstChunkID, p_rangeID, startNode);
            }

            createOrReplaceEntry(p_lastChunkID, p_rangeID);

            removeEntriesWithinRange(p_firstChunkID, p_lastChunkID);

            mergeWithSuccessor(p_lastChunkID, p_rangeID);
        }
        return true;
    }

    /**
     * Initialize new backup range. Is continued with first local insertion.
     *
     * @param p_rangeID
     *     the new backup range ID
     */
    void initializeNewBackupRange(final short p_rangeID) {
        m_oldRangeID = getRangeIDOrSuccessorsRangeID(LOCAL_MAXIMUM_ID);
        m_newRangeID = p_rangeID;
        m_initNeeded = true;
    }

    /**
     * Returns ChunkIDs of all Chunks in given range
     *
     * @param p_rangeID
     *     the RangeID
     * @return all ChunkID ranges
     */
    long[] getAllChunkIDRangesOfBackupRange(final short p_rangeID) {
        long[] ret = null;

        if (m_root != null) {
            ret = iterateNode(m_root, p_rangeID);
        }

        return ret;
    }

    /**
     * Iterates the node and returns ChunkIDs of given range
     *
     * @param p_node
     *     the node
     * @param p_rangeID
     *     the RangeID
     * @return all ChunkIDs
     */
    private long[] iterateNode(final Node p_node, final short p_rangeID) {
        long[] ret;
        int allEntriesCounter;
        int fittingEntriesCounter = 0;
        int childrenCounter;
        int fittingChildrenEntriesCounter = 0;
        int offset;
        Entry predecessor;
        long predecessorCID;
        long currentCID;
        ArrayListLong myEntries;

        // Store all ChunkIDs of given range in an array
        allEntriesCounter = p_node.getNumberOfEntries();
        myEntries = new ArrayListLong();
        for (int i = 0; i < allEntriesCounter; i++) {
            if (p_node.getRangeID(i) == p_rangeID) {
                currentCID = p_node.getChunkID(i);

                // Add predecessor (begin of range)
                predecessor = getPredecessorsEntry(currentCID, p_node);
                predecessorCID = predecessor.getChunkID();
                if (predecessorCID == ChunkID.INVALID_ID) {
                    myEntries.add((long) m_creator << 48);
                } else if (ChunkID.getCreatorID(predecessorCID) == 0) {
                    myEntries.add(((long) m_creator << 48) + predecessorCID + 1);
                } else {
                    myEntries.add(predecessorCID + 1);
                }

                // Add current ChunkID (end of range)
                if (ChunkID.getCreatorID(currentCID) == 0) {
                    currentCID = ((long) m_creator << 48) + currentCID;
                }
                myEntries.add(currentCID);
                fittingEntriesCounter += 2;
            }
        }

        // Get all fitting ChunkIDs of all children
        childrenCounter = p_node.getNumberOfChildren();
        long[][] res = new long[childrenCounter][];
        for (int i = 0; i < childrenCounter; i++) {
            res[i] = iterateNode(p_node.getChild(i), p_rangeID);
            fittingChildrenEntriesCounter += res[i].length;
        }

        // Copy this node's entries to result array
        ret = new long[fittingEntriesCounter + fittingChildrenEntriesCounter];
        System.arraycopy(myEntries.getArray(), 0, ret, 0, fittingEntriesCounter);
        offset = fittingEntriesCounter;
        // Copy all children's entries to result array
        for (long[] array : res) {
            System.arraycopy(array, 0, ret, offset, array.length);
            offset += array.length;
        }

        return ret;
    }

    /**
     * Creates a new entry or replaces the old one
     *
     * @param p_chunkID
     *     the ChunkID
     * @param p_rangeID
     *     the backup range ID
     * @return the node in which the entry is stored
     */
    private Node createOrReplaceEntry(final long p_chunkID, final short p_rangeID) {
        Node ret;
        Node node;
        int index;
        int size;

        if (m_root == null) {
            m_root = new Node(null, m_maxEntries, m_maxChildren);
            m_root.addEntry(p_chunkID, p_rangeID);
            ret = m_root;
        } else {
            node = m_root;
            while (true) {
                if (node.getNumberOfChildren() == 0) {
                    index = node.indexOf(p_chunkID);
                    if (index >= 0) {
                        m_changedEntry = new Entry(node.getChunkID(index), node.getRangeID(index));
                        node.changeEntry(p_chunkID, p_rangeID, index);
                    } else {
                        if (getRangeIDOrSuccessorsRangeID(p_chunkID) == p_rangeID) {
                            // The successor entry is equal -> no need for insertion
                            return null;
                        } else {
                            m_changedEntry = null;
                            node.addEntry(p_chunkID, p_rangeID, index * -1 - 1);
                            if (m_maxEntries < node.getNumberOfEntries()) {
                                // Need to split up
                                node = split(p_chunkID, node);
                            }
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
                    if (index >= 0) {
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
     *
     * @param p_chunkID
     *     the ChunkID
     * @param p_rangeID
     *     the backup range ID
     * @param p_node
     *     anchor node
     */
    private void mergeWithPredecessorOrBound(final long p_chunkID, final short p_rangeID, final Node p_node) {
        Entry predecessor;
        Entry successor;

        predecessor = getPredecessorsEntry(p_chunkID, p_node);
        if (predecessor == null) {
            createOrReplaceEntry(p_chunkID - 1, INVALID);
        } else {
            if (p_chunkID - 1 == predecessor.getChunkID()) {
                if (p_rangeID == predecessor.getRangeID()) {
                    removeInternal(predecessor.getChunkID(), getPredecessorsNode(p_chunkID, p_node));
                }
            } else {
                successor = getSuccessorsEntry(p_chunkID, p_node);
                if (m_changedEntry == null) {
                    // Successor is end of range
                    if (p_rangeID != successor.getRangeID()) {
                        createOrReplaceEntry(p_chunkID - 1, successor.getRangeID());
                    } else {
                        // New Object is in range that already was migrated to the same destination
                        removeInternal(p_chunkID, p_node);
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
     *
     * @param p_chunkID
     *     the ChunkID
     * @param p_rangeID
     *     the backup range ID
     */
    private void mergeWithSuccessor(final long p_chunkID, final short p_rangeID) {
        Node node;
        Entry successor;

        node = getNodeOrSuccessorsNode(p_chunkID);
        successor = getSuccessorsEntry(p_chunkID, node);
        if (successor != null && p_rangeID == successor.getRangeID()) {
            removeInternal(p_chunkID, node);
        }
    }

    /**
     * Removes all entries between start (inclusive) and end
     *
     * @param p_start
     *     the first object in range
     * @param p_end
     *     the last object in range
     */
    private void removeEntriesWithinRange(final long p_start, final long p_end) {
        long successor;

        removeInternal(p_start, getNodeOrSuccessorsNode(p_start));

        successor = getCIDOrSuccessorsCID(p_start);
        while (successor != -1 && successor < p_end) {
            removeInternal(successor);
            successor = getCIDOrSuccessorsCID(p_start);
        }
    }

    /**
     * Returns the node in which the next entry to given ChunkID (could be the ChunkID itself) is stored
     *
     * @param p_chunkID
     *     ChunkID whose node is searched
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
                if (ret.getNumberOfChildren() > 0) {
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
     * Returns next ChunkID to given ChunkID (could be the ChunkID itself)
     *
     * @param p_chunkID
     *     the ChunkID
     * @return p_chunkID if p_chunkID is in btree or successor of p_chunkID, (-1) if there is no successor
     */
    private long getCIDOrSuccessorsCID(final long p_chunkID) {
        long ret = -1;
        int index;
        Node node;

        node = getNodeOrSuccessorsNode(p_chunkID);
        if (node != null) {
            index = node.indexOf(p_chunkID);
            if (index >= 0) {
                ret = node.getChunkID(index);
            } else {
                ret = node.getChunkID(index * -1 - 1);
            }
        }

        return ret;
    }

    /**
     * Returns the backup range ID of next ChunkID to given ChunkID (could be the ChunkID itself)
     *
     * @param p_chunkID
     *     the ChunkID whose corresponding RangeID is searched
     * @return RangeID for p_chunkID if p_chunkID is in btree or successors NodeID
     */
    private short getRangeIDOrSuccessorsRangeID(final long p_chunkID) {
        short ret = INVALID;
        int index;
        Node node;

        node = getNodeOrSuccessorsNode(p_chunkID);
        if (node != null) {
            index = node.indexOf(p_chunkID);
            if (index >= 0) {
                ret = node.getRangeID(index);
            } else {
                ret = node.getRangeID(index * -1 - 1);
            }
        }

        return ret;
    }

    /**
     * Splits down the middle if node is greater than maxEntries
     *
     * @param p_chunkID
     *     the new ChunkID that causes the splitting
     * @param p_node
     *     the node that has to be split
     * @return the node in which p_chunkID must be inserted
     */
    private Node split(final long p_chunkID, final Node p_node) {
        Node ret;
        Node node;

        int size;
        int medianIndex;
        long medianCID;
        short medianRangeID;

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
     *
     * @param p_chunkID
     *     the ChunkID
     * @return p_chunkID or (-1) if there is no entry for p_chunkID
     */
    private long removeInternal(final long p_chunkID) {
        long ret;
        Node node;

        node = getNodeOrSuccessorsNode(p_chunkID);
        ret = removeInternal(p_chunkID, node);

        return ret;
    }

    /**
     * Removes the p_chunkID from given node and checks invariants
     *
     * @param p_chunkID
     *     the ChunkID
     * @param p_node
     *     the node in which p_chunkID should be stored
     * @return p_chunkID or (-1) if there is no entry for p_chunkID
     */
    private long removeInternal(final long p_chunkID, final Node p_node) {
        long ret = -1;
        int index;
        Node greatest;
        long replaceCID;
        short replaceRangeID;

        assert p_node != null;

        index = p_node.indexOf(p_chunkID);
        if (index >= 0) {
            ret = p_node.removeEntry(p_chunkID);
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
                replaceCID = -1;
                replaceRangeID = INVALID;
                if (greatest.getNumberOfEntries() > 0) {
                    replaceRangeID = greatest.getRangeID(greatest.getNumberOfEntries() - 1);
                    replaceCID = greatest.removeEntry(greatest.getNumberOfEntries() - 1);
                }
                p_node.addEntry(replaceCID, replaceRangeID);
                if (greatest.getParent() != null && greatest.getNumberOfEntries() < m_minEntries) {
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
     *
     * @param p_node
     *     the node
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
        short parentRangeID;
        long parentCID;

        short neighborRangeID;
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
        if (rightNeighbor != null && rightNeighborSize > m_minEntries) {
            // Try to borrow from right neighbor
            removeCID = rightNeighbor.getChunkID(0);
            prev = parent.indexOf(removeCID) * -1 - 2;
            parentRangeID = parent.getRangeID(prev);
            parentCID = parent.removeEntry(prev);

            neighborRangeID = rightNeighbor.getRangeID(0);
            neighborCID = rightNeighbor.removeEntry(0);

            p_node.addEntry(parentCID, parentRangeID);
            parent.addEntry(neighborCID, neighborRangeID);
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
                removeCID = leftNeighbor.getChunkID(leftNeighbor.getNumberOfEntries() - 1);
                prev = parent.indexOf(removeCID) * -1 - 1;
                parentRangeID = parent.getRangeID(prev);
                parentCID = parent.removeEntry(prev);

                neighborRangeID = leftNeighbor.getRangeID(leftNeighbor.getNumberOfEntries() - 1);
                neighborCID = leftNeighbor.removeEntry(leftNeighbor.getNumberOfEntries() - 1);

                p_node.addEntry(parentCID, parentRangeID);
                parent.addEntry(neighborCID, neighborRangeID);
                if (leftNeighbor.getNumberOfChildren() > 0) {
                    p_node.addChild(leftNeighbor.removeChild(leftNeighbor.getNumberOfChildren() - 1));
                }
            } else if (rightNeighbor != null && parent.getNumberOfEntries() > 0) {
                // Cannot borrow from neighbors, try to combined with right neighbor
                removeCID = rightNeighbor.getChunkID(0);
                prev = parent.indexOf(removeCID) * -1 - 2;
                parentRangeID = parent.getRangeID(prev);
                parentCID = parent.removeEntry(prev);
                parent.removeChild(rightNeighbor);
                p_node.addEntry(parentCID, parentRangeID);

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
                removeCID = leftNeighbor.getChunkID(leftNeighbor.getNumberOfEntries() - 1);
                prev = parent.indexOf(removeCID) * -1 - 1;
                parentRangeID = parent.getRangeID(prev);
                parentCID = parent.removeEntry(prev);
                parent.removeChild(leftNeighbor);
                p_node.addEntry(parentCID, parentRangeID);
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
     *     the node
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
                prev = p_node.getChunkID(i - 1);
                next = p_node.getChunkID(i);
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
     * A single node of the btree
     *
     * @author Kevin Beineke, kevin.beineke@hhu.de, 13.06.2013
     */
    private static final class Node implements Comparable<Node> {

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
         *     the parent
         * @param p_maxEntries
         *     the number of entries that can be stored
         * @param p_maxChildren
         *     the number of children that can be stored
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
         *     the node to compare with
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
                ret.append("(ChunkID: ");
                ret.append(ChunkID.toHexString(getChunkID(i)));
                ret.append(" location: ");
                ret.append(getRangeID(i));
                ret.append(')');
                if (i < getNumberOfEntries() - 1) {
                    ret.append(", ");
                }
            }
            ret.append("]\n");

            if (m_parent != null) {
                ret.append("parent=[");
                for (int i = 0; i < m_parent.getNumberOfEntries(); i++) {
                    ret.append("(ChunkID: ");
                    ret.append(ChunkID.toHexString(getChunkID(i)));
                    ret.append(" location: ");
                    ret.append(getRangeID(i));
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
         *     the parent node
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
         * Returns the ChunkID to given index
         *
         * @param p_index
         *     the index
         * @return the ChunkID to given index
         */
        private long getChunkID(final int p_index) {
            return m_keys[p_index];
        }

        /**
         * Returns the data leaf to given index
         *
         * @param p_index
         *     the index
         * @return the data leaf to given index
         */
        private short getRangeID(final int p_index) {
            return m_dataLeafs[p_index];
        }

        /**
         * Returns the index for given ChunkID. Uses the binary search algorithm from
         * java.util.Arrays adapted to our needs
         *
         * @param p_chunkID
         *     the ChunkID
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
            if (ret == -1) {
                ret = -(low + 1);
            }

            return ret;
        }

        /**
         * Adds an entry
         *
         * @param p_chunkID
         *     the ChunkID
         * @param p_rangeID
         *     the backup range ID
         */
        private void addEntry(final long p_chunkID, final short p_rangeID) {
            int index;

            index = indexOf(p_chunkID) * -1 - 1;

            System.arraycopy(m_keys, index, m_keys, index + 1, m_numberOfEntries - index);
            System.arraycopy(m_dataLeafs, index, m_dataLeafs, index + 1, m_numberOfEntries - index);

            m_keys[index] = p_chunkID;
            m_dataLeafs[index] = p_rangeID;

            m_numberOfEntries++;
        }

        /**
         * Adds an entry
         *
         * @param p_chunkID
         *     the ChunkID
         * @param p_rangeID
         *     the backup range ID
         * @param p_index
         *     the index to store the element at
         */
        private void addEntry(final long p_chunkID, final short p_rangeID, final int p_index) {
            System.arraycopy(m_keys, p_index, m_keys, p_index + 1, m_numberOfEntries - p_index);
            System.arraycopy(m_dataLeafs, p_index, m_dataLeafs, p_index + 1, m_numberOfEntries - p_index);

            m_keys[p_index] = p_chunkID;
            m_dataLeafs[p_index] = p_rangeID;

            m_numberOfEntries++;
        }

        /**
         * Adds entries from another node
         *
         * @param p_node
         *     the other node
         * @param p_offsetSrc
         *     the offset in source array
         * @param p_endSrc
         *     the end of source array
         * @param p_offsetDst
         *     the offset in destination array or -1 if the source array has to be prepended
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
         * @param p_chunkID
         *     the ChunkID
         * @param p_rangeID
         *     the backup range ID
         * @param p_index
         *     the index of given entry in this node
         */
        private void changeEntry(final long p_chunkID, final short p_rangeID, final int p_index) {

            if (p_chunkID == getChunkID(p_index)) {
                m_keys[p_index] = p_chunkID;
                m_dataLeafs[p_index] = p_rangeID;
            }
        }

        /**
         * Removes the entry with given ChunkID
         *
         * @param p_chunkID
         *     ChunkID of the entry that has to be deleted
         * @return p_chunkID or (-1) if there is no entry for p_chunkID in this node
         */
        private long removeEntry(final long p_chunkID) {
            long ret = -1;
            int index;

            index = indexOf(p_chunkID);
            if (index >= 0) {
                ret = getChunkID(index);

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
         *     the index of the entry that has to be deleted
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
         * Returns the child with given index
         *
         * @param p_index
         *     the index
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
         *     the child
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
            if (ret == -1) {
                ret = -(low + 1);
            }

            return ret;
        }

        /**
         * Adds a child
         *
         * @param p_child
         *     the child
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
         *     the other node
         * @param p_offsetSrc
         *     the offset in source array
         * @param p_endSrc
         *     the end of source array
         * @param p_offsetDst
         *     the offset in destination array or -1 if the source array has to be prepended
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
         *     the child
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
         *     the index
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

    /**
     * Auxiliary object to return ChunkID and backup range ID at once
     *
     * @author Kevin Beineke
     *         13.06.2013
     */
    private static final class Entry {
        // Attributes
        private long m_chunkID;
        private short m_rangeID;

        // Constructors

        /**
         * Creates an instance of Entry
         *
         * @param p_chunkID
         *     the ChunkID
         * @param p_rangeID
         *     the backup range ID
         */
        Entry(final long p_chunkID, final short p_rangeID) {
            m_chunkID = p_chunkID;
            m_rangeID = p_rangeID;
        }

        /**
         * Returns the ChunkID
         *
         * @return the ChunkID
         */
        public long getChunkID() {
            return m_chunkID;
        }

        /**
         * Returns the backup range ID
         *
         * @return the backup range ID
         */
        public short getRangeID() {
            return m_rangeID;
        }

        /**
         * Prints the entry
         *
         * @return String interpretation of the entry
         */
        @Override
        public String toString() {
            return "(ChunkID: " + m_chunkID + ", location: " + m_rangeID + ')';
        }
    }
}

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

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.utils.ArrayListLong;

/**
 * Wrapper class for all data belonging to one peer: One Btree to store ranges, one to store backup range affiliation and an ArrayList for the backup ranges
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 14.02.2017
 */
public final class PeerHandler {

    // Attributes
    private boolean m_recovered;

    private LookupTree m_lookupTree;
    private ArrayListLong m_backupRanges;

    // Constructors

    /**
     * Creates an instance of LookupTree
     *
     * @param p_order
     *     order of the btree
     */
    PeerHandler(final short p_order, final short p_creator) {
        m_recovered = false;

        m_lookupTree = new LookupTree(p_order, p_creator);
        m_backupRanges = new ArrayListLong();
    }

    // Methods

    /**
     * Returns the lookup tree
     *
     * @return the lookup tree
     */
    LookupTree getLookupTree() {
        return m_lookupTree;
    }

    /**
     * Updates the metadata of given backup range
     *
     * @param p_rangeID
     *     the backup range to update
     * @param p_recoveryPeer
     *     the peer that recovered the backup range
     * @param p_chunkIDRanges
     *     ChunkIDs of all recovered chunks arranged in ranges
     */
    void updateMetadataAfterRecovery(final short p_rangeID, final short p_recoveryPeer, final long[] p_chunkIDRanges) {
        m_recovered = true;

        // "Migrate" recovered ChunkIDs
        for (int i = 0; i < p_chunkIDRanges.length; i += 2) {
            if (p_chunkIDRanges[i] == -1) {
                break;
            }

            m_lookupTree.migrateRange(p_chunkIDRanges[i], p_chunkIDRanges[i + 1], p_recoveryPeer);
        }

        // Invalidate backup range
        m_backupRanges.set(p_rangeID, -1);
    }

    /**
     * Returns the primary peer for given object
     *
     * @param p_chunkID
     *     ChunkID of requested object
     * @return the NodeID of the primary peer for given object
     */
    short getPrimaryPeer(final long p_chunkID) {
        return m_lookupTree.getPrimaryPeer(p_chunkID);
    }

    /**
     * Returns the range given ChunkID is in
     *
     * @param p_chunkID
     *     ChunkID of requested object
     * @return the first and last ChunkID of the range
     */
    LookupRange getMetadata(final long p_chunkID) {
        return m_lookupTree.getMetadata(p_chunkID);
    }

    /**
     * Stores the migration for a single chunk
     *
     * @param p_chunkID
     *     ChunkID of migrated object
     * @param p_nodeID
     *     new primary peer
     * @return true if insertion was successful
     */
    boolean migrate(final long p_chunkID, final short p_nodeID) {
        return m_lookupTree.migrate(p_chunkID, p_nodeID);
    }

    /**
     * Stores the migration for a range
     *
     * @param p_startCID
     *     ChunkID of first migrated object
     * @param p_endCID
     *     ChunkID of last migrated object
     * @param p_nodeID
     *     new primary peer
     * @return true if insertion was successful
     */
    boolean migrateRange(final long p_startCID, final long p_endCID, final short p_nodeID) {
        return m_lookupTree.migrateRange(p_startCID, p_endCID, p_nodeID);
    }

    /**
     * Removes given chunk from btree; only necessary for migrated chunks
     *
     * @param p_chunkID
     *     ChunkID of deleted object
     * @note should always be called if an object is deleted
     */
    void remove(final long p_chunkID) {
        m_lookupTree.remove(p_chunkID);
    }

    /**
     * Removes multiple chunks from btree; only necessary for migrated chunks
     *
     * @param p_chunkIDs
     *     ChunkIDs of deleted objects
     * @note should always be called if an object is deleted
     */

    void removeObjects(final long... p_chunkIDs) {
        m_lookupTree.removeObjects(p_chunkIDs);
    }

    /**
     * Initializes a new backup range
     *
     * @param p_backupRange
     *     the backup range to initialize
     * @return true if insertion was successful
     */
    boolean initRange(final BackupRange p_backupRange) {
        byte rangeID;
        long backupPeers;

        m_backupRanges.add(p_backupRange.getRangeID(), BackupRange.convert(p_backupRange.getBackupPeers()));

        return true;
    }

    /**
     * Replaces given peer from specific backup range
     *
     * @param p_rangeID
     *     the RangeID
     * @param p_failedPeer
     *     NodeID of failed peer
     * @param p_replacement
     *     NodeID of new backup peer
     */
    void replaceBackupPeer(final short p_rangeID, final short p_failedPeer, final short p_replacement) {
        long backupPeers;

        // This is a migration backup range
        backupPeers = BackupRange.replaceBackupPeer(m_backupRanges.get(p_rangeID), p_failedPeer, p_replacement);
        m_backupRanges.set(p_rangeID, backupPeers);
    }

    /**
     * Returns the backup peers for all backup ranges
     *
     * @return an array with all backup ranges
     */
    BackupRange[] getAllBackupRanges() {
        BackupRange[] ret;
        long[] backupRanges;
        int counter = 0;

        backupRanges = m_backupRanges.getArray();
        ret = new BackupRange[backupRanges.length];
        for (int i = 0; i < backupRanges.length; i++) {
            if (backupRanges[i] != -1) {
                ret[counter++] = new BackupRange((short) i, BackupRange.convert(backupRanges[i]));
            }
        }

        if (counter != ret.length) {
            BackupRange[] tmp = new BackupRange[counter];
            System.arraycopy(ret, 0, tmp, 0, counter);
            ret = tmp;
        }

        return ret;
    }

    /**
     * Writes all peer's data to given byte buffer
     *
     * @param p_data
     *     the ByteBuffer
     */
    void receiveMetadata(ByteBuffer p_data) {
        ByteBufferImExporter exporter;

        p_data.put((byte) (m_recovered ? 1 : 0));

        exporter = new ByteBufferImExporter(p_data);
        exporter.exportObject(m_lookupTree);
        exporter.exportObject(m_backupRanges);
    }

    /**
     * Stores all given data
     *
     * @param p_data
     *     the data
     */
    void storeMetadata(final ByteBuffer p_data) {
        ByteBufferImExporter importer;

        // Creator was read before
        m_recovered = p_data.get() != 0;

        importer = new ByteBufferImExporter(p_data);
        importer.importObject(m_lookupTree);
        importer.importObject(m_backupRanges);
    }

    /**
     * Returns the serialized metadata size for this peer
     *
     * @return the size
     */
    int getSize() {
        return Short.BYTES + Byte.BYTES + m_lookupTree.sizeofObject() + m_backupRanges.sizeofObject();
    }

}

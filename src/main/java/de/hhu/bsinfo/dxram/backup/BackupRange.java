/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.backup;

import java.util.Arrays;

import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Stores a backup range (for chunk and backup service)
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 10.06.2015
 */
public class BackupRange implements Comparable<BackupRange>, Importable, Exportable {

    // Attributes
    // The replication factor is set by BackupComponent and is in [1, 4]
    private static byte ms_replicationFactor;
    // The backup range size is set by BackupComponent
    private static long ms_backupRangeSize;

    private short m_rangeID;
    private BackupPeer[] m_backupPeers;

    private int m_size;

    // Constructors

    /**
     * Creates an instance of BackupRange
     */
    public BackupRange() {
    }

    /**
     * Creates an instance of BackupRange
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_backupPeers
     *         the backup peers
     */
    public BackupRange(final short p_rangeID, final BackupPeer[] p_backupPeers) {
        m_rangeID = p_rangeID;
        m_backupPeers = p_backupPeers;
        m_size = 0;
    }

    /**
     * Converts backup peers from long to short[]
     *
     * @param p_backupPeers
     *         the backup peers in long representation
     * @return the backup peers in short[] representation
     */
    public static BackupPeer[] convert(final long p_backupPeers) {
        BackupPeer[] ret;

        ret = new BackupPeer[ms_replicationFactor];
        for (int i = 0; i < ms_replicationFactor; i++) {
            short node = (short) ((p_backupPeers & 0x000000000000FFFFL << i * 16) >> i * 16);
            if (node != NodeID.INVALID_ID) {
                ret[i] = new BackupPeer(node, (short) 0, (short) 0);
            }
        }

        return ret;
    }

    /**
     * Converts backup peers from short[] to long
     *
     * @param p_backupPeers
     *         the backup peers in short[] representation
     * @return the backup peers in long representation
     */
    public static long convert(final BackupPeer[] p_backupPeers) {
        long ret = 0;

        for (int i = 0; i < ms_replicationFactor; i++) {
            if (p_backupPeers[i] != null) {
                ret += (p_backupPeers[i].getNodeID() & 0x000000000000FFFFL) << i * 16;
            } else {
                ret += (NodeID.INVALID_ID & 0x000000000000FFFFL) << i * 16;
            }
        }

        return ret;
    }

    /**
     * Converts owner and backup peers from short + short[] to long
     *
     * @param p_owner
     *         the owner
     * @param p_backupPeers
     *         the backup peers in short[] representation
     * @return the owner and backup peers in long representation or -1 if replication factor is greater than 3
     */
    public static long convert(final short p_owner, final short[] p_backupPeers) {
        long ret = 0;

        if (ms_replicationFactor <= 3) {

            ret += p_owner & 0x000000000000FFFFL;
            for (int i = 0; i < ms_replicationFactor; i++) {
                ret += (p_backupPeers[i] & 0x000000000000FFFFL) << (i + 1) * 16;
            }
        }

        return ret;
    }

    /**
     * Adds the backup peer
     *
     * @param p_backupPeers
     *         current backup peers
     * @param p_newPeer
     *         the new backup peer
     * @return all backup peers in a short array
     */
    public static long addBackupPeer(final long p_backupPeers, final short p_newPeer) {
        long ret = -1;

        for (int i = 0; i < ms_replicationFactor; i++) {
            if ((short) ((p_backupPeers & 0xFFFFL << i * 16) >> i * 16) == -1) {
                ret = replace(p_backupPeers, i, p_newPeer);
                break;
            }
        }

        return ret;
    }

    /**
     * Replaces the failed backup peer
     *
     * @param p_backupPeers
     *         current backup peers
     * @param p_oldPeer
     *         the failed backup peer
     * @param p_newPeer
     *         the new backup peer
     * @return all backup peers in a short array
     */
    public static long replaceBackupPeer(final long p_backupPeers, final short p_oldPeer, final short p_newPeer) {
        long backupPeers = p_backupPeers;
        short nextBackupPeer;
        int addIndex = ms_replicationFactor - 1;

        for (int i = 0; i < ms_replicationFactor; i++) {
            if ((short) ((backupPeers & 0xFFFFL << i * 16) >> i * 16) == p_oldPeer) {
                for (int j = i; j < ms_replicationFactor - 1; j++) {
                    nextBackupPeer = (short) ((backupPeers & 0xFFFFL << (j + 1) * 16) >> (j + 1) * 16);
                    if (nextBackupPeer == NodeID.INVALID_ID) {
                        addIndex = j;
                        break;
                    }
                    backupPeers = replace(backupPeers, j, nextBackupPeer);
                }
                backupPeers = replace(backupPeers, addIndex, p_newPeer);
                break;
            }
        }

        return backupPeers;
    }

    /**
     * Sets the replication factor
     *
     * @param p_replicationFactor
     *         the replication factor
     */
    public static void setReplicationFactor(final byte p_replicationFactor) {
        ms_replicationFactor = p_replicationFactor;
    }

    /**
     * Sets the replication factor
     *
     * @param p_backupRangeSize
     *         the backup range size that must not be exceeded
     */
    public static void setBackupRangeSize(final long p_backupRangeSize) {
        ms_backupRangeSize = p_backupRangeSize;
    }

    /**
     * Replaces the backup peer at given index
     *
     * @param p_backupPeers
     *         all backup peers in long representation
     * @param p_index
     *         the index
     * @param p_newPeer
     *         the replacement
     * @return all backup peers including replacement
     */
    private static long replace(final long p_backupPeers, final int p_index, final short p_newPeer) {
        return p_backupPeers & ~(0xFFFFL << p_index * 16) | (long) p_newPeer << p_index * 16;
    }

    /**
     * Get RangeID
     *
     * @return the RangeID
     */
    public short getRangeID() {
        return m_rangeID;
    }

    /**
     * Set RangeID
     *
     * @param p_rangeID
     *         the RangeID
     */
    void setRangeID(final short p_rangeID) {
        m_rangeID = p_rangeID;
    }

    // Getter

    /**
     * Get backup peers
     *
     * @return the backup peers
     */
    public BackupPeer[] getBackupPeers() {
        return m_backupPeers;
    }

    /**
     * Prints the backup range
     *
     * @return String interpretation of BackupRange
     */
    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder("ID " + m_rangeID + " [");

        for (int i = 0; i < ms_replicationFactor; i++) {
            // If we have less backup peers than the replication factor
            if (m_backupPeers[i] != null) {
                ret.append(NodeID.toHexString(m_backupPeers[i].getNodeID()));

                if (i != ms_replicationFactor - 1) {
                    ret.append(", ");
                }
            }
        }

        ret.append(']');

        return ret.toString();
    }

    /**
     * Compares this backup range with another; Only compares the RangeIDs
     *
     * @param p_otherBackupRange
     *         the other backup range
     * @return 0 if backup ranges are equal; value smaller than 0 if this backup range is smaller; value greater
     * than 0 if the other backup range is smaller
     */
    @Override
    public int compareTo(BackupRange p_otherBackupRange) {
        return Short.compare(m_rangeID, p_otherBackupRange.m_rangeID);
    }

    @Override
    public void importObject(final Importer p_importer) {
        long backupPeers;

        m_rangeID = p_importer.readShort(m_rangeID);
        backupPeers = p_importer.readLong(0);
        m_backupPeers = BackupRange.convert(backupPeers);
    }

    // Methods

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeShort(m_rangeID);
        p_exporter.writeLong(getBackupPeersAsLong());
    }

    @Override
    public int sizeofObject() {
        return Short.BYTES + Long.BYTES;
    }

    /**
     * Get backup peers as long
     *
     * @return the backup peers
     */
    long getBackupPeersAsLong() {
        if (m_backupPeers != null) {
            return BackupRange.convert(m_backupPeers);
        } else {
            return -1;
        }
    }

    /**
     * Adds the backup peer at the end of the array or first entry with INVALID_ID
     *
     * @param p_newPeer
     *         the new backup peer
     * @return whether the peer was added or not
     */
    boolean addBackupPeer(final BackupPeer p_newPeer) {
        boolean ret = false;

        for (int i = 0; i < m_backupPeers.length; i++) {
            if (m_backupPeers[i] == null) {
                m_backupPeers[i] = p_newPeer;
                ret = true;
                break;
            }
        }

        return ret;
    }

    /**
     * Replaces the backup peer with another one. The old peer is removed and the new peer is appended.
     *
     * @param p_oldPeer
     *         the old backup peer
     * @param p_newPeer
     *         the new backup peer
     */
    void replaceBackupPeer(final BackupPeer p_oldPeer, final BackupPeer p_newPeer) {
        int addIndex = m_backupPeers.length - 1;

        for (int i = 0; i < m_backupPeers.length; i++) {
            if (m_backupPeers[i].getNodeID() == p_oldPeer.getNodeID()) {
                for (int j = i; j < m_backupPeers.length - 1; j++) {
                    if (m_backupPeers[j + 1] == null) {
                        addIndex = j;
                        break;
                    }
                    m_backupPeers[j] = m_backupPeers[j + 1];
                }
                m_backupPeers[addIndex] = p_newPeer;
                break;
            }
        }
    }

    /**
     * Get backup peers
     *
     * @return the backup peers
     */
    BackupPeer[] getCopyOfBackupPeers() {
        return Arrays.copyOf(m_backupPeers, m_backupPeers.length);
    }

    /**
     * Get backup peers
     *
     * @return the backup peers
     */
    short[] getNodeIDsOfBackupPeers() {
        short[] ret = new short[ms_replicationFactor];
        Arrays.fill(ret, NodeID.INVALID_ID);

        for (int i = 0; i < m_backupPeers.length; i++) {
            ret[i] = m_backupPeers[i].getNodeID();
        }

        return ret;
    }

    /**
     * Checks if the Chunk fits in backup range
     *
     * @param p_size
     *         the size of the chunk + log header size
     * @return true if it fits
     */
    boolean fits(final long p_size) {
        return p_size + m_size <= ms_backupRangeSize;
    }

    int getSize() {
        return m_size;
    }

    /**
     * Puts chunks to the backup range. Increases size only
     *
     * @param p_size
     *         the size of all chunks including log headers
     */
    void addChunks(final long p_size) {
        m_size += p_size;
    }

    /**
     * Puts a chunk to the backup range. Increases size only
     *
     * @param p_size
     *         the size of the chunk + log header size
     */
    void addChunk(final long p_size) {
        m_size += p_size;
    }

    /**
     * Removes a chunk from the backup range. Decreases size only
     *
     * @param p_size
     *         the size of the chunk + log header size
     */
    void removeChunk(final long p_size) {
        m_size -= p_size;
    }
}

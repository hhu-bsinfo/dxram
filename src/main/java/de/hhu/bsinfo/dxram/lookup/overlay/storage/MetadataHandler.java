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

package de.hhu.bsinfo.dxram.lookup.overlay.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupState;
import de.hhu.bsinfo.dxram.lookup.overlay.OverlayHelper;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.SuperpeerStorage.Status;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Wrapper class for all data of one superpeer
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.10.2016
 */
public final class MetadataHandler {

    private static final Logger LOGGER = LogManager.getFormatterLogger(MetadataHandler.class);

    // Attributes
    private PeerHandler[] m_peerHandlers;
    private NameserviceHashTable m_nameservice;
    private SuperpeerStorage m_storage;
    private BarriersTable m_barriers;

    private ArrayList<Short> m_assignedPeersIncludingBackups;

    private ReadWriteLock m_dataLock;

    // Constructors

    /**
     * Creates an instance of SuperpeerMetadata.
     *
     * @param p_peerHandlers
     *         the peer handlers
     * @param p_nameservice
     *         hash table for the nameservice
     * @param p_storage
     *         the superpeer storage
     * @param p_barriers
     *         the barriers
     * @param p_assignedPeersIncludingBackups
     *         reference to all assigned peers including backups
     */
    public MetadataHandler(final PeerHandler[] p_peerHandlers, final NameserviceHashTable p_nameservice,
            final SuperpeerStorage p_storage, final BarriersTable p_barriers,
            final ArrayList<Short> p_assignedPeersIncludingBackups) {
        m_peerHandlers = p_peerHandlers;
        m_nameservice = p_nameservice;
        m_storage = p_storage;
        m_barriers = p_barriers;

        m_assignedPeersIncludingBackups = p_assignedPeersIncludingBackups;

        m_dataLock = new ReentrantReadWriteLock(false);
    }

    /**
     * Gets the state of given peer
     *
     * @param p_nodeID
     *         the peer's NodeID
     */
    public PeerState getState(final short p_nodeID) {
        return m_peerHandlers[p_nodeID & 0xFFFF].getState();
    }

    /**
     * Sets the state of given peer
     *
     * @param p_nodeID
     *         the peer's NodeID
     * @param p_state
     *         the new state
     */
    public void setState(final short p_nodeID, final PeerState p_state) {
        if (m_peerHandlers[p_nodeID & 0xFFFF] != null) {
            m_peerHandlers[p_nodeID & 0xFFFF].setState(p_state);
        }
    }

    /**
     * Gets status of whole metadata storage.
     *
     * @return the status
     */
    public Status getStorageStatus() {
        Status ret;

        m_dataLock.readLock().lock();
        ret = m_storage.getStatus();
        m_dataLock.readLock().unlock();

        return ret;
    }

    /**
     * Gets all nameservice entries of this superpeer.
     *
     * @return all nameservice entries in a byte array
     */
    public byte[] getAllNameserviceEntries(final short p_predecessor, final short p_nodeID) {
        byte[] ret;

        m_dataLock.readLock().lock();
        ret = m_nameservice.receiveMetadataInRange(p_predecessor, p_nodeID);
        m_dataLock.readLock().unlock();

        return ret;
    }

    /**
     * Serializes all metadata in given area.
     *
     * @param p_beginOfArea
     *         the beginning of the area
     * @param p_endOfArea
     *         the end of the area
     * @return all corresponding metadata in a byte array
     * @lock overlay lock must be read-locked
     */
    public byte[] receiveMetadataInRange(final short p_beginOfArea, final short p_endOfArea) {
        int size;
        int count = 0;
        int index;
        int startIndex;
        short currentPeer;
        byte[] ret;
        byte[] nameserviceEntries;
        byte[] storages;
        byte[] barriers;
        ByteBuffer data;
        PeerHandler peerHandler;

        m_dataLock.readLock().lock();

        LOGGER.trace("Serializing metadata of area: 0x%X, 0x%X", p_beginOfArea, p_endOfArea);

        // Get all corresponding nameservice entries
        nameserviceEntries = m_nameservice.receiveMetadataInRange(p_beginOfArea, p_endOfArea);
        // Get all corresponding storages
        storages = m_storage.receiveMetadataInRange(p_beginOfArea, p_endOfArea);
        // Get all corresponding barriers
        barriers = m_barriers.receiveMetadataInRange(p_beginOfArea, p_endOfArea);

        // Get all corresponding lookup trees
        size = nameserviceEntries.length + storages.length + barriers.length + Integer.BYTES * 4;
        if (!m_assignedPeersIncludingBackups.isEmpty()) {
            // Find beginning
            index = Collections.binarySearch(m_assignedPeersIncludingBackups, p_beginOfArea);
            if (index < 0) {
                index = index * -1 - 1;
                if (index == m_assignedPeersIncludingBackups.size()) {
                    index = 0;
                }
            }

            // Iterate over assigned peers and count lookup tree sizes
            startIndex = index;
            currentPeer = m_assignedPeersIncludingBackups.get(index++);
            while (OverlayHelper.isPeerInSuperpeerRange(currentPeer, p_beginOfArea, p_endOfArea)) {
                peerHandler = getPeerHandler(currentPeer);
                // no tree available -> no chunks were created or backup system is deactivated
                if (peerHandler != null) {
                    size += peerHandler.getSize();
                    count++;
                }

                if (index == m_assignedPeersIncludingBackups.size()) {
                    index = 0;
                }
                if (index == startIndex) {
                    break;
                }
                currentPeer = m_assignedPeersIncludingBackups.get(index++);
            }

            // Allocate array for all data and copy already gathered data
            ret = new byte[size];
            data = ByteBuffer.wrap(ret);
            data.putInt(nameserviceEntries.length);
            data.put(nameserviceEntries);
            data.putInt(storages.length);
            data.put(storages);
            data.putInt(barriers.length);
            data.put(barriers);

            // Iterate over assigned peers and write lookup trees
            data.putInt(count);
            index = startIndex;
            currentPeer = m_assignedPeersIncludingBackups.get(index++);
            while (OverlayHelper.isPeerInSuperpeerRange(currentPeer, p_beginOfArea, p_endOfArea)) {

                LOGGER.trace("Including LookupTree of 0x%X", currentPeer);

                peerHandler = getPeerHandler(currentPeer);
                // no tree available -> no chunks were created or backup system is deactivated
                if (peerHandler != null) {
                    data.putShort(currentPeer);
                    peerHandler.receiveMetadata(data);
                }

                if (index == m_assignedPeersIncludingBackups.size()) {
                    index = 0;
                }
                if (index == startIndex) {
                    break;
                }
                currentPeer = m_assignedPeersIncludingBackups.get(index++);
            }
        } else {
            // There might be data in given area even without any assigned peer

            // Allocate array for all data and copy gathered data
            ret = new byte[size];
            data = ByteBuffer.wrap(ret);
            data.putInt(nameserviceEntries.length);
            data.put(nameserviceEntries);
            data.putInt(storages.length);
            data.put(storages);
            data.putInt(barriers.length);
            data.put(barriers);
        }
        m_dataLock.readLock().unlock();

        // If there is no metadata in given area, return an empty array
        if (ret.length == Integer.BYTES * 4) {
            ret = new byte[0];
        }

        return ret;
    }

    /**
     * Serializes all metadata.
     *
     * @return all metadata in a byte array
     */
    public byte[] receiveAllMetadata() {
        int size;
        int count = 0;
        byte[] ret;
        byte[] nameserviceEntries;
        byte[] storages;
        byte[] barriers;
        PeerHandler peerHandler;
        ByteBuffer data;

        m_dataLock.readLock().lock();

        LOGGER.trace("Serializing all metadata");

        // Get all nameservice entries
        nameserviceEntries = m_nameservice.receiveAllMetadata();
        // Get all storages
        storages = m_storage.receiveAllMetadata();
        // Get all barriers
        barriers = m_barriers.receiveAllMetadata();

        // Get all nameservice entries
        size = nameserviceEntries.length + storages.length + barriers.length + Integer.BYTES * 4;

        // Iterate over all peers and count lookup tree sizes
        for (int i = 0; i < Short.MAX_VALUE * 2; i++) {
            peerHandler = getPeerHandler((short) i);
            // no tree available -> no chunks were created or backup system is deactivated
            if (peerHandler != null) {
                size += peerHandler.getSize();
                count++;
            }
        }

        // Allocate array for all data and copy already gathered data
        ret = new byte[size];
        data = ByteBuffer.wrap(ret);
        data.putInt(nameserviceEntries.length);
        data.put(nameserviceEntries);
        data.putInt(storages.length);
        data.put(storages);
        data.putInt(barriers.length);
        data.put(barriers);

        // Iterate over all peers and write lookup trees
        data.putInt(count);
        for (int i = 0; i < Short.MAX_VALUE * 2; i++) {
            peerHandler = getPeerHandler((short) i);
            // no tree available -> no chunks were created or backup system is deactivated
            if (peerHandler != null) {

                LOGGER.trace("Including LookupTree of 0x%X", (short) i);

                data.putShort((short) i);
                peerHandler.receiveMetadata(data);
            }
        }
        m_dataLock.readLock().unlock();

        // If there is no metadata, return an empty array
        if (ret.length == Integer.BYTES * 4) {
            ret = new byte[0];
        }

        return ret;
    }

    /**
     * Returns the number of nameservice entries in given area
     *
     * @param p_responsibleArea
     *         the area
     * @return the number of nameservice entries
     */
    public int getNumberOfNameserviceEntries(final short[] p_responsibleArea) {
        int ret;

        m_dataLock.readLock().lock();
        ret = m_nameservice.quantifyMetadata(p_responsibleArea[0], p_responsibleArea[1]);
        m_dataLock.readLock().unlock();

        return ret;
    }

    /**
     * Returns the number of storages in given area
     *
     * @param p_responsibleArea
     *         the area
     * @return the number of storages
     */
    public int getNumberOfStorages(final short[] p_responsibleArea) {
        int ret;

        m_dataLock.readLock().lock();
        ret = m_storage.quantifyMetadata(p_responsibleArea[0], p_responsibleArea[1]);
        m_dataLock.readLock().unlock();

        return ret;
    }

    /**
     * Returns the number of barriers in given area
     *
     * @param p_responsibleArea
     *         the area
     * @return the number of barriers
     */
    public int getNumberOfBarriers(final short[] p_responsibleArea) {
        int ret;

        m_dataLock.readLock().lock();
        ret = m_barriers.quantifyMetadata(p_responsibleArea[0], p_responsibleArea[1]);
        m_dataLock.readLock().unlock();

        return ret;
    }

    /**
     * Get a summary of this superpeer's metadata
     *
     * @param p_nodeID
     *         this superpeer's NodeID
     * @param p_predecessor
     *         the predecessor's NodeID
     * @return the metadata summary
     * @lock overlay lock must be read-locked
     */
    public String getSummary(final short p_nodeID, final short p_predecessor) {
        StringBuilder ret = new StringBuilder();
        PeerHandler peerHandler;

        m_dataLock.readLock().lock();

        ret.append("Number of nameservice entries: ").append(m_nameservice.quantifyMetadata(p_nodeID, p_nodeID))
                .append(", assigned: ").append(m_nameservice.quantifyMetadata(p_predecessor, p_nodeID)).append('\n');

        ret.append("Number of storages: \t\t ").append(m_storage.quantifyMetadata(p_nodeID, p_nodeID))
                .append(", assigned: ").append(m_storage.quantifyMetadata(p_predecessor, p_nodeID)).append('\n');

        ret.append("Number of barriers: \t\t ").append(m_barriers.quantifyMetadata(p_nodeID, p_nodeID))
                .append(", assigned: ").append(m_barriers.quantifyMetadata(p_predecessor, p_nodeID)).append('\n');

        ret.append("Storing LookupTrees of following peers:\n");

        for (int i = 0; i < Short.MAX_VALUE * 2; i++) {
            peerHandler = getPeerHandler((short) i);
            // no tree available -> no chunks were created or backup system is deactivated
            if (peerHandler != null) {
                if (OverlayHelper.isPeerInSuperpeerRange((short) i, p_predecessor, p_nodeID)) {
                    ret.append("--> ").append(NodeID.toHexString((short) i));
                } else {
                    ret.append("--> ").append(NodeID.toHexString((short) i)).append(" (Backup)");
                }
            }
        }

        m_dataLock.readLock().unlock();

        return ret.toString();
    }

    /**
     * Compares given peer list with local list and returns all missing backup data between this superpeer and his
     * predecessor.
     *
     * @param p_peers
     *         all peers the requesting superpeer stores backups for
     * @param p_numberOfNameserviceEntries
     *         the number of expected nameservice entries
     * @param p_numberOfStorages
     *         the number of expected storages
     * @param p_numberOfBarriers
     *         the number of expected barriers
     * @param p_predecessor
     *         the predecessor
     * @param p_nodeID
     *         the own NodeID
     * @return the backup data of missing peers in given peer list
     * @lock overlay lock must be read-locked
     */
    public byte[] compareAndReturnBackups(final ArrayList<Short> p_peers, final int p_numberOfNameserviceEntries,
            final int p_numberOfStorages, final int p_numberOfBarriers, final short p_predecessor,
            final short p_nodeID) {
        int size;
        int count = 0;
        int index;
        int startIndex;
        short currentPeer;
        byte[] ret;
        byte[] nameserviceEntries = null;
        byte[] storages = null;
        byte[] barriers = null;
        ByteBuffer data;
        PeerHandler peerHandler;

        m_dataLock.readLock().lock();

        LOGGER.trace("Compare and return metadata of area: 0x%X, 0x%X", p_predecessor, p_nodeID);

        // TODO: Inefficient to send all data (nameservice, storages, barriers) in corresponding area if quantity
        // differs
        size = 4 * Integer.BYTES;
        // Compare number of actual nameservice entries with expected number
        if (m_nameservice.quantifyMetadata(p_predecessor, p_nodeID) != p_numberOfNameserviceEntries) {
            // Get all corresponding nameservice entries
            nameserviceEntries = m_nameservice.receiveMetadataInRange(p_predecessor, p_nodeID);
            size += nameserviceEntries.length;
        }
        // Compare number of actual storages with expected number
        if (m_storage.quantifyMetadata(p_predecessor, p_nodeID) != p_numberOfStorages) {
            // Get all corresponding storages
            storages = m_storage.receiveMetadataInRange(p_predecessor, p_nodeID);
            size += storages.length;
        }
        // Compare number of actual barriers with expected number
        if (m_barriers.quantifyMetadata(p_predecessor, p_nodeID) != p_numberOfBarriers) {
            // Get all corresponding barriers
            barriers = m_barriers.receiveMetadataInRange(p_predecessor, p_nodeID);
            size += barriers.length;
        }

        // Get all corresponding lookup trees
        if (!m_assignedPeersIncludingBackups.isEmpty()) {
            // Find beginning
            index = Collections.binarySearch(m_assignedPeersIncludingBackups, p_predecessor);
            if (index < 0) {
                index = index * -1 - 1;
                if (index == m_assignedPeersIncludingBackups.size()) {
                    index = 0;
                }
            }

            // Iterate over assigned peers and count lookup tree sizes
            startIndex = index;
            currentPeer = m_assignedPeersIncludingBackups.get(index++);
            while (OverlayHelper.isPeerInSuperpeerRange(currentPeer, p_predecessor, p_nodeID)) {
                if (Collections.binarySearch(p_peers, currentPeer) < 0) {
                    peerHandler = getPeerHandler(currentPeer);
                    // no tree available -> no chunks were created or backup system is deactivated
                    if (peerHandler != null) {
                        size += peerHandler.getSize();
                        count++;
                    }
                }

                if (index == m_assignedPeersIncludingBackups.size()) {
                    index = 0;
                }
                if (index == startIndex) {
                    break;
                }
                currentPeer = m_assignedPeersIncludingBackups.get(index++);
            }

            // Allocate array for all data and copy already gathered data
            ret = new byte[size];
            data = ByteBuffer.wrap(ret);
            if (nameserviceEntries != null) {
                data.putInt(nameserviceEntries.length);
                data.put(nameserviceEntries);
            } else {
                data.putInt(0);
            }
            if (storages != null) {
                data.putInt(storages.length);
                data.put(storages);
            } else {
                data.putInt(0);
            }
            if (barriers != null) {
                data.putInt(barriers.length);
                data.put(barriers);
            } else {
                data.putInt(0);
            }

            // Iterate over assigned peers and write lookup trees
            data.putInt(count);
            index = startIndex;
            currentPeer = m_assignedPeersIncludingBackups.get(index++);
            while (OverlayHelper.isPeerInSuperpeerRange(currentPeer, p_predecessor, p_nodeID)) {
                if (Collections.binarySearch(p_peers, currentPeer) < 0) {

                    LOGGER.trace("Including LookupTree of 0x%X", currentPeer);

                    peerHandler = getPeerHandler(currentPeer);
                    // no tree available -> no chunks were created or backup system is deactivated
                    if (peerHandler != null) {
                        data.putShort(currentPeer);
                        peerHandler.receiveMetadata(data);
                    }
                }

                if (index == m_assignedPeersIncludingBackups.size()) {
                    index = 0;
                }
                if (index == startIndex) {
                    break;
                }
                currentPeer = m_assignedPeersIncludingBackups.get(index++);
            }
        } else {
            // There might be data in given area even without any assigned peer

            // Allocate array for all data and copy gathered data
            ret = new byte[size];
            data = ByteBuffer.wrap(ret);
            if (nameserviceEntries != null) {
                data.putInt(nameserviceEntries.length);
                data.put(nameserviceEntries);
            } else {
                data.putInt(0);
            }
            if (storages != null) {
                data.putInt(storages.length);
                data.put(storages);
            } else {
                data.putInt(0);
            }
            if (barriers != null) {
                data.putInt(barriers.length);
                data.put(barriers);
            } else {
                data.putInt(0);
            }
        }
        m_dataLock.readLock().unlock();

        // If there is no missing metadata, return an empty array
        if (ret.length == Integer.BYTES * 4) {
            ret = new byte[0];
        }

        return ret;
    }

    /**
     * Deletes all metadata that is not in the responsible area.
     *
     * @param p_responsibleArea
     *         the responsible area
     * @return all unnecessary peers in assigned peer list
     * @lock overlay lock must be read-locked
     */
    public short[] deleteUnnecessaryBackups(final short[] p_responsibleArea) {
        short[] ret = null;
        short currentPeer;
        int index;
        int startIndex;
        int count = 0;

        m_dataLock.writeLock().lock();

        LOGGER.trace("Deleting all uneccessary metadata outside of area: 0x%X, 0x%X", p_responsibleArea[0],
                p_responsibleArea[1]);

        if (!m_assignedPeersIncludingBackups.isEmpty()) {
            ret = new short[m_assignedPeersIncludingBackups.size()];
            index = Collections.binarySearch(m_assignedPeersIncludingBackups, p_responsibleArea[1]);
            if (index < 0) {
                index = index * -1 - 1;
                if (index == m_assignedPeersIncludingBackups.size()) {
                    index = 0;
                }
            }
            startIndex = index;
            currentPeer = m_assignedPeersIncludingBackups.get(index);
            while (!OverlayHelper.isPeerInSuperpeerRange(currentPeer, p_responsibleArea[0], p_responsibleArea[1])) {
                // Remove lookup tree

                LOGGER.trace("Removing LookupTree of 0x%X", currentPeer);

                m_peerHandlers[currentPeer & 0xFFFF] = null;
                ret[count++] = currentPeer;

                if (++index == m_assignedPeersIncludingBackups.size()) {
                    index = 0;
                }
                if (index == startIndex) {
                    break;
                }
                currentPeer = m_assignedPeersIncludingBackups.get(index);
            }
            ret = Arrays.copyOf(ret, count);
        }
        // Remove nameservice entries
        m_nameservice.removeMetadataOutsideOfRange(p_responsibleArea[0], p_responsibleArea[1]);
        // Remove storages
        m_storage.removeMetadataOutsideOfRange(p_responsibleArea[0], p_responsibleArea[1]);
        // Remove barriers
        m_barriers.removeMetadataOutsideOfRange(p_responsibleArea[0], p_responsibleArea[1]);

        m_dataLock.writeLock().unlock();

        return ret;
    }

    /**
     * Stores given metadata.
     *
     * @param p_metadata
     *         the new metadata in an byte array
     * @return new peers to assign
     */
    public short[] storeMetadata(final byte[] p_metadata) {
        short nodeID;
        int size;
        int pos;
        short[] ret = null;
        PeerHandler peerHandler;
        ByteBuffer data;

        if (p_metadata != null && p_metadata.length != 0) {
            data = ByteBuffer.wrap(p_metadata);

            m_dataLock.writeLock().lock();

            LOGGER.trace("Storing metadata. Length: %d", p_metadata.length);

            // Put all nameservice entries
            size = data.getInt();
            pos = data.position();

            LOGGER.trace("Storing nameservice entries. Length: %d", size);

            m_nameservice.storeMetadata(p_metadata, pos, size);
            data.position(pos + size);

            // Put all storages
            size = data.getInt();
            pos = data.position();

            LOGGER.trace("Storing superpeer storages. Length: %d", size);

            m_storage.storeMetadata(p_metadata, pos, size);
            data.position(pos + size);

            // Put all barriers
            size = data.getInt();
            pos = data.position();

            LOGGER.trace("Storing barriers. Length: %d", size);

            m_barriers.storeMetadata(p_metadata, pos, size);
            data.position(pos + size);

            // Put all lookup trees
            size = data.getInt();
            ret = new short[size];

            LOGGER.trace("Storing lookup trees. Length: %d", size);

            for (int i = 0; i < size; i++) {
                nodeID = data.getShort();

                LOGGER.trace("Storing lookup tree of 0x%X", nodeID);

                peerHandler = new PeerHandler(OverlayHelper.ORDER, nodeID);
                peerHandler.storeMetadata(data);

                m_peerHandlers[nodeID & 0xFFFF] = peerHandler;
                ret[i] = nodeID;
            }
            m_dataLock.writeLock().unlock();
        }

        return ret;
    }

    /**
     * Updates the metadata of given backup range
     *
     * @param p_rangeID
     *         the backup range to update
     * @param p_creator
     *         the creator
     * @param p_recoveryPeer
     *         the peer that recovered the backup range
     * @param p_chunkIDRanges
     *         ChunkIDs of all recovered chunks arranged in ranges
     */
    public void updateMetadataAfterRecovery(final short p_rangeID, final short p_creator, final short p_recoveryPeer,
            final long[] p_chunkIDRanges) {

        m_dataLock.writeLock().lock();
        PeerHandler peerHandler = m_peerHandlers[p_creator & 0xFFFF];
        if (peerHandler != null) {
            peerHandler.updateMetadataAfterRecovery(p_rangeID, p_recoveryPeer, p_chunkIDRanges);
        }
        m_dataLock.writeLock().unlock();
    }

    /**
     * Initializes a new backup range.
     *
     * @param p_rangeOwner
     *         the creator of the backup range (not necessarily the creator of the chunks)
     * @param p_backupRange
     *         the backup range to initialize
     * @return whether a new LookupTree was added or not
     */
    public boolean initBackupRangeInLookupTree(final short p_rangeOwner, final BackupRange p_backupRange) {
        boolean ret = false;
        PeerHandler peerHandler;

        m_dataLock.writeLock().lock();
        peerHandler = getPeerHandler(p_rangeOwner);
        // no tree available -> no chunks were created yet
        if (peerHandler == null) {
            // With backup activated this is the place to initialize a lookup tree
            peerHandler = new PeerHandler(OverlayHelper.ORDER, p_rangeOwner);
            m_peerHandlers[p_rangeOwner & 0xFFFF] = peerHandler;
            ret = true;
        }

        peerHandler.initRange(p_backupRange);
        m_dataLock.writeLock().unlock();

        return ret;
    }

    /**
     * Gets corresponding lookup tree.
     *
     * @param p_nodeID
     *         lookup tree's creator
     * @return the lookup tree
     */
    public LookupTree getLookupTree(final short p_nodeID) {
        LookupTree ret = null;
        PeerHandler peerHandler;

        m_dataLock.readLock().lock();
        peerHandler = m_peerHandlers[p_nodeID & 0xFFFF];
        if (peerHandler != null) {
            ret = peerHandler.getLookupTree();
        }
        m_dataLock.readLock().unlock();

        return ret;
    }

    /**
     * Gets corresponding lookup range.
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the lookup range
     */
    public LookupRange getLookupRangeFromLookupTree(final long p_chunkID, boolean p_backupActive) {
        LookupRange ret;
        PeerHandler peerHandler;

        m_dataLock.readLock().lock();
        peerHandler = getPeerHandler(ChunkID.getCreatorID(p_chunkID));
        // no tree available -> no chunks were created or backup system is deactivated
        if (peerHandler != null) {
            ret = peerHandler.getMetadata(p_chunkID);
        } else {
            if (!p_backupActive) {
                // With backup deactivated a lookup tree is only created for migrations -> no migrations
                // -> return complete range
                ret = new LookupRange(ChunkID.getCreatorID(p_chunkID), new long[] {0, (long) Math.pow(2, 48) - 1},
                        LookupState.OK);
            } else {
                ret = new LookupRange(LookupState.DOES_NOT_EXIST);
            }
        }
        m_dataLock.readLock().unlock();

        return ret;
    }

    /**
     * Puts a ChunkID.
     *
     * @param p_chunkID
     *         the ChunkID
     * @param p_owner
     *         the NodeID of the new owner
     * @return whether the ChunkID could be put or not
     */
    public boolean putChunkIDInLookupTree(final long p_chunkID, final short p_owner, final boolean p_backupActive) {
        boolean ret;
        PeerHandler peerHandler;

        m_dataLock.writeLock().lock();
        peerHandler = getPeerHandler(ChunkID.getCreatorID(p_chunkID));
        // no tree available -> no chunks were created or backup system is deactivated
        if (peerHandler == null) {
            if (!p_backupActive) {
                // With backup deactivated this is the place to initialize a peer handler
                short creator = ChunkID.getCreatorID(p_chunkID);
                peerHandler = new PeerHandler(OverlayHelper.ORDER, creator);

                m_peerHandlers[creator & 0xFFFF] = peerHandler;
                return peerHandler.migrate(p_chunkID, p_owner);
            }
            m_dataLock.writeLock().unlock();

            return false;
        } else {
            ret = peerHandler.migrate(p_chunkID, p_owner);
            m_dataLock.writeLock().unlock();

            return ret;
        }
    }

    /**
     * Puts a ChunkID.
     *
     * @param p_firstChunkID
     *         the first ChunkID
     * @param p_lastChunkID
     *         the last ChunkID
     * @param p_owner
     *         the NodeID of the new owner
     * @return whether the ChunkID could be put or not
     */
    public boolean putChunkIDRangeInLookupTree(final long p_firstChunkID, final long p_lastChunkID, final short p_owner,
            final boolean p_backupActive) {
        boolean ret;
        PeerHandler peerHandler;

        m_dataLock.writeLock().lock();
        peerHandler = getPeerHandler(ChunkID.getCreatorID(p_firstChunkID));
        // no tree available -> no chunks were created or backup system is deactivated
        if (peerHandler == null) {
            if (!p_backupActive) {
                // With backup deactivated this is the place to initialize a peer handler
                short creator = ChunkID.getCreatorID(p_firstChunkID);
                peerHandler = new PeerHandler(OverlayHelper.ORDER, creator);

                m_peerHandlers[creator & 0xFFFF] = peerHandler;
                return peerHandler.migrateRange(p_firstChunkID, p_lastChunkID, p_owner);
            }
            m_dataLock.writeLock().unlock();

            return false;
        } else {
            ret = peerHandler.migrateRange(p_firstChunkID, p_lastChunkID, p_owner);
            m_dataLock.writeLock().unlock();

            return ret;
        }
    }

    /**
     * Removes multiple ChunkIDs
     *
     * @param p_chunkIDs
     *         Chunk IDs to remove
     * @return whether the ChunkIDs could be removed or not
     */
    public boolean removeChunkIDsFromLookupTree(final boolean p_backupActive, final long... p_chunkIDs) {
        PeerHandler peerHandler;

        if (p_chunkIDs.length == 0) {
            return false;
        }

        m_dataLock.writeLock().lock();
        peerHandler = getPeerHandler(ChunkID.getCreatorID(p_chunkIDs[0]));
        // no tree available -> no chunks were created or backup system is deactivated
        if (peerHandler == null) {
            m_dataLock.writeLock().unlock();

            // Backup activated and no tree -> error
            // Backup deactivated and no migrations (-> tree is null) -> no need to remove ChunkIDs
            return !p_backupActive;
        } else {
            peerHandler.removeObjects(p_chunkIDs);
            m_dataLock.writeLock().unlock();

            return true;
        }
    }

    /**
     * Returns all backup ranges for given node
     *
     * @param p_nodeID
     *         the NodeID
     * @return all backup ranges
     */
    public BackupRange[] getAllBackupRangesFromLookupTree(final short p_nodeID) {
        BackupRange[] ret = null;
        PeerHandler peerHandler;

        m_dataLock.readLock().lock();
        peerHandler = getPeerHandler(p_nodeID);
        // no tree available -> no chunks were created or backup system is deactivated
        if (peerHandler != null) {
            ret = peerHandler.getAllBackupRanges();
        }
        m_dataLock.readLock().unlock();

        return ret;
    }

    /* Nameservice */

    /**
     * Replaces given peer from specific backup ranges as backup peer
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_failedPeer
     *         the failed peer
     * @param p_newBackupPeer
     *         the replacement
     */
    public void replaceFailedPeerInLookupTree(final short p_rangeID, final short p_nodeID, final short p_failedPeer,
            final short p_newBackupPeer) {
        PeerHandler peerHandler;

        m_dataLock.writeLock().lock();
        peerHandler = getPeerHandler(p_nodeID);
        // no tree available -> no chunks were created or backup system is deactivated
        if (peerHandler != null) {
            // Replace failedPeer from specific backup peer lists
            peerHandler.replaceBackupPeer(p_rangeID, p_failedPeer, p_newBackupPeer);
        }
        m_dataLock.writeLock().unlock();
    }

    /**
     * Gets nameservice entry.
     *
     * @param p_nameserviceID
     *         the nameservice ID
     * @return the ChunkID
     */
    public long getNameserviceEntry(final int p_nameserviceID) {
        long ret;

        m_dataLock.readLock().lock();
        ret = m_nameservice.getChunkID(p_nameserviceID);
        m_dataLock.readLock().unlock();

        return ret;
    }

    /**
     * Puts a single nameservice entry.
     *
     * @param p_nameserviceID
     *         the nameservice ID
     * @param p_chunkID
     *         the ChunkID
     */
    public void putNameserviceEntry(final int p_nameserviceID, final long p_chunkID) {
        m_dataLock.writeLock().lock();
        m_nameservice.putChunkID(p_nameserviceID, p_chunkID);
        m_dataLock.writeLock().unlock();
    }

    /**
     * Counts nameservice entries within range.
     *
     * @param p_bound1
     *         lowest NodeID
     * @param p_bound2
     *         highest NodeID (might be smaller than p_bound1)
     * @return the number of nameservice entries in given NodeID range
     */
    public int countNameserviceEntries(final short p_bound1, final short p_bound2) {
        int ret;

        m_dataLock.readLock().lock();
        ret = m_nameservice.quantifyMetadata(p_bound1, p_bound2);
        m_dataLock.readLock().unlock();

        return ret;
    }

    /* Superpeer Storage */

    /**
     * Creates a metadata storage.
     *
     * @param p_storageID
     *         the storage ID
     * @param p_size
     *         the size of metadata storage
     * @return 0 on success, -1 if quota reached, -2 if max num entries reached, -3 if id already in use
     */
    public int createStorage(final int p_storageID, final int p_size) {
        int ret;

        m_dataLock.writeLock().lock();
        ret = m_storage.create(p_storageID, p_size);
        m_dataLock.writeLock().unlock();

        return ret;
    }

    /**
     * Gets a metadata storage.
     *
     * @param p_storageID
     *         the storage ID
     * @return the data
     */
    public byte[] getStorage(final int p_storageID) {
        byte[] ret;

        m_dataLock.readLock().lock();
        ret = m_storage.get(p_storageID);
        m_dataLock.readLock().unlock();

        return ret;
    }

    /**
     * Puts data in metadata storage.
     *
     * @param p_storageID
     *         the storage ID
     * @param p_data
     *         the data
     * @return number of bytes written to the block or -1 if the block does not exist
     */
    public int putStorage(final int p_storageID, final byte[] p_data) {
        int ret;

        m_dataLock.readLock().lock();
        ret = m_storage.put(p_storageID, p_data);
        m_dataLock.readLock().unlock();

        return ret;
    }

    /**
     * Removes metadata storage.
     *
     * @param p_storageID
     *         the storage ID
     * @return false if the block does not exist, true on success
     */
    public boolean removeStorage(final int p_storageID) {
        boolean ret;

        m_dataLock.writeLock().lock();
        ret = m_storage.remove(p_storageID);
        m_dataLock.writeLock().unlock();

        return ret;
    }

    /**
     * Creates a barrier.
     *
     * @param p_nodeID
     *         the creator
     * @param p_size
     *         the number of peers to sign on
     * @return barrier ID on success, -1 on failure
     */
    public int createBarrier(final short p_nodeID, final int p_size) {
        int ret;

        m_dataLock.writeLock().lock();
        ret = m_barriers.allocateBarrier(p_nodeID, p_size);
        m_dataLock.writeLock().unlock();

        return ret;
    }

    /* Barriers */

    /**
     * Changes size of a barrier.
     *
     * @param p_nodeID
     *         the creator
     * @param p_barrierID
     *         the barrier ID
     * @param p_newSize
     *         the new size of the barrier
     * @return true if changing size was successful, false otherwise
     */
    public boolean changeSizeOfBarrier(final short p_nodeID, final int p_barrierID, final int p_newSize) {
        boolean ret;

        m_dataLock.writeLock().lock();
        ret = m_barriers.changeBarrierSize(p_nodeID, p_barrierID, p_newSize);
        m_dataLock.writeLock().unlock();

        return ret;
    }

    /**
     * Removes a barrier.
     *
     * @param p_nodeID
     *         the creator
     * @param p_barrierID
     *         the barrier ID
     * @return true if successful, false on failure
     */
    public boolean removeBarrier(final short p_nodeID, final int p_barrierID) {
        boolean ret;

        m_dataLock.writeLock().lock();
        ret = m_barriers.freeBarrier(p_nodeID, p_barrierID);
        m_dataLock.writeLock().unlock();

        return ret;
    }

    /**
     * Resets a barrier.
     *
     * @param p_nodeID
     *         the creator
     * @param p_barrierID
     *         the barrier ID
     * @return true if successful, false otherwise
     */
    public boolean resetBarrier(final short p_nodeID, final int p_barrierID) {
        boolean ret;

        m_dataLock.writeLock().lock();
        ret = m_barriers.reset(p_nodeID, p_barrierID);
        m_dataLock.writeLock().unlock();

        return ret;
    }

    /**
     * Signs-on on barrier.
     *
     * @param p_nodeID
     *         the creator
     * @param p_barrierID
     *         the barrier ID
     * @param p_nodeIDToSignOn
     *         the NodeID
     * @param p_barrierData
     *         the barrier data
     * @return the number of peers left to sign on, -1 on failure
     */
    public int signOnBarrier(final short p_nodeID, final int p_barrierID, final short p_nodeIDToSignOn,
            final long p_barrierData) {
        int ret;

        m_dataLock.writeLock().lock();
        ret = m_barriers.signOn(p_nodeID, p_barrierID, p_nodeIDToSignOn, p_barrierData);
        m_dataLock.writeLock().unlock();

        return ret;
    }

    /**
     * Returns the sign on status of a barrier
     *
     * @param p_nodeID
     *         the creator
     * @param p_barrierID
     *         the barrier ID
     * @return BarrierStatus with results of the sign or null if the specified barrier does not exist
     */
    public BarrierStatus getSignOnStatusOfBarrier(final short p_nodeID, final int p_barrierID) {
        BarrierStatus ret;

        m_dataLock.readLock().lock();
        ret = m_barriers.getBarrierSignOnStatus(p_nodeID, p_barrierID);
        m_dataLock.readLock().unlock();

        return ret;
    }

    /**
     * Gets corresponding peer handler.
     *
     * @param p_nodeID
     *         the creator
     * @return the peer handler
     */
    private PeerHandler getPeerHandler(final short p_nodeID) {
        return m_peerHandlers[p_nodeID & 0xFFFF];
    }

}

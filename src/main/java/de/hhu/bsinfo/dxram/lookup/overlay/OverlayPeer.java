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

package de.hhu.bsinfo.dxram.lookup.overlay;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.data.ChunkAnon;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupState;
import de.hhu.bsinfo.dxram.lookup.events.NameserviceCacheEntryUpdateEvent;
import de.hhu.bsinfo.dxram.lookup.events.NodeJoinEvent;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutSuccessorRequest;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutSuccessorResponse;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierAllocRequest;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierAllocResponse;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierChangeSizeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierChangeSizeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierFreeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierFreeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierGetStatusRequest;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierGetStatusResponse;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierReleaseMessage;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierSignOnRequest;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierSignOnResponse;
import de.hhu.bsinfo.dxram.lookup.messages.FinishedStartupMessage;
import de.hhu.bsinfo.dxram.lookup.messages.GetAllBackupRangesRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetAllBackupRangesResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetChunkIDForNameserviceEntryRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetChunkIDForNameserviceEntryResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetLookupRangeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetLookupRangeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetNameserviceEntriesRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetNameserviceEntriesResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetNameserviceEntryCountRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetNameserviceEntryCountResponse;
import de.hhu.bsinfo.dxram.lookup.messages.InitRangeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.InitRangeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.InsertNameserviceEntriesRequest;
import de.hhu.bsinfo.dxram.lookup.messages.InsertNameserviceEntriesResponse;
import de.hhu.bsinfo.dxram.lookup.messages.JoinRequest;
import de.hhu.bsinfo.dxram.lookup.messages.JoinResponse;
import de.hhu.bsinfo.dxram.lookup.messages.LookupMessages;
import de.hhu.bsinfo.dxram.lookup.messages.MigrateRangeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.MigrateRangeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.MigrateRequest;
import de.hhu.bsinfo.dxram.lookup.messages.MigrateResponse;
import de.hhu.bsinfo.dxram.lookup.messages.NameserviceUpdatePeerCachesMessage;
import de.hhu.bsinfo.dxram.lookup.messages.NodeJoinEventMessage;
import de.hhu.bsinfo.dxram.lookup.messages.PingSuperpeerMessage;
import de.hhu.bsinfo.dxram.lookup.messages.RemoveChunkIDsRequest;
import de.hhu.bsinfo.dxram.lookup.messages.RemoveChunkIDsResponse;
import de.hhu.bsinfo.dxram.lookup.messages.ReplaceBackupPeerRequest;
import de.hhu.bsinfo.dxram.lookup.messages.ReplaceBackupPeerResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SendSuperpeersMessage;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageCreateRequest;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageCreateResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageGetAnonRequest;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageGetAnonResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageGetRequest;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageGetResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStoragePutAnonRequest;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStoragePutAnonResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStoragePutRequest;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStoragePutResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageRemoveMessage;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageStatusRequest;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageStatusResponse;
import de.hhu.bsinfo.dxram.lookup.messages.UpdateMetadataAfterRecoveryMessage;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.NameserviceEntry;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.NameserviceHashTable;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.SuperpeerStorage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.ArrayListLong;
import de.hhu.bsinfo.dxutils.CRC16;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Peer functionality for overlay
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 24.05.2018
 */
public class OverlayPeer implements MessageReceiver {
    private static final Logger LOGGER = LogManager.getFormatterLogger(OverlayPeer.class);

    private static final int MSG_TIMEOUT_MS = 100;

    // Attributes
    private AbstractBootComponent m_boot;
    private NetworkComponent m_network;
    private EventComponent m_event;

    private short m_nodeID;
    private short m_mySuperpeer = NodeID.INVALID_ID;
    private ArrayList<Short> m_superpeers;
    private int m_initialNumberOfSuperpeers;
    private ReentrantReadWriteLock m_overlayLock;

    /**
     * Creates an instance of OverlayPeer
     *
     * @param p_nodeID
     *         the own NodeID
     * @param p_contactSuperpeer
     *         the superpeer to contact for joining
     * @param p_initialNumberOfSuperpeers
     *         the number of expeced superpeers
     * @param p_boot
     *         the BootComponent
     * @param p_network
     *         the NetworkComponent
     * @param p_event
     *         the EventComponent
     */
    public OverlayPeer(final short p_nodeID, final short p_contactSuperpeer, final int p_initialNumberOfSuperpeers,
            final AbstractBootComponent p_boot, final NetworkComponent p_network, final EventComponent p_event) {
        m_boot = p_boot;
        m_network = p_network;
        m_event = p_event;

        m_initialNumberOfSuperpeers = p_initialNumberOfSuperpeers;

        m_nodeID = p_nodeID;

        registerNetworkMessages();
        registerNetworkMessageListener();

        m_overlayLock = new ReentrantReadWriteLock(false);
        joinSuperpeerOverlay(p_contactSuperpeer);
    }

    /* Lookup */

    /**
     * Returns all known superpeers
     *
     * @return array with all superpeers
     */
    public ArrayList<Short> getAllSuperpeers() {
        ArrayList<Short> ret;

        m_overlayLock.readLock().lock();
        ret = m_superpeers;
        m_overlayLock.readLock().unlock();

        return ret;
    }

    /**
     * Get the number of entries in name service
     *
     * @return the number of name service entries
     */
    public int getNameserviceEntryCount() {
        int ret = 0;
        Short[] superpeers;
        GetNameserviceEntryCountRequest request;
        GetNameserviceEntryCountResponse response;

        m_overlayLock.readLock().lock();
        superpeers = m_superpeers.toArray(new Short[m_superpeers.size()]);
        m_overlayLock.readLock().unlock();

        for (short superpeer : superpeers) {
            request = new GetNameserviceEntryCountRequest(superpeer);
            try {
                m_network.sendSync(request);
            } catch (final NetworkException ignored) {

                LOGGER.error("Could not determine nameservice entry count");

                ret = -1;
                break;
            }

            response = request.getResponse(GetNameserviceEntryCountResponse.class);
            ret += response.getCount();
        }

        return ret;
    }

    /**
     * Get all available nameservice entries.
     *
     * @return List of nameservice entries or null on error;
     */
    public ArrayList<NameserviceEntry> getNameserviceEntries() {
        ArrayList<NameserviceEntry> entries = new ArrayList<>();
        Short[] superpeers;
        GetNameserviceEntriesRequest request;
        GetNameserviceEntriesResponse response;

        m_overlayLock.readLock().lock();
        superpeers = m_superpeers.toArray(new Short[m_superpeers.size()]);
        m_overlayLock.readLock().unlock();

        for (short superpeer : superpeers) {
            request = new GetNameserviceEntriesRequest(superpeer);
            try {
                m_network.sendSync(request);
            } catch (final NetworkException ignored) {

                LOGGER.error("Could not determine nameservice entries");

                entries = null;
                break;
            }

            response = request.getResponse(GetNameserviceEntriesResponse.class);
            entries.addAll(NameserviceHashTable.convert(response.getEntries()));
        }

        return entries;
    }

    /* Name Service */

    /**
     * Get the corresponding LookupRange for the given ChunkID
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the current location and the range borders
     */
    public LookupRange getLookupRange(final long p_chunkID) {
        LookupRange ret = null;
        short nodeID;
        short responsibleSuperpeer;
        boolean check = false;

        GetLookupRangeRequest request;
        GetLookupRangeResponse response;

        m_overlayLock.readLock().lock();
        if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
            check = true;
        }
        nodeID = ChunkID.getCreatorID(p_chunkID);
        responsibleSuperpeer = getResponsibleSuperpeer(nodeID, check);
        m_overlayLock.readLock().unlock();

        if (responsibleSuperpeer != NodeID.INVALID_ID) {
            request = new GetLookupRangeRequest(responsibleSuperpeer, p_chunkID);
            try {
                m_network.sendSync(request);
            } catch (final NetworkException e) {
                // Responsible superpeer is not available
                return new LookupRange(LookupState.DATA_TEMPORARY_UNAVAILABLE);
            }

            response = request.getResponse(GetLookupRangeResponse.class);

            ret = response.getLookupRange();
        }

        return ret;
    }

    /**
     * Returns the responsible superpeer for given peer
     *
     * @param p_nodeID
     *         the peer
     * @return the responsible superpeer
     */
    public short getResponsibleSuperpeer(final short p_nodeID) {
        short ret;

        m_overlayLock.readLock().lock();
        if (m_nodeID == p_nodeID) {
            ret = m_mySuperpeer;
        } else {
            ret = OverlayHelper.getResponsibleSuperpeer(p_nodeID, m_superpeers);
        }
        m_overlayLock.readLock().unlock();

        return ret;
    }

    /**
     * Remove the ChunkIDs from range after deletion of that chunks
     *
     * @param p_chunkIDs
     *         the ChunkIDs
     */
    public void removeChunkIDs(final ArrayListLong p_chunkIDs) {
        short responsibleSuperpeer;
        short[] backupSuperpeers;

        RemoveChunkIDsRequest request;
        RemoveChunkIDsResponse response;

        while (true) {
            m_overlayLock.readLock().lock();
            responsibleSuperpeer = m_mySuperpeer;
            m_overlayLock.readLock().unlock();

            request = new RemoveChunkIDsRequest(responsibleSuperpeer, p_chunkIDs, false);
            try {
                m_network.sendSync(request);
            } catch (final NetworkException e) {
                // Responsible superpeer is not available, try again (superpeers will be updated
                // automatically by network thread)
                try {
                    Thread.sleep(MSG_TIMEOUT_MS);
                } catch (final InterruptedException ignored) {
                }
                continue;
            }

            response = request.getResponse(RemoveChunkIDsResponse.class);

            backupSuperpeers = response.getBackupSuperpeers();
            if (backupSuperpeers != null) {
                if (backupSuperpeers[0] != NodeID.INVALID_ID) {
                    // Send backups
                    for (short backupSuperpeer : backupSuperpeers) {
                        request = new RemoveChunkIDsRequest(backupSuperpeer, p_chunkIDs, true);
                        try {
                            m_network.sendSync(request);
                        } catch (final NetworkException e) {
                            // Ignore superpeer failure, own superpeer will fix this
                        }
                    }
                }
                break;
            }
        }
    }

    /**
     * Insert a new name service entry
     *
     * @param p_id
     *         the AID
     * @param p_chunkID
     *         the ChunkID
     */
    public void insertNameserviceEntry(final int p_id, final long p_chunkID) {
        short responsibleSuperpeer;
        short[] backupSuperpeers;
        boolean check = false;
        InsertNameserviceEntriesRequest request;
        InsertNameserviceEntriesResponse response;

        // Insert ChunkID <-> ApplicationID mapping
        assert p_id < Math.pow(2, 31) && p_id >= 0;

        m_overlayLock.readLock().lock();
        if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
            check = true;
        }
        responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_id), check);
        m_overlayLock.readLock().unlock();

        while (true) {
            if (responsibleSuperpeer != NodeID.INVALID_ID) {
                request = new InsertNameserviceEntriesRequest(responsibleSuperpeer, p_id, p_chunkID, false);
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // Responsible superpeer is not available, try again (superpeers will be updated
                    // automatically by network thread)
                    try {
                        Thread.sleep(MSG_TIMEOUT_MS);
                    } catch (final InterruptedException ignored) {
                    }

                    m_overlayLock.readLock().lock();
                    responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_id), check);
                    m_overlayLock.readLock().unlock();

                    continue;
                }

                response = request.getResponse(InsertNameserviceEntriesResponse.class);

                backupSuperpeers = response.getBackupSuperpeers();
                if (backupSuperpeers != null) {
                    if (backupSuperpeers[0] != NodeID.INVALID_ID) {
                        // Send backups
                        for (short backupSuperpeer : backupSuperpeers) {
                            request = new InsertNameserviceEntriesRequest(backupSuperpeer, p_id, p_chunkID, true);
                            try {
                                m_network.sendSync(request);
                            } catch (final NetworkException e) {
                                // Ignore superpeer failure, own superpeer will fix this
                            }
                        }
                    }
                    break;
                }
            }

            m_overlayLock.readLock().lock();
            responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_id), check);
            m_overlayLock.readLock().unlock();
        }
    }

    /* Migration */

    /**
     * Get ChunkID for give nameservice id. Use this if you assume
     * that your entry has to exist.
     *
     * @param p_id
     *         the nameservice id
     * @param p_timeoutMs
     *         Timeout for trying to get the entry (if it does not exist, yet).
     *         set this to -1 for infinite loop if you know for sure, that the entry has to exist
     * @return the corresponding ChunkID
     */
    public long getChunkIDForNameserviceEntry(final int p_id, final int p_timeoutMs) {
        long ret = ChunkID.INVALID_ID;
        short responsibleSuperpeer;
        boolean check = false;
        GetChunkIDForNameserviceEntryRequest request;

        // Resolve ChunkID <-> ApplicationID mapping to return corresponding ChunkID
        m_overlayLock.readLock().lock();
        if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
            check = true;
        }
        responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_id), check);
        m_overlayLock.readLock().unlock();

        long start = System.currentTimeMillis();
        do {
            if (responsibleSuperpeer != NodeID.INVALID_ID) {
                request = new GetChunkIDForNameserviceEntryRequest(responsibleSuperpeer, p_id);
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // Responsible superpeer is not available, try again (superpeers will be updated
                    // automatically by network thread)
                    try {
                        Thread.sleep(MSG_TIMEOUT_MS);
                    } catch (final InterruptedException ignored) {
                    }

                    m_overlayLock.readLock().lock();
                    responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_id), check);
                    m_overlayLock.readLock().unlock();

                    continue;
                }

                ret = request.getResponse(GetChunkIDForNameserviceEntryResponse.class).getChunkID();

                if (ret != -1) {
                    break;
                }
            }

            m_overlayLock.readLock().lock();
            responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_id), check);
            m_overlayLock.readLock().unlock();
        } while (p_timeoutMs == -1 || System.currentTimeMillis() - start < p_timeoutMs);

        return ret;
    }

    /**
     * Store migration of given ChunkID to a new location
     *
     * @param p_chunkID
     *         the ChunkID
     * @param p_nodeID
     *         the new owner
     */
    public void migrate(final long p_chunkID, final short p_nodeID) {
        short responsibleSuperpeer;
        boolean finished = false;

        MigrateRequest request;

        while (!finished) {
            m_overlayLock.readLock().lock();
            responsibleSuperpeer = m_mySuperpeer;
            m_overlayLock.readLock().unlock();

            request = new MigrateRequest(responsibleSuperpeer, p_chunkID, p_nodeID, false);
            try {
                m_network.sendSync(request);
            } catch (final NetworkException e) {
                // Responsible superpeer is not available, try again (superpeers will be updated
                // automatically by network thread)
                try {
                    Thread.sleep(MSG_TIMEOUT_MS);
                } catch (final InterruptedException ignored) {
                }
                continue;
            }

            finished = request.getResponse(MigrateResponse.class).getStatus();
        }
    }

    /* Backup */

    /**
     * Store migration of a range of ChunkIDs to a new location
     *
     * @param p_startCID
     *         the first ChunkID
     * @param p_endCID
     *         the last ChunkID
     * @param p_nodeID
     *         the new owner
     */
    public void migrateRange(final long p_startCID, final long p_endCID, final short p_nodeID) {
        short creator;
        short responsibleSuperpeer;
        boolean finished = false;

        MigrateRangeRequest request;

        creator = ChunkID.getCreatorID(p_startCID);
        if (creator != ChunkID.getCreatorID(p_endCID)) {

            LOGGER.error("Start and end object's creators not equal");

        } else {
            while (!finished) {
                m_overlayLock.readLock().lock();
                responsibleSuperpeer = m_mySuperpeer;
                m_overlayLock.readLock().unlock();

                request = new MigrateRangeRequest(responsibleSuperpeer, p_startCID, p_endCID, p_nodeID, false);
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // Responsible superpeer is not available, try again (superpeers will be updated
                    // automatically by network thread)
                    try {
                        Thread.sleep(MSG_TIMEOUT_MS);
                    } catch (final InterruptedException ignored) {
                    }
                    continue;
                }

                finished = request.getResponse(MigrateRangeResponse.class).getStatus();
            }
        }
    }

    /**
     * Initialize a new backup range
     *
     * @param p_backupRange
     *         the backup range to initialize
     */
    public void initRange(final BackupRange p_backupRange) {
        short responsibleSuperpeer;
        boolean finished = false;

        InitRangeRequest request;

        while (!finished) {
            m_overlayLock.readLock().lock();
            responsibleSuperpeer = m_mySuperpeer;
            m_overlayLock.readLock().unlock();

            request = new InitRangeRequest(responsibleSuperpeer, m_nodeID, p_backupRange, false);
            try {
                m_network.sendSync(request);
            } catch (final NetworkException e) {
                // Responsible superpeer is not available, try again (superpeers will be updated
                // automatically by network thread)
                try {
                    Thread.sleep(MSG_TIMEOUT_MS);
                } catch (final InterruptedException ignored) {
                }
                continue;
            }

            finished = request.getResponse(InitRangeResponse.class).getStatus();
        }
    }

    /* Recovery */

    /**
     * Get all backup ranges for given node
     *
     * @param p_nodeID
     *         the NodeID
     * @return all backup ranges for given node
     */
    public BackupRange[] getAllBackupRanges(final short p_nodeID) {
        BackupRange[] ret = null;
        short responsibleSuperpeer;
        boolean check = false;

        GetAllBackupRangesRequest request;
        GetAllBackupRangesResponse response;

        m_overlayLock.readLock().lock();
        if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
            check = true;
        }
        responsibleSuperpeer = getResponsibleSuperpeer(p_nodeID, check);
        m_overlayLock.readLock().unlock();

        while (ret == null) {
            if (responsibleSuperpeer != NodeID.INVALID_ID) {
                request = new GetAllBackupRangesRequest(responsibleSuperpeer, p_nodeID);
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // Responsible superpeer is not available, try again and check responsible superpeer
                    check = true;

                    m_overlayLock.readLock().lock();
                    responsibleSuperpeer = getResponsibleSuperpeer(p_nodeID, true);
                    m_overlayLock.readLock().unlock();

                    continue;
                }

                response = request.getResponse(GetAllBackupRangesResponse.class);
                ret = response.getBackupRanges();
            }

            m_overlayLock.readLock().lock();
            responsibleSuperpeer = getResponsibleSuperpeer(p_nodeID, check);
            m_overlayLock.readLock().unlock();
        }

        return ret;
    }

    /**
     * Checks if all superpeers are offline
     *
     * @return if all superpeers are offline
     */
    public boolean allSuperpeersDown() {
        boolean ret = true;
        short superpeer;
        int i = 0;

        m_overlayLock.readLock().lock();
        try {
            m_network.sendMessage(new PingSuperpeerMessage(m_mySuperpeer));
            ret = false;
        } catch (final NetworkException ignored) {
            if (!m_superpeers.isEmpty()) {
                while (i < m_superpeers.size()) {
                    superpeer = m_superpeers.get(i++);

                    try {
                        m_network.sendMessage(new PingSuperpeerMessage(superpeer));
                    } catch (final NetworkException ignored2) {
                        continue;
                    }

                    ret = false;
                    break;
                }
            }
        }

        m_overlayLock.readLock().unlock();

        return ret;
    }

    /**
     * Allocate a new barrier.
     *
     * @param p_size
     *         Size of the barrier (i.e. number of peers that have to sign on).
     * @return Id of the barrier allocated or -1 on failure.
     */
    public int barrierAllocate(final int p_size) {
        // the superpeer responsible for the peer will be the storage for this barrier
        m_overlayLock.readLock().lock();
        BarrierAllocRequest request = new BarrierAllocRequest(m_mySuperpeer, p_size, false);
        m_overlayLock.readLock().unlock();

        try {
            m_network.sendSync(request);
        } catch (final NetworkException e) {

            LOGGER.error("Allocating barrier with size %d on superpeer 0x%X failed: %s", p_size, m_mySuperpeer, e);

            return BarrierID.INVALID_ID;
        }

        BarrierAllocResponse response = (BarrierAllocResponse) request.getResponse();
        return response.getBarrierId();
    }

    /**
     * Free an allocate barrier.
     *
     * @param p_barrierId
     *         Id of the barrier to free.
     * @return True if successful, false otherwise.
     */
    public boolean barrierFree(final int p_barrierId) {
        if (p_barrierId == BarrierID.INVALID_ID) {
            return false;
        }

        short responsibleSuperpeer = BarrierID.getOwnerID(p_barrierId);
        BarrierFreeRequest message = new BarrierFreeRequest(responsibleSuperpeer, p_barrierId, false);

        try {
            m_network.sendSync(message);
        } catch (final NetworkException e) {

            LOGGER.error("Freeing barrier 0x%X on superpeer 0x%X failed: %s", p_barrierId, responsibleSuperpeer, e);

            return false;
        }

        BarrierFreeResponse response = (BarrierFreeResponse) message.getResponse();
        if (response.getStatus() == -1) {

            LOGGER.error("Freeing barrier 0x%X on superpeer 0x%X failed: barrier does not exist", p_barrierId,
                    responsibleSuperpeer);

            return false;
        }

        return true;
    }

    /**
     * Alter the size of an existing barrier (i.e. you want to keep the barrier id but with a different size).
     *
     * @param p_barrierId
     *         Id of an allocated barrier to change the size of.
     * @param p_size
     *         New size for the barrier.
     * @return True if changing size was successful, false otherwise.
     */
    public boolean barrierChangeSize(final int p_barrierId, final int p_size) {
        if (p_barrierId == BarrierID.INVALID_ID) {
            return false;
        }

        short responsibleSuperpeer = BarrierID.getOwnerID(p_barrierId);
        BarrierChangeSizeRequest request =
                new BarrierChangeSizeRequest(responsibleSuperpeer, p_barrierId, p_size, false);
        try {
            m_network.sendSync(request);
        } catch (final NetworkException e) {

            LOGGER.error("Sending barrier change size request to superpeer 0x%X failed: %s", responsibleSuperpeer, e);

            return false;
        }

        BarrierChangeSizeResponse response = (BarrierChangeSizeResponse) request.getResponse();

        if (response.getStatus() != 0) {
            LOGGER.error("Changing size of barrier 0x%X failed", p_barrierId);
        }

        return response.getStatus() == 0;
    }

    /**
     * Sign on to a barrier and wait for it getting released (number of peers, barrier size, have signed on).
     *
     * @param p_barrierId
     *         Id of the barrier to sign on to.
     * @param p_customData
     *         Custom data to pass along with the sign on
     * @param p_waitForRelease
     *         True to wait for the barrier to be released, false to just sign on and don't wait for release
     *         (e.g. signal for remotes)
     * @return A pair consisting of the list of signed on peers and their custom data passed along with the sign ons,
     * null on error
     */
    public BarrierStatus barrierSignOn(final int p_barrierId, final long p_customData, final boolean p_waitForRelease) {
        if (p_barrierId == BarrierID.INVALID_ID) {
            return null;
        }

        Semaphore waitForRelease = new Semaphore(0);
        MessageReceiver msg = null;
        final BarrierReleaseMessage[] releaseMessage = {null};

        if (p_waitForRelease) {
            msg = p_message -> {
                if (p_message != null) {
                    if (p_message.getType() == DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE) {
                        switch (p_message.getSubtype()) {
                            case LookupMessages.SUBTYPE_BARRIER_RELEASE_MESSAGE: {
                                releaseMessage[0] = (BarrierReleaseMessage) p_message;
                                if (releaseMessage[0].getBarrierId() == p_barrierId) {
                                    waitForRelease.release();
                                }
                                break;
                            }
                            default:
                                break;
                        }
                    }
                }
            };

            // make sure to register the listener BEFORE sending the sign on to not miss the release message
            m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_RELEASE_MESSAGE,
                    msg);
        }

        short responsibleSuperpeer = BarrierID.getOwnerID(p_barrierId);
        BarrierSignOnRequest request = new BarrierSignOnRequest(responsibleSuperpeer, p_barrierId, p_customData);
        try {
            m_network.sendSync(request);
        } catch (final NetworkException e) {

            LOGGER.error("Sign on barrier 0x%X failed: %s", p_barrierId, e);

            if (p_waitForRelease) {
                m_network.unregister(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                        LookupMessages.SUBTYPE_BARRIER_RELEASE_MESSAGE, msg);
            }

            return null;
        }

        BarrierSignOnResponse response = (BarrierSignOnResponse) request.getResponse();
        if (response.getBarrierId() != p_barrierId || response.getStatus() != 0) {

            LOGGER.error("Sign on barrier 0x%X failed", p_barrierId);

            if (p_waitForRelease) {
                m_network.unregister(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                        LookupMessages.SUBTYPE_BARRIER_RELEASE_MESSAGE, msg);
            }

            return null;
        }

        if (p_waitForRelease) {
            try {
                waitForRelease.acquire();
            } catch (final InterruptedException ignored) {
            }

            m_network.unregister(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_RELEASE_MESSAGE,
                    msg);

            return releaseMessage[0].getBarrierResults();
        } else {
            // Return empty status on no wait
            return new BarrierStatus();
        }
    }

    /**
     * Get the status of a barrier.
     *
     * @param p_barrierId
     *         Id of the barrier.
     * @return Status of the barrier or null if the barrier does not exist
     */
    public BarrierStatus barrierGetStatus(final int p_barrierId) {
        if (p_barrierId == BarrierID.INVALID_ID) {
            return null;
        }

        m_overlayLock.readLock().lock();
        BarrierGetStatusRequest request = new BarrierGetStatusRequest(m_mySuperpeer, p_barrierId);
        m_overlayLock.readLock().unlock();
        try {
            m_network.sendSync(request);
        } catch (final NetworkException e) {

            LOGGER.error("Getting status request of barrier 0x%X failed: %s", p_barrierId, e);

            return null;
        }

        BarrierGetStatusResponse response = (BarrierGetStatusResponse) request.getResponse();
        if (response.getStatus() == -1) {

            LOGGER.error("Getting status request of barrier 0x%X failed: barrier does not exist", p_barrierId);

            return null;
        }

        return response.getBarrierStatus();
    }

    /**
     * Create a block of memory in the superpeer storage.
     *
     * @param p_storageId
     *         Local storage id to assign to the newly created block.
     * @param p_size
     *         Size of the block to create
     * @return True if creating successful, false if failed.
     */
    public boolean superpeerStorageCreate(final int p_storageId, final int p_size) {
        assert p_storageId < Math.pow(2, 31) && p_storageId >= 0;

        boolean check = false;
        short responsibleSuperpeer;

        m_overlayLock.readLock().lock();
        if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
            check = true;
        }
        responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_storageId), check);
        m_overlayLock.readLock().unlock();

        while (true) {
            if (responsibleSuperpeer != NodeID.INVALID_ID) {
                SuperpeerStorageCreateRequest request =
                        new SuperpeerStorageCreateRequest(responsibleSuperpeer, p_storageId, p_size, false);
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // Responsible superpeer is not available, try again (superpeers will be updated
                    // automatically by network thread)
                    try {
                        Thread.sleep(MSG_TIMEOUT_MS);
                    } catch (final InterruptedException ignored) {
                    }

                    m_overlayLock.readLock().lock();
                    responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_storageId), check);
                    m_overlayLock.readLock().unlock();

                    continue;
                }

                SuperpeerStorageCreateResponse response = request.getResponse(SuperpeerStorageCreateResponse.class);
                if (response.getStatus() != 0) {

                    LOGGER.error("Allocating temporary storage on superpeer 0x%X for 0x%X, size %d failed: %d",
                            response.getSource(), p_storageId, p_size, response.getStatus());

                    return false;
                } else {
                    return true;
                }
            }

            m_overlayLock.readLock().lock();
            responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_storageId), check);
            m_overlayLock.readLock().unlock();
        }
    }

    /**
     * Put data into an allocated block in the superpeer storage.
     *
     * @param p_chunk
     *         Data structure with data to put.
     * @return True if successful, false otherwise.
     */
    public boolean superpeerStoragePut(final AbstractChunk p_chunk) {
        if (p_chunk.getID() > 0x7FFFFFFF && p_chunk.getID() < 0) {

            LOGGER.error("Cannot put data structure into superpeer storage, invalid id 0x%X", p_chunk.getID());

            return false;
        }

        int storageId = (int) (p_chunk.getID() & 0x7FFFFFFF);

        boolean check = false;
        short responsibleSuperpeer;

        m_overlayLock.readLock().lock();
        if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
            check = true;
        }
        responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
        m_overlayLock.readLock().unlock();

        while (true) {
            if (responsibleSuperpeer != NodeID.INVALID_ID) {
                SuperpeerStoragePutRequest request =
                        new SuperpeerStoragePutRequest(responsibleSuperpeer, p_chunk, false);
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // Responsible superpeer is not available, try again (superpeers will be updated
                    // automatically by network thread)
                    try {
                        Thread.sleep(MSG_TIMEOUT_MS);
                    } catch (final InterruptedException ignored) {
                    }

                    m_overlayLock.readLock().lock();
                    responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
                    m_overlayLock.readLock().unlock();

                    continue;
                }

                SuperpeerStoragePutResponse response = request.getResponse(SuperpeerStoragePutResponse.class);
                return response.getStatus() == 0;
            }

            m_overlayLock.readLock().lock();
            responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
            m_overlayLock.readLock().unlock();
        }
    }

    /**
     * Put data of an anonymous chunk into an allocated block in the superpeer storage.
     *
     * @param p_chunk
     *         Chunk with data to put.
     * @return True if successful, false otherwise.
     */
    public boolean superpeerStoragePutAnon(final ChunkAnon p_chunk) {
        if (p_chunk.getID() > 0x7FFFFFFF && p_chunk.getID() < 0) {

            LOGGER.error("Cannot put data structure into superpeer storage, invalid id 0x%X", p_chunk.getID());

            return false;
        }

        int storageId = (int) (p_chunk.getID() & 0x7FFFFFFF);

        boolean check = false;
        short responsibleSuperpeer;

        m_overlayLock.readLock().lock();
        if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
            check = true;
        }
        responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
        m_overlayLock.readLock().unlock();

        while (true) {
            if (responsibleSuperpeer != NodeID.INVALID_ID) {
                SuperpeerStoragePutAnonRequest request =
                        new SuperpeerStoragePutAnonRequest(responsibleSuperpeer, p_chunk, false);
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // Responsible superpeer is not available, try again (superpeers will be updated
                    // automatically by network thread)
                    try {
                        Thread.sleep(MSG_TIMEOUT_MS);
                    } catch (final InterruptedException ignored) {
                    }

                    m_overlayLock.readLock().lock();
                    responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
                    m_overlayLock.readLock().unlock();

                    continue;
                }

                SuperpeerStoragePutAnonResponse response = request.getResponse(SuperpeerStoragePutAnonResponse.class);
                return response.getStatus() == 0;
            }

            m_overlayLock.readLock().lock();
            responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
            m_overlayLock.readLock().unlock();
        }
    }

    /**
     * Get data from an allocated block in the superpeer storage.
     *
     * @param p_chunk
     *         Data structure with set storage id to read the data from the storage into.
     * @return True if successful, false otherwise.
     */
    public boolean superpeerStorageGet(final AbstractChunk p_chunk) {
        if (p_chunk.getID() > 0x7FFFFFFF && p_chunk.getID() < 0) {

            LOGGER.error("Cannot get data structure from superpeer storage, invalid id 0x%X", p_chunk.getID());

            return false;
        }

        int storageId = (int) (p_chunk.getID() & 0x7FFFFFFF);

        boolean check = false;
        short responsibleSuperpeer;

        m_overlayLock.readLock().lock();
        if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
            check = true;
        }
        responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
        m_overlayLock.readLock().unlock();

        while (true) {
            if (responsibleSuperpeer != NodeID.INVALID_ID) {
                SuperpeerStorageGetRequest request = new SuperpeerStorageGetRequest(responsibleSuperpeer, p_chunk);
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // Responsible superpeer is not available, try again (superpeers will be updated
                    // automatically by network thread)
                    try {
                        Thread.sleep(MSG_TIMEOUT_MS);
                    } catch (final InterruptedException ignored) {
                    }

                    m_overlayLock.readLock().lock();
                    responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
                    m_overlayLock.readLock().unlock();

                    continue;
                }

                SuperpeerStorageGetResponse response = request.getResponse(SuperpeerStorageGetResponse.class);
                return response.getStatus() == 0;
            }

            m_overlayLock.readLock().lock();
            responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
            m_overlayLock.readLock().unlock();
        }
    }

    /**
     * Get data from an allocated block in the superpeer storage.
     *
     * @param p_chunk
     *         Reference to an instance of an anonymous chunk
     * @return True if successful, false otherwise.
     */
    public boolean superpeerStorageGetAnon(final ChunkAnon p_chunk) {
        if (p_chunk.getID() > 0x7FFFFFFF && p_chunk.getID() < 0) {

            LOGGER.error("Cannot get data structure from superpeer storage, invalid id 0x%X", p_chunk.getID());

            return false;
        }

        int storageId = (int) (p_chunk.getID() & 0x7FFFFFFF);

        boolean check = false;
        short responsibleSuperpeer;

        m_overlayLock.readLock().lock();
        if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
            check = true;
        }
        responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
        m_overlayLock.readLock().unlock();

        while (true) {
            if (responsibleSuperpeer != NodeID.INVALID_ID) {
                SuperpeerStorageGetAnonRequest request =
                        new SuperpeerStorageGetAnonRequest(responsibleSuperpeer, p_chunk);
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // Responsible superpeer is not available, try again (superpeers will be updated
                    // automatically by network thread)
                    try {
                        Thread.sleep(MSG_TIMEOUT_MS);
                    } catch (final InterruptedException ignored) {
                    }

                    m_overlayLock.readLock().lock();
                    responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
                    m_overlayLock.readLock().unlock();

                    continue;
                }

                SuperpeerStorageGetAnonResponse response = request.getResponse(SuperpeerStorageGetAnonResponse.class);
                return response.getStatus() == 0;
            }

            m_overlayLock.readLock().lock();
            responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(storageId), check);
            m_overlayLock.readLock().unlock();
        }
    }

    /**
     * Remove an allocated block in the superpeer storage.
     *
     * @param p_superpeerStorageId
     *         Id of the allocated block to remove.
     */
    public void superpeerStorageRemove(final int p_superpeerStorageId) {
        boolean check = false;
        short responsibleSuperpeer;

        m_overlayLock.readLock().lock();
        if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
            check = true;
        }
        responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_superpeerStorageId), check);
        m_overlayLock.readLock().unlock();

        while (true) {
            if (responsibleSuperpeer != NodeID.INVALID_ID) {
                SuperpeerStorageRemoveMessage message =
                        new SuperpeerStorageRemoveMessage(responsibleSuperpeer, p_superpeerStorageId, false);
                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {
                    // Responsible superpeer is not available, try again (superpeers will be updated
                    // automatically by network thread)
                    try {
                        Thread.sleep(MSG_TIMEOUT_MS);
                    } catch (final InterruptedException ignored) {
                    }

                    m_overlayLock.readLock().lock();
                    responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_superpeerStorageId), check);
                    m_overlayLock.readLock().unlock();

                    continue;
                }

                return;
            }

            m_overlayLock.readLock().lock();
            responsibleSuperpeer = getResponsibleSuperpeer(CRC16.hash(p_superpeerStorageId), check);
            m_overlayLock.readLock().unlock();
        }
    }

    /**
     * Get the status of the superpeer storage.
     *
     * @return Status of the superpeer storage.
     */
    public SuperpeerStorage.Status superpeerStorageGetStatus() {
        SuperpeerStorage.Status[] statusArray = new SuperpeerStorage.Status[m_superpeers.size()];

        m_overlayLock.readLock().lock();
        for (int i = 0; i < m_superpeers.size(); i++) {
            short superpeer = m_superpeers.get(i);
            m_overlayLock.readLock().unlock();

            SuperpeerStorageStatusRequest request = new SuperpeerStorageStatusRequest(superpeer);
            try {
                m_network.sendSync(request);
                statusArray[i] = request.getResponse(SuperpeerStorageStatusResponse.class).getStatus();
            } catch (final NetworkException e) {

                LOGGER.error("Getting superpeer 0x%X storage status failed", superpeer);

                statusArray[i] = null;
            }

            m_overlayLock.readLock().lock();
        }
        m_overlayLock.readLock().unlock();

        // aggregate status...bad performance =(
        ArrayList<Long> aggregatedStatus = new ArrayList<>();
        for (SuperpeerStorage.Status aStatusArray : statusArray) {
            ArrayList<Long> toMergeArray = aStatusArray.getStatusArray();
            toMergeArray.stream().filter(val -> !aggregatedStatus.contains(val)).forEach(aggregatedStatus::add);
        }

        // and finally...sort
        aggregatedStatus.sort((p_o1, p_o2) -> {
            Integer i1 = (int) (p_o1 >> 32);
            Integer i2 = (int) (p_o2 >> 32);

            return i1.compareTo(i2);
        });

        return new SuperpeerStorage.Status(statusArray[0].getMaxNumItems(), statusArray[0].getMaxStorageSizeBytes(),
                aggregatedStatus);
    }

    /**
     * Replaces the backup peer for given range on responsible superpeer
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_failedPeer
     *         the failed peer
     * @param p_newPeer
     *         the replacement
     */
    public void replaceBackupPeer(final short p_rangeID, final short p_failedPeer, final short p_newPeer) {
        short responsibleSuperpeer;

        m_overlayLock.readLock().lock();
        responsibleSuperpeer = m_mySuperpeer;
        m_overlayLock.readLock().unlock();

        ReplaceBackupPeerRequest request =
                new ReplaceBackupPeerRequest(responsibleSuperpeer, p_rangeID, p_failedPeer, p_newPeer, false);

        try {
            m_network.sendSync(request);
        } catch (final NetworkException e) {

            LOGGER.error("Replacing backup peer on 0x%X failed", responsibleSuperpeer);

        }
    }

    /**
     * Handles an incoming Message
     *
     * @param p_message
     *         the Message
     */
    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE:
                        incomingSendSuperpeersMessage((SendSuperpeersMessage) p_message);
                        break;
                    case LookupMessages.SUBTYPE_NAMESERVICE_UPDATE_PEER_CACHES_MESSAGE:
                        incomingNameserviceUpdatePeerCachesMessage((NameserviceUpdatePeerCachesMessage) p_message);
                        break;
                    case LookupMessages.SUBTYPE_NODE_JOIN_EVENT_REQUEST:
                        incomingNodeJoinEventRequest((NodeJoinEventMessage) p_message);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    /**
     * Informs responsible superpeer about finished startup
     */
    public boolean finishStartup(final short p_rack, final short p_switch, final boolean p_availableForBackup,
            int p_capabilities, final IPV4Unit p_address) {
        short responsibleSuperpeer;

        while (true) {
            m_overlayLock.readLock().lock();
            responsibleSuperpeer = m_mySuperpeer;
            m_overlayLock.readLock().unlock();

            try {
                m_network.sendMessage(
                        new FinishedStartupMessage(responsibleSuperpeer, p_rack, p_switch, p_availableForBackup,
                                p_capabilities, p_address));
            } catch (final NetworkException ignore) {
                // Try again. Responsible superpeer is changed automatically.
                continue;
            }
            break;
        }

        return true;
    }

    /**
     * Joins the superpeer overlay through contactSuperpeer
     *
     * @param p_contactSuperpeer
     *         NodeID of a known superpeer
     * @return whether joining was successful
     * @lock no need for acquiring overlay lock in this method
     */
    private boolean joinSuperpeerOverlay(final short p_contactSuperpeer) {
        short contactSuperpeer;
        JoinRequest joinRequest;
        JoinResponse joinResponse = null;

        LOGGER.trace("Entering joinSuperpeerOverlay with: p_contactSuperpeer=0x%X", p_contactSuperpeer);

        contactSuperpeer = p_contactSuperpeer;

        if (p_contactSuperpeer == NodeID.INVALID_ID) {

            LOGGER.error("Cannot join superpeer overlay, no bootstrap superpeer available to contact.");

            return false;
        }

        while (contactSuperpeer != NodeID.INVALID_ID) {
            LOGGER.debug("Contacting 0x%X to get the responsible superpeer, I am 0x%X", contactSuperpeer, m_nodeID);

            joinRequest = new JoinRequest(contactSuperpeer, m_boot.getDetails());

            try {
                m_network.sendSync(joinRequest);
            } catch (final NetworkException e) {
                // Contact superpeer is not available, get a new contact superpeer
                contactSuperpeer = m_boot.getBootstrapId();
                continue;
            }

            joinResponse = joinRequest.getResponse(JoinResponse.class);
            contactSuperpeer = joinResponse.getNewContactSuperpeer();
        }
        assert joinResponse != null;
        m_superpeers = joinResponse.getSuperpeers();
        m_mySuperpeer = joinResponse.getSource();
        OverlayHelper.insertSuperpeer(m_mySuperpeer, m_superpeers);

        LOGGER.trace("Exiting joinSuperpeerOverlay");

        return true;
    }

    /**
     * Determines the responsible superpeer for given NodeID
     *
     * @param p_nodeID
     *         NodeID from chunk whose location is searched
     * @param p_check
     *         whether the result has to be checked (in case of incomplete superpeer overlay) or not
     * @return the responsible superpeer for given ChunkID
     * @lock overlay lock must be read-locked
     */
    private short getResponsibleSuperpeer(final short p_nodeID, final boolean p_check) {
        short responsibleSuperpeer = NodeID.INVALID_ID;
        short predecessor;
        short hisSuccessor;
        int index;
        AskAboutSuccessorRequest request;
        AskAboutSuccessorResponse response;

        LOGGER.trace("Entering getResponsibleSuperpeer with: p_nodeID=0x%X", p_nodeID);

        if (!m_superpeers.isEmpty()) {
            index = Collections.binarySearch(m_superpeers, p_nodeID);
            if (index < 0) {
                index = index * -1 - 1;
                if (index == m_superpeers.size()) {
                    index = 0;
                }
            }
            responsibleSuperpeer = m_superpeers.get(index);

            if (p_check && m_superpeers.size() > 1) {
                if (index == 0) {
                    index = m_superpeers.size() - 1;
                } else {
                    index--;
                }
                predecessor = m_superpeers.get(index);

                while (true) {
                    m_overlayLock.readLock().unlock();
                    request = new AskAboutSuccessorRequest(predecessor);
                    try {
                        m_network.sendSync(request);
                    } catch (final NetworkException e) {
                        // Predecessor is not available, try responsibleSuperpeer without checking
                        m_overlayLock.readLock().lock();
                        break;
                    }

                    m_overlayLock.readLock().lock();

                    response = request.getResponse(AskAboutSuccessorResponse.class);
                    hisSuccessor = response.getSuccessor();
                    if (responsibleSuperpeer == hisSuccessor) {
                        break;
                    } else if (OverlayHelper.isSuperpeerInRange(p_nodeID, predecessor, hisSuccessor)) {
                        responsibleSuperpeer = hisSuccessor;
                        break;
                    } else {
                        predecessor = hisSuccessor;
                    }
                }
            }
        } else {

            LOGGER.warn("Do not know any superpeer");

        }

        LOGGER.trace("Exiting getResponsibleSuperpeer");

        return responsibleSuperpeer;
    }

    /**
     * Handles an incoming SendSuperpeersMessage
     *
     * @param p_sendSuperpeersMessage
     *         the SendSuperpeersMessage
     */
    private void incomingSendSuperpeersMessage(final SendSuperpeersMessage p_sendSuperpeersMessage) {
        short source;

        source = p_sendSuperpeersMessage.getSource();

        LOGGER.trace("Got Message: SEND_SUPERPEERS_MESSAGE from 0x%X", source);

        m_overlayLock.writeLock().lock();
        m_superpeers = p_sendSuperpeersMessage.getSuperpeers();
        OverlayHelper.insertSuperpeer(source, m_superpeers);

        if (m_mySuperpeer != source) {
            if (source == getResponsibleSuperpeer(m_nodeID, false)) {
                m_mySuperpeer = source;
            }
        }
        m_overlayLock.writeLock().unlock();
    }

    /**
     * Handles an incoming NameserviceUpdatePeerCachesMessage
     *
     * @param p_message
     *         the NameserviceUpdatePeerCachesMessage
     */
    private void incomingNameserviceUpdatePeerCachesMessage(final NameserviceUpdatePeerCachesMessage p_message) {
        m_event.fireEvent(new NameserviceCacheEntryUpdateEvent(getClass().getSimpleName(), p_message.getID(),
                p_message.getChunkID()));
    }

    /**
     * Handles an incoming NodeJoinEventMessage
     *
     * @param p_nodeJoinEventMessage
     *         the NodeJoinEventMessage
     */
    private void incomingNodeJoinEventRequest(final NodeJoinEventMessage p_nodeJoinEventMessage) {
        LOGGER.trace("Got request: NodeJoinEventMessage 0x%X", p_nodeJoinEventMessage.getSource());

        // Notify other components/services
        m_event.fireEvent(new NodeJoinEvent(getClass().getSimpleName(), p_nodeJoinEventMessage.getJoinedPeer(),
                p_nodeJoinEventMessage.getRole(), p_nodeJoinEventMessage.getCapabilities(),
                p_nodeJoinEventMessage.getRack(), p_nodeJoinEventMessage.getSwitch(),
                p_nodeJoinEventMessage.isAvailableForBackup(), p_nodeJoinEventMessage.getAddress()));
    }

    // -----------------------------------------------------------------------------------

    /**
     * Register network messages we use in here.
     */
    private void registerNetworkMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_JOIN_REQUEST,
                JoinRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_JOIN_RESPONSE,
                JoinResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_FINISHED_STARTUP_MESSAGE, FinishedStartupMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_REQUEST, GetLookupRangeRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_RESPONSE, GetLookupRangeResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_REQUEST, RemoveChunkIDsRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_RESPONSE, RemoveChunkIDsResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_REQUEST, InsertNameserviceEntriesRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_RESPONSE, InsertNameserviceEntriesResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST,
                GetChunkIDForNameserviceEntryRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_RESPONSE,
                GetChunkIDForNameserviceEntryResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_REQUEST, GetNameserviceEntryCountRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_RESPONSE, GetNameserviceEntryCountResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRIES_REQUEST, GetNameserviceEntriesRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRIES_RESPONSE, GetNameserviceEntriesResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_NAMESERVICE_UPDATE_PEER_CACHES_MESSAGE,
                NameserviceUpdatePeerCachesMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_MIGRATE_REQUEST,
                MigrateRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_MIGRATE_RESPONSE,
                MigrateResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST, MigrateRangeRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_MIGRATE_RANGE_RESPONSE, MigrateRangeResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST,
                InitRangeRequest.class);
        m_network
                .registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_INIT_RANGE_RESPONSE,
                        InitRangeResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_REQUEST, GetAllBackupRangesRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_RESPONSE, GetAllBackupRangesResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_UPDATE_METADATA_AFTER_RECOVERY_MESSAGE,
                UpdateMetadataAfterRecoveryMessage.class);

        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE, PingSuperpeerMessage.class);

        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST, AskAboutSuccessorRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE, AskAboutSuccessorResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE, SendSuperpeersMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_NODE_JOIN_EVENT_REQUEST, NodeJoinEventMessage.class);

        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_BARRIER_ALLOC_REQUEST, BarrierAllocRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_BARRIER_ALLOC_RESPONSE, BarrierAllocResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_BARRIER_FREE_REQUEST, BarrierFreeRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_BARRIER_FREE_RESPONSE, BarrierFreeResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_BARRIER_SIGN_ON_REQUEST, BarrierSignOnRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_BARRIER_SIGN_ON_RESPONSE, BarrierSignOnResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_BARRIER_RELEASE_MESSAGE, BarrierReleaseMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_BARRIER_STATUS_REQUEST, BarrierGetStatusRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_BARRIER_STATUS_RESPONSE, BarrierGetStatusResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_REQUEST, BarrierChangeSizeRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_RESPONSE, BarrierChangeSizeResponse.class);

        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_CREATE_REQUEST, SuperpeerStorageCreateRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_CREATE_RESPONSE, SuperpeerStorageCreateResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_REQUEST, SuperpeerStorageGetRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_RESPONSE, SuperpeerStorageGetResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_ANON_REQUEST, SuperpeerStorageGetAnonRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_ANON_RESPONSE, SuperpeerStorageGetAnonResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_REQUEST, SuperpeerStoragePutRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_RESPONSE, SuperpeerStoragePutResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_ANON_REQUEST, SuperpeerStoragePutAnonRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_ANON_RESPONSE, SuperpeerStoragePutAnonResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_REMOVE_MESSAGE, SuperpeerStorageRemoveMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_STATUS_REQUEST, SuperpeerStorageStatusRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_STATUS_RESPONSE, SuperpeerStorageStatusResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_REPLACE_BACKUP_PEER_RESPONSE, ReplaceBackupPeerResponse.class);
    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network
                .register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_NAMESERVICE_UPDATE_PEER_CACHES_MESSAGE, this);
        m_network
                .register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_NODE_JOIN_EVENT_REQUEST, this);
    }

}

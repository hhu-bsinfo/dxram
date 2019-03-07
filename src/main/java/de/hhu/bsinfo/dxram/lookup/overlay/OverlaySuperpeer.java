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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.boot.NodeRegistry;
import de.hhu.bsinfo.dxram.chunk.data.ChunkAnon;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.failure.messages.FailureRequest;
import de.hhu.bsinfo.dxram.failure.messages.FailureResponse;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.events.NodeJoinEvent;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutBackupsRequest;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutBackupsResponse;
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
import de.hhu.bsinfo.dxram.lookup.messages.GetMetadataSummaryRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetMetadataSummaryResponse;
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
import de.hhu.bsinfo.dxram.lookup.messages.NotifyAboutNewPredecessorMessage;
import de.hhu.bsinfo.dxram.lookup.messages.NotifyAboutNewSuccessorMessage;
import de.hhu.bsinfo.dxram.lookup.messages.PingSuperpeerMessage;
import de.hhu.bsinfo.dxram.lookup.messages.RemoveChunkIDsRequest;
import de.hhu.bsinfo.dxram.lookup.messages.RemoveChunkIDsResponse;
import de.hhu.bsinfo.dxram.lookup.messages.ReplaceBackupPeerRequest;
import de.hhu.bsinfo.dxram.lookup.messages.ReplaceBackupPeerResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SendBackupsMessage;
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
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarriersTable;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.LookupTree;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.MetadataHandler;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.NameserviceHashTable;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.PeerHandler;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.PeerState;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.SuperpeerStorage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.recovery.messages.RecoverBackupRangeRequest;
import de.hhu.bsinfo.dxram.recovery.messages.RecoverBackupRangeResponse;
import de.hhu.bsinfo.dxram.recovery.messages.RecoveryMessages;
import de.hhu.bsinfo.dxram.recovery.messages.ReplicateBackupRangeRequest;
import de.hhu.bsinfo.dxram.recovery.messages.ReplicateBackupRangeResponse;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.ArrayListLong;
import de.hhu.bsinfo.dxutils.CRC16;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Superpeer functionality for overlay
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class OverlaySuperpeer implements MessageReceiver {

    private static final Logger LOGGER = LogManager.getFormatterLogger(OverlaySuperpeer.class);

    // Attributes
    private NetworkComponent m_network;
    private AbstractBootComponent m_boot;
    private EventComponent m_event;

    private boolean m_backupActive;

    private short m_nodeID;
    private short m_predecessor = NodeID.INVALID_ID;
    private short m_successor = NodeID.INVALID_ID;
    private int m_initialNumberOfSuperpeers;

    // All known superpeers: New superpeers are added when there is a new predecessor or successor
    // and during fix fingers method (fixSuperpeers) in stabilization thread
    private ArrayList<Short> m_superpeers;
    // All assigned peers (between this superpeer and his predecessor): New peers are added when
    // a peer joins the overlay and this superpeer is responsible and when another superpeer left
    // the overlay and this superpeer takes over
    private ArrayList<Short> m_peers;
    // All assigned peers including the backups (between this superpeer and his fourth predecessor):
    // New peers are added when a peer initializes a backup range for the first time on this superpeer
    // (also backups) and when applying backups from another superpeer and the peer is not yet in the
    // list
    private ArrayList<Short> m_assignedPeersIncludingBackups;

    private MetadataHandler m_metadata;

    private SuperpeerStabilizationThread m_stabilizationThread;

    private ReentrantReadWriteLock m_overlayLock;

    /**
     * Creates an instance of OverlaySuperpeer
     *
     * @param p_nodeID
     *         the own NodeID
     * @param p_contactSuperpeer
     *         the superpeer to contact for joining
     * @param p_initialNumberOfSuperpeers
     *         the number of expeced superpeers
     * @param p_sleepInterval
     *         the ping interval in ms
     * @param p_maxNumOfBarriers
     *         Max number of barriers
     * @param p_storageMaxNumEntries
     *         Max number of entries for the superpeer storage (-1 to disable)
     * @param p_storageMaxSizeBytes
     *         Max size for the superpeer storage in bytes
     * @param p_backupActive
     *         whether backup component is active or not
     * @param p_boot
     *         the BootComponent
     * @param p_network
     *         the NetworkComponent
     */
    public OverlaySuperpeer(final short p_nodeID, final short p_contactSuperpeer, final int p_initialNumberOfSuperpeers,
            final int p_sleepInterval, final int p_maxNumOfBarriers, final int p_storageMaxNumEntries,
            final int p_storageMaxSizeBytes, final boolean p_backupActive, final AbstractBootComponent p_boot,
            final NetworkComponent p_network, final EventComponent p_event) {
        m_boot = p_boot;
        m_network = p_network;
        m_event = p_event;

        m_backupActive = p_backupActive;

        m_nodeID = p_nodeID;
        m_initialNumberOfSuperpeers = p_initialNumberOfSuperpeers;

        m_superpeers = new ArrayList<>();
        m_peers = new ArrayList<>();
        m_assignedPeersIncludingBackups = new ArrayList<>();

        m_metadata = new MetadataHandler(new PeerHandler[NodeID.MAX_ID], new NameserviceHashTable(1000),
                new SuperpeerStorage(p_storageMaxNumEntries, p_storageMaxSizeBytes),
                new BarriersTable(p_maxNumOfBarriers, m_nodeID), m_assignedPeersIncludingBackups);

        m_overlayLock = new ReentrantReadWriteLock(false);

        m_initialNumberOfSuperpeers--;

        registerNetworkMessages();
        registerNetworkMessageListener();

        createOrJoinSuperpeerOverlay(p_contactSuperpeer, p_sleepInterval);
    }

    /**
     * Returns whether this superpeer is last in overlay or not
     *
     * @return whether this superpeer is last in overlay or not
     */
    public boolean isLastSuperpeer() {
        boolean ret = true;
        short superpeer;
        int i = 0;

        m_overlayLock.readLock().lock();
        if (!m_superpeers.isEmpty()) {
            while (i < m_superpeers.size()) {
                superpeer = m_superpeers.get(i++);
                try {
                    m_network.sendMessage(new PingSuperpeerMessage(superpeer));
                } catch (final NetworkException ignored) {
                    continue;
                }

                ret = false;
                break;
            }
        }
        m_overlayLock.readLock().unlock();

        return ret;
    }

    /**
     * Shuts down the stabilization thread
     */
    public void shutdown() {
        m_stabilizationThread.interrupt();
        m_stabilizationThread.shutdown();
        try {
            m_stabilizationThread.join();

            LOGGER.debug("Shutdown of StabilizationThread successful");
        } catch (final InterruptedException ignored) {
            LOGGER.warn("Could not wait for stabilization thread to finish. Interrupted");
        }
    }

    /**
     * Returns the corresponding lookup tree
     *
     * @param p_nodeID
     *         the NodeID
     * @return the lookup tree
     */
    public LookupTree getLookupTree(final short p_nodeID) {
        return m_metadata.getLookupTree(p_nodeID);
    }

    /**
     * Determines if this superpeer is responsible for failure handling
     *
     * @param p_failedNode
     *         NodeID of failed node
     * @param p_nodeRole
     *         NodeRole of failed node
     * @return true if superpeer is responsible for failed node, false otherwise
     */
    public boolean nodeFailureHandling(final short p_failedNode, final NodeRole p_nodeRole) {
        if (p_nodeRole == NodeRole.SUPERPEER) {
            return superpeerFailureHandling(p_failedNode);
        } else {
            return peerFailureHandling(p_failedNode);
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

        LOGGER.trace("Entering incomingMessage with: p_message=%s", p_message);

        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case LookupMessages.SUBTYPE_JOIN_REQUEST:
                        incomingJoinRequest((JoinRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_FINISHED_STARTUP_MESSAGE:
                        incomingFinishedStartupMessage((FinishedStartupMessage) p_message);
                        break;
                    case LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_REQUEST:
                        incomingGetLookupRangeRequest((GetLookupRangeRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_REQUEST:
                        incomingRemoveChunkIDsRequest((RemoveChunkIDsRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_REQUEST:
                        incomingInsertNameserviceEntriesRequest((InsertNameserviceEntriesRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST:
                        incomingGetChunkIDForNameserviceEntryRequest((GetChunkIDForNameserviceEntryRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_REQUEST:
                        incomingGetNameserviceEntryCountRequest((GetNameserviceEntryCountRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRIES_REQUEST:
                        incomingGetNameserviceEntriesRequest((GetNameserviceEntriesRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_MIGRATE_REQUEST:
                        incomingMigrateRequest((MigrateRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST:
                        incomingMigrateRangeRequest((MigrateRangeRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_INIT_RANGE_REQUEST:
                        incomingInitRangeRequest((InitRangeRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_REQUEST:
                        incomingGetAllBackupRangesRequest((GetAllBackupRangesRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_UPDATE_METADATA_AFTER_RECOVERY_MESSAGE:
                        incomingUpdateMetadataAfterRecoveryMessage((UpdateMetadataAfterRecoveryMessage) p_message);
                        break;
                    case LookupMessages.SUBTYPE_NODE_JOIN_EVENT_REQUEST:
                        incomingPeerJoinEventRequest((NodeJoinEventMessage) p_message);
                        break;
                    case LookupMessages.SUBTYPE_BARRIER_ALLOC_REQUEST:
                        incomingBarrierAllocRequest((BarrierAllocRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_BARRIER_FREE_REQUEST:
                        incomingBarrierFreeRequest((BarrierFreeRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_BARRIER_SIGN_ON_REQUEST:
                        incomingBarrierSignOnRequest((BarrierSignOnRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_BARRIER_STATUS_REQUEST:
                        incomingBarrierGetStatusRequest((BarrierGetStatusRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_REQUEST:
                        incomingBarrierChangeSizeRequest((BarrierChangeSizeRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_CREATE_REQUEST:
                        incomingSuperpeerStorageCreateRequest((SuperpeerStorageCreateRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_REQUEST:
                        incomingSuperpeerStorageGetRequest((SuperpeerStorageGetRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_ANON_REQUEST:
                        incomingSuperpeerStorageGetAnonRequest((SuperpeerStorageGetAnonRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_REQUEST:
                        incomingSuperpeerStoragePutRequest((SuperpeerStoragePutRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_ANON_REQUEST:
                        incomingSuperpeerStoragePutAnonRequest((SuperpeerStoragePutAnonRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_REMOVE_MESSAGE:
                        incomingSuperpeerStorageRemoveMessage((SuperpeerStorageRemoveMessage) p_message);
                        break;
                    case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_STATUS_REQUEST:
                        incomingSuperpeerStorageStatusRequest((SuperpeerStorageStatusRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_GET_METADATA_SUMMARY_REQUEST:
                        incomingGetMetadataSummaryRequest((GetMetadataSummaryRequest) p_message);
                        break;
                    case LookupMessages.SUBTYPE_REPLACE_BACKUP_PEER_REQUEST:
                        incomingReplaceBackupPeerRequest((ReplaceBackupPeerRequest) p_message);
                        break;
                    default:
                        break;
                }
            }
        }

        LOGGER.trace("Exiting incomingMessage");

    }

    /**
     * Returns current predecessor
     *
     * @return the predecessor
     * @lock overlay lock must be write-locked
     */
    protected short getPredecessor() {
        return m_predecessor;
    }

    /**
     * Sets the predecessor for the current superpeer
     *
     * @param p_nodeID
     *         NodeID of the predecessor
     * @lock overlay lock must be write-locked
     */
    protected void setPredecessor(final short p_nodeID) {
        m_predecessor = p_nodeID;
        if (m_predecessor != m_successor) {
            OverlayHelper.insertSuperpeer(m_predecessor, m_superpeers);
        }
    }

    /**
     * Returns current successor
     *
     * @return the sucessor
     * @lock overlay lock must be write-locked
     */
    protected short getSuccessor() {
        return m_successor;
    }

    /**
     * Sets the successor for the current superpeer
     *
     * @param p_nodeID
     *         NodeID of the successor
     * @lock overlay lock must be write-locked
     */
    protected void setSuccessor(final short p_nodeID) {
        m_successor = p_nodeID;
        if (m_successor != NodeID.INVALID_ID && m_nodeID != m_successor) {
            OverlayHelper.insertSuperpeer(m_successor, m_superpeers);
        }
    }

    /**
     * Returns all peers
     *
     * @return all peers
     * @lock overlay lock must be write-locked
     */
    protected ArrayList<Short> getPeers() {
        return m_peers;
    }

    /**
     * Determines all peers that are in the responsible area
     *
     * @param p_firstSuperpeer
     *         the first superpeer
     * @param p_lastSuperpeer
     *         the last superpeer
     * @return all peers in responsible area
     * @lock overlay lock must be read-locked
     */
    ArrayList<Short> getPeersInResponsibleArea(final short p_firstSuperpeer, final short p_lastSuperpeer) {
        short currentPeer;
        int index;
        int startIndex;
        ArrayList<Short> peers;

        peers = new ArrayList<Short>();
        if (!m_assignedPeersIncludingBackups.isEmpty()) {
            // Search for the first superpeer in list of all assigned peers
            index = Collections.binarySearch(m_assignedPeersIncludingBackups, p_firstSuperpeer);
            // Result must be negative because there is no peer with NodeID of a superpeer
            // Get the index where the superpeer would be in the list to get first peer with higher NodeID
            index = index * -1 - 1;
            if (index == m_assignedPeersIncludingBackups.size()) {
                // There is no peer with higher NodeID -> take the one with lowest NodeID
                index = 0;
            }

            startIndex = index;
            currentPeer = m_assignedPeersIncludingBackups.get(index++);
            while (OverlayHelper.isPeerInSuperpeerRange(currentPeer, p_firstSuperpeer, p_lastSuperpeer)) {
                // Add current peer to peer list
                peers.add(Collections.binarySearch(peers, currentPeer) * -1 - 1, currentPeer);
                if (index == m_assignedPeersIncludingBackups.size()) {
                    index = 0;
                }
                if (index == startIndex) {
                    break;
                }
                currentPeer = m_assignedPeersIncludingBackups.get(index++);
            }
        }

        return peers;
    }

    /**
     * Returns the number of nameservice entries in given area
     *
     * @param p_responsibleArea
     *         the area
     * @return the number of nameservice entries
     */
    int getNumberOfNameserviceEntries(final short[] p_responsibleArea) {
        return m_metadata.getNumberOfNameserviceEntries(p_responsibleArea);
    }

    /**
     * Returns the number of storages in given area
     *
     * @param p_responsibleArea
     *         the area
     * @return the number of storages
     */
    int getNumberOfStorages(final short[] p_responsibleArea) {
        return m_metadata.getNumberOfStorages(p_responsibleArea);
    }

    /**
     * Returns the number of barriers in given area
     *
     * @param p_responsibleArea
     *         the area
     * @return the number of barriers
     */
    int getNumberOfBarriers(final short[] p_responsibleArea) {
        return m_metadata.getNumberOfBarriers(p_responsibleArea);
    }

    /**
     * Compares given peer list with local list and returns all missing backup data
     *
     * @param p_peers
     *         all peers the requesting superpeer stores backups for
     * @param p_numberOfNameserviceEntries
     *         the number of expected nameservice entries
     * @param p_numberOfStorages
     *         the number of expected storages
     * @param p_numberOfBarriers
     *         the number of expected barriers
     * @return the backup data of missing peers in given peer list
     * @lock overlay lock must be write-locked
     */
    byte[] compareAndReturnBackups(final ArrayList<Short> p_peers, final int p_numberOfNameserviceEntries,
            final int p_numberOfStorages, final int p_numberOfBarriers) {
        return m_metadata
                .compareAndReturnBackups(p_peers, p_numberOfNameserviceEntries, p_numberOfStorages, p_numberOfBarriers,
                        m_predecessor, m_nodeID);
    }

    /**
     * Stores given backups
     *
     * @param p_missingMetadata
     *         the new metadata in a byte array
     * @lock overlay lock must be write-locked
     */
    void storeIncomingBackups(final byte[] p_missingMetadata) {
        short[] newPeers;

        newPeers = m_metadata.storeMetadata(p_missingMetadata);
        if (newPeers != null) {
            for (short peer : newPeers) {
                addToAssignedPeers(peer);
            }
        }
    }

    /**
     * Deletes all metadata of peers and superpeers that are not in the responsible area
     *
     * @param p_responsibleArea
     *         the responsible area
     * @lock overlay lock must be write-locked
     */
    void deleteUnnecessaryBackups(final short[] p_responsibleArea) {
        short[] peersToRemove;

        peersToRemove = m_metadata.deleteUnnecessaryBackups(p_responsibleArea);
        if (peersToRemove != null) {
            for (short peer : peersToRemove) {
                removeFromAssignedPeers(peer);
            }
        }
    }

    /**
     * Takes over failed superpeers peers
     *
     * @param p_nodeID
     *         the NodeID
     * @lock overlay lock must be write-locked
     */
    void takeOverPeers(final short p_nodeID) {
        short predecessor;
        short firstPeer;
        short currentPeer;
        int index;
        int startIndex;

        if (m_superpeers.isEmpty()) {
            firstPeer = (short) (m_nodeID + 1);
        } else {
            index = Collections.binarySearch(m_superpeers, p_nodeID);
            if (index < 0) {
                index = index * -1 - 1;
            }
            if (index == 0) {
                predecessor = m_superpeers.get(m_superpeers.size() - 1);
            } else {
                predecessor = m_superpeers.get(index - 1);
            }
            if (predecessor == p_nodeID) {
                firstPeer = (short) (m_nodeID + 1);
            } else {
                firstPeer = predecessor;
            }
        }

        if (!m_assignedPeersIncludingBackups.isEmpty()) {
            index = Collections.binarySearch(m_assignedPeersIncludingBackups, firstPeer);
            if (index < 0) {
                index = index * -1 - 1;
                if (index == m_assignedPeersIncludingBackups.size()) {
                    index = 0;
                }
            }
            startIndex = index;
            currentPeer = m_assignedPeersIncludingBackups.get(index++);
            while (OverlayHelper.isPeerInSuperpeerRange(currentPeer, firstPeer, p_nodeID)) {
                if (Collections.binarySearch(m_peers, currentPeer) < 0 &&
                        Collections.binarySearch(m_superpeers, currentPeer) < 0) {
                    if (m_metadata.getState(currentPeer) == PeerState.ONLINE) {

                        LOGGER.info("** Taking over 0x%X", currentPeer);

                        OverlayHelper.insertPeer(currentPeer, m_peers);
                        addToAssignedPeers(currentPeer);
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
        }
    }

    /**
     * Updates the metadata on this superpeer and all responsible superpeers after recovery
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_restorer
     *         the NodeID of the restorer
     * @param p_chunkIDRanges
     *         all ChunkIDs in ranges
     * @lock overlay lock must be write-locked
     */
    private void updateMetadata(final short p_rangeID, final short p_restorer, final long[] p_chunkIDRanges) {
        short creator;

        // Sort ChunkIDs by creator
        ArrayListLong chunkIDList;
        HashMap<Short, ArrayListLong> map = new HashMap<>();
        for (int i = 0; i < p_chunkIDRanges.length; i += 2) {
            creator = ChunkID.getCreatorID(p_chunkIDRanges[i]);
            chunkIDList = map.get(creator);
            if (chunkIDList == null) {
                chunkIDList = new ArrayListLong();
                map.put(creator, chunkIDList);
            }
            chunkIDList.add(p_chunkIDRanges[i]);
            chunkIDList.add(p_chunkIDRanges[i + 1]);
        }

        // Iterate over creators, apply changes for creators in assigned range and send changes to
        // responsible superpeers
        Set<Map.Entry<Short, ArrayListLong>> entrySet = map.entrySet();
        for (Map.Entry<Short, ArrayListLong> entry : entrySet) {
            creator = entry.getKey();
            chunkIDList = entry.getValue();

            // Update metadata for assigned (including backups) peers locally
            m_metadata.updateMetadataAfterRecovery(p_rangeID, creator, p_restorer, chunkIDList.getArray());

            // Inform other superpeers
            short[] responsibleSuperpeers = OverlayHelper.getResponsibleSuperpeers(creator, m_superpeers);
            if (responsibleSuperpeers != null) {
                for (short superpeer : responsibleSuperpeers) {
                    try {
                        m_network.sendMessage(
                                new UpdateMetadataAfterRecoveryMessage(superpeer, p_rangeID, creator, p_restorer,
                                        chunkIDList.getArray()));
                    } catch (final NetworkException ignore) {
                    }
                }
            }
        }
    }

    /**
     * Handles a superpeer failure for the superpeer overlay
     *
     * @param p_failedNode
     *         the failed node's NodeID
     * @return whether this superpeer is responsible (successor) for the failed superpeer
     */
    private boolean superpeerFailureHandling(final short p_failedNode) {
        boolean ret = false;
        short[] responsibleArea;
        short[] backupSuperpeers;

        m_overlayLock.writeLock().lock();

        // Inform all other superpeers actively and take over peers if failed superpeer was the predecessor
        if (p_failedNode == m_predecessor) {

            LOGGER.debug("Failed node 0x%X was my predecessor -> informing all other superpeers and taking " +
                    "over all peers", p_failedNode);

            ret = true;

            // Inform all superpeers
            for (short superpeer : m_superpeers) {
                if (superpeer != p_failedNode) {
                    FailureRequest request = new FailureRequest(superpeer, p_failedNode);
                    try {
                        m_network.sendSync(request);
                    } catch (final NetworkException e) {
                        // Ignore, failure is detected by network module
                        continue;
                    }

                    request.getResponse(FailureResponse.class);
                }
            }

            // Take over failed superpeer's peers
            takeOverPeers(m_predecessor);
        }

        // Send failed superpeer's metadata to this superpeers successor if it is the last backup superpeer
        // of the failed superpeer
        responsibleArea = OverlayHelper.getResponsibleArea(m_nodeID, m_predecessor, m_superpeers);
        if (m_superpeers.size() > 3 &&
                OverlayHelper.getResponsibleSuperpeer((short) (responsibleArea[0] + 1), m_superpeers) == p_failedNode) {

            LOGGER.debug("Failed node 0x%X was in my responsible area -> spreading his data", p_failedNode);

            spreadDataOfFailedSuperpeer(responsibleArea);
        }

        // Send this superpeer's metadata to new backup superpeer if failed superpeer was one of its backup
        // superpeers
        backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
        if (m_superpeers.size() > 3 &&
                OverlayHelper.isSuperpeerInRange(p_failedNode, backupSuperpeers[0], backupSuperpeers[2])) {

            LOGGER.debug("Failed node 0x%X was one of my backup nodes -> spreading my data", p_failedNode);

            spreadBackupsOfThisSuperpeer(backupSuperpeers[2]);
        }

        // Remove superpeer
        final int index = OverlayHelper.removeSuperpeer(p_failedNode, m_superpeers);
        if (index >= 0) {
            // Set new predecessor/successor if failed superpeer was pre-/succeeding
            if (p_failedNode == m_successor) {
                if (!m_superpeers.isEmpty()) {
                    if (index < m_superpeers.size()) {
                        m_successor = m_superpeers.get(index);
                    } else {
                        m_successor = m_superpeers.get(0);
                    }
                } else {
                    m_successor = NodeID.INVALID_ID;
                }
            }

            if (p_failedNode == m_predecessor) {
                if (!m_superpeers.isEmpty()) {
                    if (index > 0) {
                        m_predecessor = m_superpeers.get(index - 1);
                    } else {
                        m_predecessor = m_superpeers.get(m_superpeers.size() - 1);
                    }
                } else {
                    m_predecessor = NodeID.INVALID_ID;
                }
            }
        }

        LOGGER.debug("Removed failed node 0x%X", p_failedNode);

        m_overlayLock.writeLock().unlock();

        return ret;
    }

    /**
     * Handles a peer failure for the superpeer overlay
     *
     * @param p_failedNode
     *         the failed node's NodeID
     * @return whether this superpeer is responsible for the failed peer
     */
    private boolean peerFailureHandling(final short p_failedNode) {
        boolean ret = false;
        int counter;
        int numberOfRecoveredChunks;
        BackupRange[] backupRanges;
        BackupPeer[] backupPeers;
        RecoverBackupRangeRequest[] requests;
        RecoverBackupRangeRequest[] processedRequests = null;

        LOGGER.info("Starting failure handling for failed node 0x%X", p_failedNode);

        m_overlayLock.writeLock().lock();

        LOGGER.info("Informing all other peers about failed node 0x%X", p_failedNode);

        // Inform all peers (this is done by all superpeers)
        for (short peer : m_peers) {
            if (peer != p_failedNode && m_boot.getDetails(peer).getRole() == NodeRole.PEER) {

                LOGGER.debug("Informing peer 0x%X about failure of 0x%X", peer, p_failedNode);

                FailureRequest failureRequest = new FailureRequest(peer, p_failedNode);

                try {
                    m_network.sendSync(failureRequest);
                } catch (final NetworkException e) {
                    // Ignore, failure is detected by network module
                }
            }
        }

        if (OverlayHelper.containsPeer(p_failedNode, m_peers)) {
            // Only the responsible superpeer executes this

            ret = true;

            LOGGER.info("Informing all other superpeers about failed node 0x%X", p_failedNode);

            // Inform all superpeers about failed peer
            for (short superpeer : m_superpeers) {

                LOGGER.debug("Informing superpeer 0x%X about failure of 0x%X", superpeer, p_failedNode);

                FailureRequest request = new FailureRequest(superpeer, p_failedNode);
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // Ignore, failure is detected by network module
                    continue;
                }

                request.getResponse(RecoverBackupRangeResponse.class);
            }

            // Remove peer
            OverlayHelper.removePeer(p_failedNode, m_peers);

            // Start recovery
            if (m_backupActive) {

                LOGGER.info("Starting recovery for failed node 0x%X", p_failedNode);

                m_metadata.setState(p_failedNode, PeerState.IN_RECOVERY);

                int waitingTimerPerBackupRange = 3000;

                // Send all recovery requests
                counter = 0;
                RecoverBackupRangeRequest request;
                RecoverBackupRangeResponse response;
                backupRanges = m_metadata.getAllBackupRangesFromLookupTree(p_failedNode);
                short[] numberOfRangesPerPeer = new short[65535];

                m_overlayLock.writeLock().unlock();

                if (backupRanges != null) {
                    requests = new RecoverBackupRangeRequest[backupRanges.length];
                    processedRequests = new RecoverBackupRangeRequest[backupRanges.length];
                    for (BackupRange backupRange : backupRanges) {
                        backupPeers = backupRange.getBackupPeers();

                        for (BackupPeer backupPeer : backupPeers) {
                            if (backupPeer != null) {

                                LOGGER.info("Initiating recovery of range %s on peer 0x%X", backupRange, backupPeer);

                                request = new RecoverBackupRangeRequest(backupPeer.getNodeID(), p_failedNode,
                                        backupRange);
                                try {
                                    // Do not wait for response to enable parallel recovery
                                    m_network.sendSync(request, false);
                                    requests[counter] = request;
                                    numberOfRangesPerPeer[backupPeer.getNodeID() & 0xFFFF]++;
                                    break;
                                } catch (final NetworkException ignored) {
                                    // Try next backup peer
                                }
                            }
                        }

                        // Increase counter for direct access in next step, even if there is no request
                        // for the last backup range
                        counter++;
                    }

                    // Collect and evaluate responses
                    long timeStart = System.currentTimeMillis();
                    boolean finished = false;
                    while (!finished) {
                        finished = true;
                        for (int i = 0; i < requests.length; i++) {
                            RecoverBackupRangeRequest currentRequest = requests[i];
                            if (currentRequest != null) {
                                response = currentRequest.getResponse(RecoverBackupRangeResponse.class);
                                if (response != null) {
                                    long[] chunkIDRanges = response.getChunkIDRanges();
                                    if (chunkIDRanges != null) {

                                        LOGGER.info("Recovered %d chunks of range %s", response.getNumberOfChunks(),
                                                backupRanges[i]);

                                        // Update metadata in superpeer overlay
                                        updateMetadata(currentRequest.getBackupRange().getRangeID(),
                                                response.getSource(), chunkIDRanges);
                                    }
                                    requests[i] = null;
                                    processedRequests[i] = currentRequest;
                                    currentRequest.setBackupRange(response.getNewBackupRange());
                                } else {
                                    if (System.currentTimeMillis() >
                                            numberOfRangesPerPeer[currentRequest.getDestination() & 0xFFFF] *
                                                    waitingTimerPerBackupRange + timeStart) {

                                        LOGGER.info("Backup peer 0x%X is not responding! Trying next backup peer " +
                                                        "for %s (sync).", currentRequest.getDestination(),
                                                currentRequest.getBackupRange());

                                        // Try again with other backup peer and wait for response
                                        numberOfRecoveredChunks = 0;
                                        backupPeers = backupRanges[i].getBackupPeers();
                                        for (BackupPeer backupPeer : backupPeers) {
                                            if (backupPeer != null &&
                                                    backupPeer.getNodeID() != currentRequest.getDestination()) {

                                                LOGGER.info("Initiating recovery of range %s on peer 0x%X",
                                                        backupRanges[i], backupPeer);

                                                request = new RecoverBackupRangeRequest(backupPeer.getNodeID(),
                                                        p_failedNode, backupRanges[i]);
                                                try {
                                                    m_network.sendSync(request, waitingTimerPerBackupRange);

                                                    response = request.getResponse(RecoverBackupRangeResponse.class);
                                                    long[] chunkIDRanges = response.getChunkIDRanges();
                                                    numberOfRecoveredChunks = response.getNumberOfChunks();
                                                    if (numberOfRecoveredChunks > 0) {

                                                        LOGGER.info("Recovered %d chunks of range %s",
                                                                numberOfRecoveredChunks, backupRanges[i]);

                                                        // Update metadata in superpeer overlay
                                                        updateMetadata(currentRequest.getBackupRange().getRangeID(),
                                                                response.getSource(), chunkIDRanges);

                                                        requests[i] = null;
                                                        processedRequests[i] = currentRequest;
                                                        currentRequest.setBackupRange(response.getNewBackupRange());
                                                        break;
                                                    }
                                                } catch (final NetworkException ignored) {
                                                    // Try next backup peer
                                                }
                                            }
                                        }

                                        if (numberOfRecoveredChunks == 0) {

                                            LOGGER.info("Range %s could not be recovered!", backupRanges[i]);

                                        }
                                    } else {
                                        finished = false;
                                    }
                                }
                            }
                        }
                        Thread.yield();
                    }
                }

                m_metadata.setState(p_failedNode, PeerState.RECOVERED);

                LOGGER.info("Recovery of failed node 0x%X complete", p_failedNode);

                LOGGER.info("Starting replication for recovered backup ranges of failed node 0x%X", p_failedNode);

                waitingTimerPerBackupRange = 5000;

                if (backupRanges != null) {
                    ReplicateBackupRangeRequest[] replicationRequests =
                            new ReplicateBackupRangeRequest[backupRanges.length];

                    counter = 0;
                    for (RecoverBackupRangeRequest processedRequest : processedRequests) {
                        if (processedRequest != null) {

                            LOGGER.info("Initiating replication on peer 0x%X (is now range %s)",
                                    processedRequest.getDestination(), processedRequest);

                            ReplicateBackupRangeRequest replicationRequest =
                                    new ReplicateBackupRangeRequest(processedRequest.getDestination(),
                                            processedRequest.getBackupRange().getRangeID());
                            try {
                                // Do not wait for response to enable parallel recovery
                                m_network.sendSync(replicationRequest, false);
                                replicationRequests[counter++] = replicationRequest;
                            } catch (final NetworkException ignored) {
                                // Try next backup peer
                            }
                        }
                    }

                    long timeStart = System.currentTimeMillis();
                    boolean finished = false;
                    while (!finished) {
                        finished = true;
                        for (int i = 0; i < replicationRequests.length; i++) {
                            ReplicateBackupRangeRequest currentRequest = replicationRequests[i];
                            if (currentRequest != null) {
                                ReplicateBackupRangeResponse replicationResponse =
                                        currentRequest.getResponse(ReplicateBackupRangeResponse.class);
                                if (replicationResponse != null) {

                                    LOGGER.info("Finished replication of backup range %d on peer 0x%X",
                                            currentRequest.getRangeID(), currentRequest.getDestination());

                                    replicationRequests[i] = null;
                                } else {
                                    if (System.currentTimeMillis() >
                                            numberOfRangesPerPeer[currentRequest.getDestination() & 0xFFFF] *
                                                    waitingTimerPerBackupRange + timeStart) {

                                        LOGGER.error("Replication of backup range %d on peer 0x%X failed. " +
                                                        "Reliability might be compromised!",
                                                currentRequest.getRangeID(),
                                                currentRequest.getDestination());

                                        replicationRequests[i] = null;
                                    } else {
                                        finished = false;
                                    }
                                }
                            }
                        }
                        Thread.yield();
                    }
                }

                LOGGER.info("Replication of failed node 0x%X complete", p_failedNode);

            } else {
                m_metadata.setState(p_failedNode, PeerState.LOST);
                m_overlayLock.writeLock().unlock();
            }
        } else {
            m_overlayLock.writeLock().unlock();
        }

        return ret;
    }

    /**
     * Adds given NodeID to the list of assigned peers
     *
     * @param p_nodeID
     *         the NodeID
     * @lock overlay lock must be write-locked
     */
    private void addToAssignedPeers(final short p_nodeID) {
        int index;
        index = Collections.binarySearch(m_assignedPeersIncludingBackups, p_nodeID);
        if (index < 0) {
            index = index * -1 - 1;
            m_assignedPeersIncludingBackups.add(index, p_nodeID);
        }
    }

    /**
     * Removes given NodeID from the list of assigned peers
     *
     * @param p_nodeID
     *         the NodeID
     * @lock overlay lock must be write-locked
     */
    private void removeFromAssignedPeers(final short p_nodeID) {
        m_assignedPeersIncludingBackups.remove(new Short(p_nodeID));
    }

    /**
     * Joins the superpeer overlay through contactSuperpeer
     *
     * @param p_contactSuperpeer
     *         NodeID of a known superpeer
     * @param p_sleepInterval
     *         the ping interval in ms
     * @return whether the joining was successful
     * @lock no need for acquiring overlay lock in this method
     */
    private boolean createOrJoinSuperpeerOverlay(final short p_contactSuperpeer, final int p_sleepInterval) {
        short contactSuperpeer;
        JoinRequest joinRequest;
        JoinResponse joinResponse = null;
        short[] newPeers;

        LOGGER.trace("Entering createOrJoinSuperpeerOverlay with: p_contactSuperpeer=0x%X", p_contactSuperpeer);

        contactSuperpeer = p_contactSuperpeer;

        if (p_contactSuperpeer == NodeID.INVALID_ID) {

            LOGGER.error("Cannot join superpeer overlay, no bootstrap superpeer available to contact");

            return false;
        }

        if (m_nodeID == contactSuperpeer) {
            if (m_boot.getDetails().getRole() == NodeRole.SUPERPEER) {

                LOGGER.trace("Setting up new ring, I am 0x%X", m_nodeID);

                setSuccessor(m_nodeID);
            } else {

                LOGGER.error("Bootstrap has to be a superpeer, exiting now");

                return false;
            }
        } else {
            while (contactSuperpeer != NodeID.INVALID_ID) {
                LOGGER.debug("Contacting 0x%X to join the ring, I am 0x%X", contactSuperpeer, m_nodeID);

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

            m_peers = joinResponse.getPeers();

            newPeers = m_metadata.storeMetadata(joinResponse.getMetadata());
            if (newPeers != null) {
                for (short peer : newPeers) {
                    addToAssignedPeers(peer);
                }
            }

            setSuccessor(joinResponse.getSuccessor());
            setPredecessor(joinResponse.getPredecessor());
        }

        LOGGER.trace("Starting stabilization thread");

        m_stabilizationThread =
                new SuperpeerStabilizationThread(this, m_nodeID, m_overlayLock, m_initialNumberOfSuperpeers,
                        m_superpeers, p_sleepInterval, m_network);
        m_stabilizationThread.setDaemon(true);
        m_stabilizationThread.start();

        // Inform all peers and superpeers about joining
        m_overlayLock.readLock().lock();
        // Inform all superpeers
        NodeRegistry.NodeDetails details = m_boot.getDetails();
        for (short superpeer : m_superpeers) {
            InetSocketAddress socketAddress = details.getAddress();
            NodeJoinEventMessage message =
                    new NodeJoinEventMessage(superpeer, m_nodeID, NodeRole.SUPERPEER, NodeCapabilities.NONE,
                            details.getRack(), details.getSwitch(), false,
                            new IPV4Unit(socketAddress.getHostName(), socketAddress.getPort()));
            try {
                m_network.sendMessage(message);
            } catch (final NetworkException e) {
                // Ignore, failure is detected by network module
                continue;
            }
        }

        // Inform own peers
        for (short peer : m_peers) {
            InetSocketAddress socketAddress = details.getAddress();
            NodeJoinEventMessage message =
                    new NodeJoinEventMessage(peer, m_nodeID, NodeRole.SUPERPEER, NodeCapabilities.NONE,
                            details.getRack(), details.getSwitch(), false,
                            new IPV4Unit(socketAddress.getHostName(), socketAddress.getPort()));
            try {
                m_network.sendMessage(message);
            } catch (final NetworkException e) {
                // Ignore, failure is detected by network module
                continue;
            }
        }
        m_overlayLock.readLock().unlock();

        LOGGER.trace("Exiting createOrJoinSuperpeerOverlay");

        return true;
    }

    /**
     * Spread data of failed superpeer
     *
     * @param p_responsibleArea
     *         the responsible area
     * @lock overlay lock must be read-locked
     */
    private void spreadDataOfFailedSuperpeer(final short[] p_responsibleArea) {
        byte[] metadata;

        metadata = m_metadata.receiveMetadataInRange(p_responsibleArea[0], p_responsibleArea[1]);

        while (!m_superpeers.isEmpty()) {

            LOGGER.debug("Spreading superpeer's meta-data to 0x%X", m_successor);

            try {
                m_network.sendMessage(new SendBackupsMessage(m_successor, metadata));
            } catch (final NetworkException e) {
                // Successor is not available anymore, remove from superpeer array and try next superpeer

                LOGGER.error("Successor failed, too");

                continue;
            }
            break;
        }
    }

    /**
     * Spread backups of failed superpeer
     *
     * @param p_lastBackupSuperpeer
     *         the last backup superpeer
     * @lock overlay lock must be read-locked
     */
    private void spreadBackupsOfThisSuperpeer(final short p_lastBackupSuperpeer) {
        short newBackupSuperpeer;
        int index;
        boolean superpeerToSendData = false;
        byte[] metadata;
        String str = "Spreaded data of ";

        metadata = m_metadata.receiveMetadataInRange(m_predecessor, m_nodeID);

        while (!m_superpeers.isEmpty()) {
            // Determine successor of last backup superpeer
            index = (short) Collections.binarySearch(m_superpeers, (short) (p_lastBackupSuperpeer + 1));
            if (index < 0) {
                index = index * -1 - 1;
                if (index == m_superpeers.size()) {
                    index = 0;
                }
            }
            newBackupSuperpeer = m_superpeers.get(index);

            superpeerToSendData = true;
            str += " to " + NodeID.toHexString(newBackupSuperpeer);

            try {
                m_network.sendMessage(new SendBackupsMessage(newBackupSuperpeer, metadata));
            } catch (final NetworkException e) {
                // Superpeer is not available anymore, remove from superpeer array and try next superpeer

                LOGGER.error("new backup superpeer (0x%X) failed, too", newBackupSuperpeer);

                continue;
            }
            break;
        }

        if (metadata.length > 0 && superpeerToSendData) {
            LOGGER.debug(str);
        } else {
            LOGGER.debug("No need to spread data");
        }

    }

    /**
     * Handles an incoming JoinRequest
     *
     * @param p_joinRequest
     *         the JoinRequest
     */
    private void incomingJoinRequest(final JoinRequest p_joinRequest) {
        short joiningNode;
        short currentPeer;
        Iterator<Short> iter;
        ArrayList<Short> peers;

        byte[] metadata;
        short joiningNodesPredecessor;
        short superpeer;
        short[] responsibleArea;

        boolean newNodeisSuperpeer;

        LOGGER.debug("Received JoinRequest from 0x%X", p_joinRequest.getSource());

        joiningNode = p_joinRequest.getNodeId();
        newNodeisSuperpeer = p_joinRequest.isSuperPeer();

        if (newNodeisSuperpeer) {
            if (OverlayHelper.isSuperpeerInRange(joiningNode, m_predecessor, m_nodeID)) {
                m_overlayLock.writeLock().lock();
                // Send the joining node not only the successor, but the predecessor, superpeers
                // and all metadata
                if (m_superpeers.isEmpty()) {
                    joiningNodesPredecessor = m_nodeID;
                } else {
                    joiningNodesPredecessor = m_predecessor;
                }

                iter = m_peers.iterator();
                peers = new ArrayList<>();
                while (iter.hasNext()) {
                    currentPeer = iter.next();
                    if (OverlayHelper.isPeerInSuperpeerRange(currentPeer, joiningNodesPredecessor, joiningNode)) {
                        peers.add(currentPeer);
                    }
                }

                responsibleArea = OverlayHelper.getResponsibleArea(joiningNode, m_predecessor, m_superpeers);
                metadata = m_metadata.receiveMetadataInRange(responsibleArea[0], responsibleArea[1]);

                try {
                    m_network.sendMessage(
                            new JoinResponse(p_joinRequest, NodeID.INVALID_ID, joiningNodesPredecessor, m_nodeID,
                                    m_superpeers, peers, null, metadata));
                } catch (final NetworkException e) {
                    // Joining node is not available anymore -> ignore request and return directly
                    return;
                }

                for (Short peer : peers) {
                    OverlayHelper.removePeer(peer, m_peers);
                }

                // Notify predecessor about the joining node
                if (m_superpeers.isEmpty()) {
                    setSuccessor(joiningNode);
                    setPredecessor(joiningNode);
                } else {
                    setPredecessor(joiningNode);

                    try {
                        m_network.sendMessage(
                                new NotifyAboutNewSuccessorMessage(joiningNodesPredecessor, m_predecessor));
                    } catch (final NetworkException e) {
                        // Old predecessor is not available anymore, ignore it
                    }
                }
                m_overlayLock.writeLock().unlock();
            } else {
                m_overlayLock.readLock().lock();
                superpeer = OverlayHelper.getResponsibleSuperpeer(joiningNode, m_superpeers);
                m_overlayLock.readLock().unlock();

                try {
                    m_network.sendMessage(
                            new JoinResponse(p_joinRequest, superpeer, NodeID.INVALID_ID, NodeID.INVALID_ID, null, null,
                                    null, null));
                } catch (final NetworkException e) {
                    // Joining node is not available anymore, ignore request
                }
            }
        } else {
            if (OverlayHelper.isPeerInSuperpeerRange(joiningNode, m_predecessor, m_nodeID)) {
                m_overlayLock.writeLock().lock();
                OverlayHelper.insertPeer(joiningNode, m_peers);
                addToAssignedPeers(joiningNode);
                // Lock downgrade
                m_overlayLock.readLock().lock();
                m_overlayLock.writeLock().unlock();
                try {
                    m_network.sendMessage(
                            new JoinResponse(p_joinRequest, NodeID.INVALID_ID, NodeID.INVALID_ID, NodeID.INVALID_ID,
                                    m_superpeers, null, null, null));
                } catch (final NetworkException e) {
                    // Joining node is not available anymore, ignore request
                }
                m_overlayLock.readLock().unlock();
            } else {
                m_overlayLock.readLock().lock();
                superpeer = OverlayHelper.getResponsibleSuperpeer(joiningNode, m_superpeers);
                m_overlayLock.readLock().unlock();
                try {
                    m_network.sendMessage(
                            new JoinResponse(p_joinRequest, superpeer, NodeID.INVALID_ID, NodeID.INVALID_ID, null, null,
                                    null, null));
                } catch (final NetworkException e) {
                    // Joining node is not available anymore, ignore request
                }
            }
        }
    }

    /**
     * Handles an incoming FinishedStartupMessage
     *
     * @param p_finishedStartupMessage
     *         the FinishedStartupMessage
     */
    private void incomingFinishedStartupMessage(final FinishedStartupMessage p_finishedStartupMessage) {
        short newPeer;

        newPeer = p_finishedStartupMessage.getSource();

        // Outsource informing other superpeers/peers to another thread to avoid blocking a message handler
        Runnable task = () -> {
            m_overlayLock.readLock().lock();
            // Inform all superpeers
            for (short superpeer : m_superpeers) {
                NodeJoinEventMessage message = new NodeJoinEventMessage(superpeer, newPeer, NodeRole.PEER,
                        p_finishedStartupMessage.getCapabilities(), p_finishedStartupMessage.getRack(),
                        p_finishedStartupMessage.getSwitch(), p_finishedStartupMessage.isAvailableForBackup(),
                        p_finishedStartupMessage.getAddress());
                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {
                    // Ignore, failure is detected by network module
                    continue;
                }
            }

            // Inform own peers
            for (short peer : m_peers) {
                if (peer != newPeer) {
                    NodeJoinEventMessage message = new NodeJoinEventMessage(peer, newPeer, NodeRole.PEER,
                            p_finishedStartupMessage.getCapabilities(), p_finishedStartupMessage.getRack(),
                            p_finishedStartupMessage.getSwitch(), p_finishedStartupMessage.isAvailableForBackup(),
                            p_finishedStartupMessage.getAddress());
                    try {
                        m_network.sendMessage(message);
                    } catch (final NetworkException e) {
                        // Ignore, failure is detected by network module
                        continue;
                    }
                }
            }
            m_overlayLock.readLock().unlock();
        };

        new Thread(task).start();

        // Notify other components/services
        m_event.fireEvent(
                new NodeJoinEvent(getClass().getSimpleName(), p_finishedStartupMessage.getSource(), NodeRole.PEER,
                        p_finishedStartupMessage.getCapabilities(), p_finishedStartupMessage.getRack(),
                        p_finishedStartupMessage.getSwitch(), p_finishedStartupMessage.isAvailableForBackup(),
                        p_finishedStartupMessage.getAddress()));
    }

    /**
     * Handles an incoming GetLookupRangeRequest
     *
     * @param p_getLookupRangeRequest
     *         the GetLookupRangeRequest
     */
    private void incomingGetLookupRangeRequest(final GetLookupRangeRequest p_getLookupRangeRequest) {
        long chunkID;
        LookupRange result;

        chunkID = p_getLookupRangeRequest.getChunkID();

        LOGGER.trace("Got request: GET_LOOKUP_RANGE_REQUEST 0x%X chunkID: 0x%X", p_getLookupRangeRequest.getSource(),
                chunkID);

        result = m_metadata.getLookupRangeFromLookupTree(chunkID, m_backupActive);

        LOGGER.trace("GET_LOOKUP_RANGE_REQUEST 0x%X chunkID 0x%X reply location: %s",
                p_getLookupRangeRequest.getSource(), chunkID, result);

        try {
            m_network.sendMessage(new GetLookupRangeResponse(p_getLookupRangeRequest, result));
        } catch (final NetworkException e) {
            // Requesting peer is not available anymore, ignore it
        }
    }

    /**
     * Handles an incoming RemoveChunkIDsRequest
     *
     * @param p_removeChunkIDsRequest
     *         the RemoveChunkIDsRequest
     */
    private void incomingRemoveChunkIDsRequest(final RemoveChunkIDsRequest p_removeChunkIDsRequest) {
        long[] chunkIDs;
        short creator;
        short[] backupSuperpeers;
        boolean isBackup;

        LOGGER.trace("Got Message: REMOVE_CHUNKIDS_REQUEST from 0x%X", p_removeChunkIDsRequest.getSource());

        chunkIDs = p_removeChunkIDsRequest.getChunkIDs();
        isBackup = p_removeChunkIDsRequest.isBackup();

        if (chunkIDs.length == 0) {
            try {
                m_network.sendMessage(
                        new RemoveChunkIDsResponse(p_removeChunkIDsRequest, new short[] {NodeID.INVALID_ID}));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore it
            }

            return;
        }

        creator = ChunkID.getCreatorID(chunkIDs[0]);
        if (OverlayHelper.isPeerInSuperpeerRange(creator, m_predecessor, m_nodeID)) {
            if (m_metadata.removeChunkIDsFromLookupTree(m_backupActive, chunkIDs)) {
                m_overlayLock.readLock().lock();
                backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
                m_overlayLock.readLock().unlock();
                try {
                    m_network.sendMessage(new RemoveChunkIDsResponse(p_removeChunkIDsRequest, backupSuperpeers));
                } catch (final NetworkException e) {
                    // Requesting peer is not available anymore, ignore it
                }
            } else {

                LOGGER.error("CIDTree range not initialized on responsible superpeer 0x%X", m_nodeID);

                try {
                    m_network.sendMessage(
                            new RemoveChunkIDsResponse(p_removeChunkIDsRequest, new short[] {NodeID.INVALID_ID}));
                } catch (final NetworkException e) {
                    // Requesting peer is not available anymore, ignore it
                }
            }
        } else if (isBackup) {
            if (!m_metadata.removeChunkIDsFromLookupTree(m_backupActive, chunkIDs)) {

                LOGGER.warn("CIDTree range not initialized on backup superpeer 0x%X", m_nodeID);

            }

            try {
                m_network.sendMessage(new RemoveChunkIDsResponse(p_removeChunkIDsRequest, null));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore it
            }
        } else {
            // Not responsible for requesting peer
            try {
                m_network.sendMessage(new RemoveChunkIDsResponse(p_removeChunkIDsRequest, null));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore it
            }
        }
    }

    /**
     * Handles an incoming InsertIDRequest
     *
     * @param p_insertIDRequest
     *         the InsertIDRequest
     */
    private void incomingInsertNameserviceEntriesRequest(final InsertNameserviceEntriesRequest p_insertIDRequest) {
        int id;
        short[] backupSuperpeers;

        id = p_insertIDRequest.getID();

        LOGGER.trace("Got request: INSERT_ID_REQUEST from 0x%X, id %d", p_insertIDRequest.getSource(), id);

        m_overlayLock.readLock().lock();
        if (OverlayHelper.isHashInSuperpeerRange(CRC16.hash(id), m_predecessor, m_nodeID)) {
            m_metadata.putNameserviceEntry(id, p_insertIDRequest.getChunkID());

            backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
            try {
                m_network.sendMessage(new InsertNameserviceEntriesResponse(p_insertIDRequest, backupSuperpeers));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore it
            }

            ArrayList<Short> peers = m_peers;
            // notify peers about this to update caches
            for (short peer : peers) {
                NameserviceUpdatePeerCachesMessage message =
                        new NameserviceUpdatePeerCachesMessage(peer, id, p_insertIDRequest.getChunkID());
                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {
                    // peer is not available anymore, ignore it
                }
            }
        } else if (p_insertIDRequest.isBackup()) {
            m_metadata.putNameserviceEntry(id, p_insertIDRequest.getChunkID());

            try {
                m_network.sendMessage(new InsertNameserviceEntriesResponse(p_insertIDRequest, null));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore it
            }
        } else {
            // Not responsible for that chunk
            try {
                m_network.sendMessage(new InsertNameserviceEntriesResponse(p_insertIDRequest, null));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore it
            }
        }
        m_overlayLock.readLock().unlock();
    }

    /**
     * Handles an incoming GetChunkIDForNameserviceEntryRequest
     *
     * @param p_getChunkIDForNameserviceEntryRequest
     *         the GetChunkIDForNameserviceEntryRequest
     */
    private void incomingGetChunkIDForNameserviceEntryRequest(
            final GetChunkIDForNameserviceEntryRequest p_getChunkIDForNameserviceEntryRequest) {
        int id;
        long chunkID = ChunkID.INVALID_ID;

        id = p_getChunkIDForNameserviceEntryRequest.getID();

        LOGGER.trace("Got request: GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST from 0x%X, id %d",
                p_getChunkIDForNameserviceEntryRequest.getSource(), id);

        if (OverlayHelper.isHashInSuperpeerRange(CRC16.hash(id), m_predecessor, m_nodeID)) {
            chunkID = m_metadata.getNameserviceEntry(id);

            LOGGER.trace("GET_CHUNKID_REQUEST from 0x%X, id %d, reply chunkID 0x%X",
                    p_getChunkIDForNameserviceEntryRequest.getSource(), id, chunkID);

        }

        try {
            m_network.sendMessage(
                    new GetChunkIDForNameserviceEntryResponse(p_getChunkIDForNameserviceEntryRequest, chunkID));
        } catch (final NetworkException e) {
            // Requesting peer is not available anymore, ignore it
        }
    }

    /**
     * Handles an incoming GetNameserviceEntryCountRequest
     *
     * @param p_getNameserviceEntryCountRequest
     *         the GetNameserviceEntryCountRequest
     */
    private void incomingGetNameserviceEntryCountRequest(
            final GetNameserviceEntryCountRequest p_getNameserviceEntryCountRequest) {

        LOGGER.trace("Got request: GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST from 0x%X",
                p_getNameserviceEntryCountRequest.getSource());

        try {
            m_network.sendMessage(new GetNameserviceEntryCountResponse(p_getNameserviceEntryCountRequest,
                    m_metadata.countNameserviceEntries(m_predecessor, m_nodeID)));
        } catch (final NetworkException e) {
            // Requesting peer is not available anymore, ignore it
        }
    }

    /**
     * Handles an incoming GetNameserviceEntriesRequest
     *
     * @param p_getNameserviceEntriesRequest
     *         the GetNameserviceEntriesRequest
     */
    private void incomingGetNameserviceEntriesRequest(
            final GetNameserviceEntriesRequest p_getNameserviceEntriesRequest) {

        LOGGER.trace("Got request: GET_NAMESERVICE_ENTRIES from 0x%X", p_getNameserviceEntriesRequest.getSource());

        try {
            m_network.sendMessage(new GetNameserviceEntriesResponse(p_getNameserviceEntriesRequest,
                    m_metadata.getAllNameserviceEntries(m_predecessor, m_nodeID)));
        } catch (final NetworkException e) {
            // Requesting peer is not available anymore, ignore it
        }
    }

    /**
     * Handles an incoming MigrateRequest
     *
     * @param p_migrateRequest
     *         the MigrateRequest
     */
    private void incomingMigrateRequest(final MigrateRequest p_migrateRequest) {
        short nodeID;
        long chunkID;
        short creator;
        short[] backupSuperpeers;
        boolean isBackup;

        LOGGER.trace("Got Message: MIGRATE_REQUEST from 0x%X", p_migrateRequest.getSource());

        nodeID = p_migrateRequest.getNodeID();
        chunkID = p_migrateRequest.getChunkID();
        creator = ChunkID.getCreatorID(chunkID);
        isBackup = p_migrateRequest.isBackup();

        m_overlayLock.readLock().lock();
        if (OverlayHelper.isPeerInSuperpeerRange(creator, m_predecessor, m_nodeID)) {
            if (m_metadata.putChunkIDInLookupTree(chunkID, nodeID, m_backupActive)) {
                backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
                m_overlayLock.readLock().unlock();
                if (backupSuperpeers[0] != NodeID.INVALID_ID) {
                    // Outsource informing backups to another thread to avoid blocking a message handler
                    Runnable task = () -> {
                        // Send backups
                        for (short backupSuperpeer : backupSuperpeers) {
                            MigrateRequest request = new MigrateRequest(backupSuperpeer, chunkID, nodeID, true);

                            try {
                                m_network.sendSync(request);
                            } catch (final NetworkException e) {
                                // Ignore superpeer failure, superpeer will fix this later
                            }
                        }
                    };
                    new Thread(task).start();
                }

                try {
                    m_network.sendMessage(new MigrateResponse(p_migrateRequest, true));
                } catch (final NetworkException e) {
                    // Requesting peer is not available anymore, ignore it
                }
            } else {
                m_overlayLock.readLock().unlock();

                LOGGER.error("CIDTree range not initialized on responsible superpeer 0x%X", m_nodeID);

                try {
                    m_network.sendMessage(new MigrateResponse(p_migrateRequest, false));
                } catch (final NetworkException e) {
                    // Requesting peer is not available anymore, ignore request it
                }
            }
        } else if (isBackup) {
            if (!m_metadata.putChunkIDInLookupTree(chunkID, nodeID, m_backupActive)) {

                LOGGER.warn("CIDTree range not initialized on backup superpeer 0x%X", m_nodeID);

            }
            m_overlayLock.readLock().unlock();

            try {
                m_network.sendMessage(new MigrateResponse(p_migrateRequest, true));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore it
            }
        } else {
            m_overlayLock.readLock().unlock();
            // Not responsible for requesting peer
            try {
                m_network.sendMessage(new MigrateResponse(p_migrateRequest, false));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore it
            }
        }
    }

    /**
     * Handles an incoming MigrateRangeRequest
     *
     * @param p_migrateRangeRequest
     *         the MigrateRangeRequest
     */

    private void incomingMigrateRangeRequest(final MigrateRangeRequest p_migrateRangeRequest) {
        short nodeID;
        long startChunkID;
        long endChunkID;
        short creator;
        short[] backupSuperpeers;
        boolean isBackup;

        LOGGER.trace("Got Message: MIGRATE_RANGE_REQUEST from 0x%X", p_migrateRangeRequest.getSource());

        nodeID = p_migrateRangeRequest.getNodeID();
        startChunkID = p_migrateRangeRequest.getStartChunkID();
        endChunkID = p_migrateRangeRequest.getEndChunkID();
        creator = ChunkID.getCreatorID(startChunkID);
        isBackup = p_migrateRangeRequest.isBackup();

        if (creator != ChunkID.getCreatorID(endChunkID)) {

            LOGGER.error("Start and end objects creators not equal");

            return;
        }

        m_overlayLock.readLock().lock();
        if (OverlayHelper.isPeerInSuperpeerRange(creator, m_predecessor, m_nodeID)) {
            if (m_metadata.putChunkIDRangeInLookupTree(startChunkID, endChunkID, nodeID, m_backupActive)) {
                backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
                m_overlayLock.readLock().unlock();
                if (backupSuperpeers[0] != NodeID.INVALID_ID) {
                    // Outsource informing backups to another thread to avoid blocking a message handler
                    Runnable task = () -> {
                        // Send backups
                        for (short backupSuperpeer : backupSuperpeers) {
                            MigrateRangeRequest request =
                                    new MigrateRangeRequest(backupSuperpeer, startChunkID, endChunkID, nodeID, true);

                            try {
                                m_network.sendSync(request);
                            } catch (final NetworkException e) {
                                // Ignore superpeer failure, superpeer will fix this later
                            }
                        }
                    };
                    new Thread(task).start();
                }

                try {
                    m_network.sendMessage(new MigrateRangeResponse(p_migrateRangeRequest, true));
                } catch (final NetworkException e) {
                    // Requesting peer is not available anymore, ignore it
                }
            } else {
                m_overlayLock.readLock().unlock();

                LOGGER.error("CIDTree range not initialized on responsible superpeer 0x%X", m_nodeID);

                try {
                    m_network.sendMessage(new MigrateRangeResponse(p_migrateRangeRequest, false));
                } catch (final NetworkException e) {
                    // Requesting peer is not available anymore, ignore it
                }
            }
        } else if (isBackup) {
            if (!m_metadata.putChunkIDRangeInLookupTree(startChunkID, endChunkID, nodeID, m_backupActive)) {

                LOGGER.warn("CIDTree range not initialized on backup superpeer 0x%X", m_nodeID);

            }
            m_overlayLock.readLock().unlock();

            try {
                m_network.sendMessage(new MigrateRangeResponse(p_migrateRangeRequest, true));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore it
            }
        } else {
            m_overlayLock.readLock().unlock();
            // Not responsible for requesting peer

            try {
                m_network.sendMessage(new MigrateRangeResponse(p_migrateRangeRequest, false));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore request it
            }
        }
    }

    /**
     * Handles an incoming InitRangeRequest
     *
     * @param p_initRangeRequest
     *         the InitRangeRequest
     */

    private void incomingInitRangeRequest(final InitRangeRequest p_initRangeRequest) {
        short rangeOwner;
        short[] backupSuperpeers;
        boolean isBackup;
        BackupRange backupRange;

        LOGGER.trace("Got Message: INIT_RANGE_REQUEST from 0x%X", p_initRangeRequest.getSource());

        rangeOwner = p_initRangeRequest.getBackupRangeOwner();
        backupRange = p_initRangeRequest.getBackupRange();
        isBackup = p_initRangeRequest.isBackup();

        m_overlayLock.writeLock().lock();
        if (OverlayHelper.isPeerInSuperpeerRange(rangeOwner, m_predecessor, m_nodeID)) {
            if (m_metadata.initBackupRangeInLookupTree(rangeOwner, backupRange)) {
                addToAssignedPeers(rangeOwner);
            }

            backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
            m_overlayLock.writeLock().unlock();
            if (backupSuperpeers[0] != NodeID.INVALID_ID) {
                // Outsource informing backups to another thread to avoid blocking a message handler
                Runnable task = () -> {
                    // Send backups
                    for (short backupSuperpeer : backupSuperpeers) {
                        InitRangeRequest request = new InitRangeRequest(backupSuperpeer, rangeOwner, backupRange, true);

                        try {
                            m_network.sendSync(request);
                        } catch (final NetworkException e) {
                            // Ignore superpeer failure, superpeer will fix this later
                        }
                    }
                };
                new Thread(task).start();
            }

            try {
                m_network.sendMessage(new InitRangeResponse(p_initRangeRequest, true));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore it
            }
        } else if (isBackup) {
            if (m_metadata.initBackupRangeInLookupTree(rangeOwner, backupRange)) {
                addToAssignedPeers(rangeOwner);
            }
            m_overlayLock.writeLock().unlock();

            try {
                m_network.sendMessage(new InitRangeResponse(p_initRangeRequest, true));
            } catch (final NetworkException e) {
                // Requesting peer is not available anymore, ignore it
            }
        } else {
            m_overlayLock.writeLock().unlock();
            // Not responsible for requesting peer

            try {
                m_network.sendMessage(new InitRangeResponse(p_initRangeRequest, false));
            } catch (final NetworkException e) {
                // Requesting node is not available anymore, ignore it
            }
        }
    }

    /**
     * Handles an incoming GetAllBackupRangesRequest
     *
     * @param p_getAllBackupRangesRequest
     *         the GetAllBackupRangesRequest
     */

    private void incomingGetAllBackupRangesRequest(final GetAllBackupRangesRequest p_getAllBackupRangesRequest) {
        BackupRange[] result;

        LOGGER.trace("Got request: GET_ALL_BACKUP_RANGES_REQUEST 0x%X", p_getAllBackupRangesRequest.getSource());

        result = m_metadata.getAllBackupRangesFromLookupTree(p_getAllBackupRangesRequest.getNodeID());
        try {
            m_network.sendMessage(new GetAllBackupRangesResponse(p_getAllBackupRangesRequest, result));
        } catch (final NetworkException e) {
            // Requesting peer is not available anymore, ignore it
        }
    }

    /**
     * Handles an incoming UpdateMetadataAfterRecoveryMessage
     *
     * @param p_updateMetadataAfterRecoveryMessage
     *         the UpdateMetadataAfterRecoveryMessage
     */
    private void incomingUpdateMetadataAfterRecoveryMessage(
            final UpdateMetadataAfterRecoveryMessage p_updateMetadataAfterRecoveryMessage) {

        LOGGER.trace("Got request: UPDATE_METADATA_AFTER_RECOVERY_MESSAGE 0x%X",
                p_updateMetadataAfterRecoveryMessage.getSource());

        m_metadata.updateMetadataAfterRecovery(p_updateMetadataAfterRecoveryMessage.getRangeID(),
                p_updateMetadataAfterRecoveryMessage.getCreator(), p_updateMetadataAfterRecoveryMessage.getRestorer(),
                p_updateMetadataAfterRecoveryMessage.getChunkIDRanges());
    }

    /**
     * Handles an incoming NodeJoinEventMessage
     *
     * @param p_peerJoinEventRequest
     *         the NodeJoinEventMessage
     */
    private void incomingPeerJoinEventRequest(final NodeJoinEventMessage p_peerJoinEventRequest) {
        LOGGER.trace("Got request: NodeJoinEventMessage 0x%X", p_peerJoinEventRequest.getSource());

        m_overlayLock.readLock().lock();
        // Inform own peers
        for (short p : m_peers) {
            NodeJoinEventMessage message = new NodeJoinEventMessage(p, p_peerJoinEventRequest.getJoinedPeer(),
                    p_peerJoinEventRequest.getRole(), p_peerJoinEventRequest.getCapabilities(),
                    p_peerJoinEventRequest.getRack(), p_peerJoinEventRequest.getSwitch(),
                    p_peerJoinEventRequest.isAvailableForBackup(), p_peerJoinEventRequest.getAddress());
            try {
                m_network.sendMessage(message);
            } catch (final NetworkException e) {
                // Ignore, failure is detected by network module
                continue;
            }
        }
        m_overlayLock.readLock().unlock();

        // Notify other components/services
        m_event.fireEvent(new NodeJoinEvent(getClass().getSimpleName(), p_peerJoinEventRequest.getJoinedPeer(),
                p_peerJoinEventRequest.getRole(), p_peerJoinEventRequest.getCapabilities(),
                p_peerJoinEventRequest.getRack(), p_peerJoinEventRequest.getSwitch(),
                p_peerJoinEventRequest.isAvailableForBackup(), p_peerJoinEventRequest.getAddress()));
    }

    /**
     * Handles an incoming BarrierAllocRequest
     *
     * @param p_request
     *         the BarrierAllocRequest
     */
    private void incomingBarrierAllocRequest(final BarrierAllocRequest p_request) {
        int barrierId;

        if (!p_request.isReplicate()) {
            barrierId = m_metadata.createBarrier(m_nodeID, p_request.getBarrierSize());
        } else {
            barrierId = m_metadata.createBarrier(p_request.getSource(), p_request.getBarrierSize());
        }

        if (barrierId == BarrierID.INVALID_ID) {
            LOGGER.error("Creating barrier for size %d failed", p_request.getBarrierSize());
        }

        BarrierAllocResponse response = new BarrierAllocResponse(p_request, barrierId);
        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending response to barrier request %s failed: %s", p_request, e);

        }

        if (!p_request.isReplicate()) {
            // Outsource informing backups to another thread to avoid blocking a message handler
            Runnable task = () -> {
                m_overlayLock.readLock().lock();
                short[] backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
                m_overlayLock.readLock().unlock();

                for (short backupSuperpeer : backupSuperpeers) {
                    if (backupSuperpeer == NodeID.INVALID_ID) {
                        continue;
                    }

                    BarrierAllocRequest request =
                            new BarrierAllocRequest(backupSuperpeer, p_request.getBarrierSize(), true);
                    // send as message, only
                    try {
                        m_network.sendMessage(request);
                    } catch (final NetworkException e) {
                        // ignore result
                    }
                }
            };
            new Thread(task).start();
        }
    }

    /**
     * Handles an incoming BarrierFreeRequest
     *
     * @param p_request
     *         the BarrierFreeRequest
     */
    private void incomingBarrierFreeRequest(final BarrierFreeRequest p_request) {
        short creator;

        if (!p_request.isReplicate()) {
            creator = m_nodeID;
        } else {
            creator = p_request.getSource();
        }

        BarrierFreeResponse response;
        if (!m_metadata.removeBarrier(creator, p_request.getBarrierId())) {

            LOGGER.error("Free'ing barrier 0x%X failed", p_request.getBarrierId());

            response = new BarrierFreeResponse(p_request, (byte) -1);
        } else {
            response = new BarrierFreeResponse(p_request, (byte) 0);
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending back response for barrier free message %s failed: %s", p_request, e);

        }

        if (!p_request.isReplicate()) {
            // Outsource informing backups to another thread to avoid blocking a message handler
            Runnable task = () -> {
                m_overlayLock.readLock().lock();
                short[] backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
                m_overlayLock.readLock().unlock();

                for (short backupSuperpeer : backupSuperpeers) {
                    if (backupSuperpeer == NodeID.INVALID_ID) {
                        continue;
                    }

                    BarrierFreeRequest request =
                            new BarrierFreeRequest(backupSuperpeer, p_request.getBarrierId(), true);
                    // send as message, only
                    try {
                        m_network.sendMessage(request);
                    } catch (final NetworkException e) {
                        // ignore result
                    }
                }
            };
            new Thread(task).start();
        }
    }

    /**
     * Handles an incoming BarrierSignOnRequest
     *
     * @param p_request
     *         the BarrierSignOnRequest
     */
    private void incomingBarrierSignOnRequest(final BarrierSignOnRequest p_request) {
        int barrierId = p_request.getBarrierId();
        int res = m_metadata.signOnBarrier(m_nodeID, barrierId, p_request.getSource(), p_request.getCustomData());
        BarrierSignOnResponse response = new BarrierSignOnResponse(p_request, (byte) (res >= 0 ? 0 : -1));
        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending response to sign on request %s failed: %s", p_request, e);

        }

        // release all if this was the last sign on
        if (res == 0) {
            BarrierStatus barrierStatus = m_metadata.getSignOnStatusOfBarrier(m_nodeID, barrierId);

            barrierStatus.forEachSignedOnPeer((p_nodeId, p_customData) -> {
                BarrierReleaseMessage message = new BarrierReleaseMessage(p_nodeId, barrierId, barrierStatus);

                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {

                    LOGGER.error("Releasing peer 0x%X of barrier 0x%X failed: %s", p_nodeId, barrierId, e);

                }
            });

            // Reset for reuse
            m_metadata.resetBarrier(m_nodeID, barrierId);
        }
    }

    /**
     * Handles an incoming BarrierGetStatusRequest
     *
     * @param p_request
     *         the BarrierGetStatusRequest
     */

    private void incomingBarrierGetStatusRequest(final BarrierGetStatusRequest p_request) {
        BarrierStatus barrierStatus = m_metadata.getSignOnStatusOfBarrier(m_nodeID, p_request.getBarrierId());
        BarrierGetStatusResponse response;
        if (barrierStatus == null) {
            // barrier does not exist
            response = new BarrierGetStatusResponse(p_request, new BarrierStatus(), (byte) -1);
        } else {
            response = new BarrierGetStatusResponse(p_request, barrierStatus, (byte) 0);
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending response to status request %s failed: %s", p_request, e);

        }
    }

    /**
     * Handles an incoming BarrierChangeSizeRequest
     *
     * @param p_request
     *         the BarrierChangeSizeRequest
     */
    private void incomingBarrierChangeSizeRequest(final BarrierChangeSizeRequest p_request) {
        short creator;

        if (!p_request.isReplicate()) {
            creator = m_nodeID;
        } else {
            creator = p_request.getSource();
        }

        BarrierChangeSizeResponse response;
        if (!m_metadata.changeSizeOfBarrier(creator, p_request.getBarrierId(), p_request.getBarrierSize())) {
            response = new BarrierChangeSizeResponse(p_request, (byte) -1);
        } else {
            response = new BarrierChangeSizeResponse(p_request, (byte) 0);
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending response for barrier change size request %s failed: %s", p_request, e);

        }

        if (!p_request.isReplicate()) {
            // Outsource informing backups to another thread to avoid blocking a message handler
            Runnable task = () -> {
                m_overlayLock.readLock().lock();
                short[] backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
                m_overlayLock.readLock().unlock();

                for (short backupSuperpeer : backupSuperpeers) {
                    if (backupSuperpeer == NodeID.INVALID_ID) {
                        continue;
                    }

                    BarrierChangeSizeRequest request =
                            new BarrierChangeSizeRequest(backupSuperpeer, p_request.getBarrierId(),
                                    p_request.getBarrierSize(), true);
                    // send as message, only
                    try {
                        m_network.sendMessage(request);
                    } catch (final NetworkException e) {
                        // ignore result
                    }
                }
            };
            new Thread(task).start();
        }
    }

    /**
     * Handles an incoming SuperpeerStorageCreateRequest
     *
     * @param p_request
     *         the SuperpeerStorageCreateRequest
     */
    private void incomingSuperpeerStorageCreateRequest(final SuperpeerStorageCreateRequest p_request) {
        int ret = m_metadata.createStorage(p_request.getStorageId(), p_request.getSize());

        if (!p_request.isReplicate()) {
            SuperpeerStorageCreateResponse response = new SuperpeerStorageCreateResponse(p_request, (byte) ret);
            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {

                LOGGER.error("Sending response to storage create with size %d failed: %s", p_request.getSize(), e);

            }
        }

        // replicate to next 3 superpeers
        if (ret != 0 && !p_request.isReplicate()) {
            // Outsource informing backups to another thread to avoid blocking a message handler
            Runnable task = () -> {
                m_overlayLock.readLock().lock();
                short[] backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
                m_overlayLock.readLock().unlock();

                for (short backupSuperpeer : backupSuperpeers) {
                    if (backupSuperpeer == NodeID.INVALID_ID) {
                        continue;
                    }

                    SuperpeerStorageCreateRequest request =
                            new SuperpeerStorageCreateRequest(backupSuperpeer, p_request.getStorageId(),
                                    p_request.getSize(), true);
                    // send as message, only
                    try {
                        m_network.sendMessage(request);
                    } catch (final NetworkException e) {
                        // ignore result
                    }
                }
            };
            new Thread(task).start();
        }
    }

    /**
     * Handles an incoming SuperpeerStorageGetRequest
     *
     * @param p_request
     *         the SuperpeerStorageGetRequest
     */
    private void incomingSuperpeerStorageGetRequest(final SuperpeerStorageGetRequest p_request) {
        byte[] data = m_metadata.getStorage(p_request.getStorageID());

        ChunkByteArray chunk;
        if (data == null) {
            // create invalid entry
            chunk = new ChunkByteArray(p_request.getStorageID(), new byte[0]);
            chunk.setState(ChunkState.DOES_NOT_EXIST);
        } else {
            chunk = new ChunkByteArray(p_request.getStorageID(), data);
            chunk.setState(ChunkState.OK);
        }

        SuperpeerStorageGetResponse response;
        if (chunk.getID() == ChunkID.INVALID_ID) {
            response = new SuperpeerStorageGetResponse(p_request, chunk, (byte) -1);
        } else {
            response = new SuperpeerStorageGetResponse(p_request, chunk, (byte) 0);
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending response to storage get failed: %s", e);

        }
    }

    /**
     * Handles an incoming SuperpeerStorageGetAnonRequest
     *
     * @param p_request
     *         the SuperpeerStorageGetAnonRequest
     */
    private void incomingSuperpeerStorageGetAnonRequest(final SuperpeerStorageGetAnonRequest p_request) {
        byte[] data = m_metadata.getStorage(p_request.getStorageID());

        SuperpeerStorageGetAnonResponse response;
        if (data == null) {
            response = new SuperpeerStorageGetAnonResponse(p_request, null, (byte) -1);
        } else {
            response = new SuperpeerStorageGetAnonResponse(p_request, data, (byte) 0);
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending response to storage get failed: %s", e);

        }
    }

    /**
     * Handles an incoming SuperpeerStoragePutRequest
     *
     * @param p_request
     *         the SuperpeerStoragePutRequest
     */
    private void incomingSuperpeerStoragePutRequest(final SuperpeerStoragePutRequest p_request) {
        ChunkByteArray chunk = p_request.getChunk();

        int res = m_metadata.putStorage((int) chunk.getID(), chunk.getData());
        if (!p_request.isReplicate()) {
            SuperpeerStoragePutResponse response;
            if (res != chunk.sizeofObject()) {
                response = new SuperpeerStoragePutResponse(p_request, (byte) -1);
            } else {
                response = new SuperpeerStoragePutResponse(p_request, (byte) 0);
            }

            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {

                LOGGER.error("Sending response to put request to superpeer storage failed: %s", e);

            }
        }

        // replicate to next 3 superpeers
        if (res != 0 && !p_request.isReplicate()) {
            // Outsource informing backups to another thread to avoid blocking a message handler
            Runnable task = () -> {
                m_overlayLock.readLock().lock();
                short[] backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
                m_overlayLock.readLock().unlock();

                for (short backupSuperpeer : backupSuperpeers) {
                    if (backupSuperpeer == NodeID.INVALID_ID) {
                        continue;
                    }

                    SuperpeerStoragePutRequest request =
                            new SuperpeerStoragePutRequest(backupSuperpeer, p_request.getChunk(), true);
                    // send as message, only
                    try {
                        m_network.sendMessage(request);
                    } catch (final NetworkException e) {
                        // ignore result
                    }
                }
            };
            new Thread(task).start();
        }
    }

    /**
     * Handles an incoming SuperpeerStoragePutAnonRequest
     *
     * @param p_request
     *         the SuperpeerStoragePutAnonRequest
     */
    private void incomingSuperpeerStoragePutAnonRequest(final SuperpeerStoragePutAnonRequest p_request) {
        ChunkByteArray chunk = p_request.getChunk();

        int res = m_metadata.putStorage((int) chunk.getID(), chunk.getData());
        if (!p_request.isReplicate()) {
            SuperpeerStoragePutAnonResponse response;
            if (res != chunk.sizeofObject()) {
                response = new SuperpeerStoragePutAnonResponse(p_request, (byte) -1);
            } else {
                response = new SuperpeerStoragePutAnonResponse(p_request, (byte) 0);
            }

            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {

                LOGGER.error("Sending response to put request to superpeer storage failed: %s", e);

            }
        }

        // replicate to next 3 superpeers
        if (res != 0 && !p_request.isReplicate()) {
            // Outsource informing backups to another thread to avoid blocking a message handler
            Runnable task = () -> {
                m_overlayLock.readLock().lock();
                short[] backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
                m_overlayLock.readLock().unlock();

                for (short backupSuperpeer : backupSuperpeers) {
                    if (backupSuperpeer == NodeID.INVALID_ID) {
                        continue;
                    }

                    SuperpeerStoragePutAnonRequest request = new SuperpeerStoragePutAnonRequest(backupSuperpeer,
                            new ChunkAnon(p_request.getChunk().getID(), p_request.getChunk().getData()), true);

                    // send as message, only
                    try {
                        m_network.sendMessage(request);
                    } catch (final NetworkException e) {
                        // ignore result
                    }
                }
            };
            new Thread(task).start();
        }
    }

    /**
     * Handles an incoming SuperpeerStorageRemoveMessage
     *
     * @param p_request
     *         the SuperpeerStorageRemoveMessage
     */
    private void incomingSuperpeerStorageRemoveMessage(final SuperpeerStorageRemoveMessage p_request) {
        boolean res = m_metadata.removeStorage(p_request.getStorageId());

        if (!res) {
            LOGGER.error("Removing object %d from superpeer storage failed", p_request.getStorageId());
        }

        // replicate to next 3 superpeers
        if (res && !p_request.isReplicate()) {
            // Outsource informing backups to another thread to avoid blocking a message handler
            Runnable task = () -> {
                m_overlayLock.readLock().lock();
                short[] backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
                m_overlayLock.readLock().unlock();

                for (short backupSuperpeer : backupSuperpeers) {
                    if (backupSuperpeer == NodeID.INVALID_ID) {
                        continue;
                    }

                    SuperpeerStorageRemoveMessage request =
                            new SuperpeerStorageRemoveMessage(backupSuperpeer, p_request.getStorageId(), true);
                    // send as message, only
                    try {
                        m_network.sendMessage(request);
                    } catch (final NetworkException e) {
                        // ignore result
                    }
                }
            };
            new Thread(task).start();
        }
    }

    /**
     * Handles an incoming SuperpeerStorageStatusRequest
     *
     * @param p_request
     *         the SuperpeerStorageStatusRequest
     */
    private void incomingSuperpeerStorageStatusRequest(final SuperpeerStorageStatusRequest p_request) {
        SuperpeerStorage.Status status = m_metadata.getStorageStatus();

        SuperpeerStorageStatusResponse response = new SuperpeerStorageStatusResponse(p_request, status);
        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending response to superpeer storage get status message failed: %s", e);

        }
    }

    /**
     * Handles an incoming GetMetadataSummaryRequest
     *
     * @param p_request
     *         the GetMetadataSummaryRequest
     */
    private void incomingGetMetadataSummaryRequest(final GetMetadataSummaryRequest p_request) {
        m_overlayLock.readLock().lock();
        GetMetadataSummaryResponse response =
                new GetMetadataSummaryResponse(p_request, m_metadata.getSummary(m_nodeID, m_predecessor));
        m_overlayLock.readLock().unlock();
        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending response to get metadata summary request failed: %s", e);

        }
    }

    /**
     * Handles an incoming ReplaceBackupPeerRequest
     *
     * @param p_replaceBackupPeerRequest
     *         the ReplaceBackupPeerRequest
     */
    private void incomingReplaceBackupPeerRequest(final ReplaceBackupPeerRequest p_replaceBackupPeerRequest) {
        short rangeID;
        short failedPeer;
        short newBackupPeer;

        LOGGER.trace("Got message: NOTIFY_ABOUT_FAILED_PEER_REQUEST from 0x%X", p_replaceBackupPeerRequest.getSource());

        failedPeer = p_replaceBackupPeerRequest.getFailedPeer();
        newBackupPeer = p_replaceBackupPeerRequest.getNewPeer();
        rangeID = p_replaceBackupPeerRequest.getRangeID();

        m_metadata.replaceFailedPeerInLookupTree(rangeID, p_replaceBackupPeerRequest.getSource(), failedPeer,
                newBackupPeer);

        try {
            m_network.sendMessage(new ReplaceBackupPeerResponse(p_replaceBackupPeerRequest));
        } catch (final NetworkException ignored) {

        }

        if (!p_replaceBackupPeerRequest.isBackup()) {
            // Outsource updating all backup superpeers to another thread to avoid blocking a message handler
            Runnable task = () -> {
                m_overlayLock.readLock().lock();
                short[] backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
                m_overlayLock.readLock().unlock();

                for (short backupSuperpeer : backupSuperpeers) {
                    if (backupSuperpeer == NodeID.INVALID_ID) {
                        continue;
                    }

                    ReplaceBackupPeerRequest request;
                    request = new ReplaceBackupPeerRequest(backupSuperpeer, rangeID, failedPeer, newBackupPeer, true);
                    // send as message, only
                    try {
                        m_network.sendMessage(request);
                    } catch (final NetworkException e) {
                        // ignore result
                    }
                }
            };
            new Thread(task).start();
        }
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
                LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE, SendBackupsMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_UPDATE_METADATA_AFTER_RECOVERY_MESSAGE,
                UpdateMetadataAfterRecoveryMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_REPLACE_BACKUP_PEER_REQUEST, ReplaceBackupPeerRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_REPLACE_BACKUP_PEER_RESPONSE, ReplaceBackupPeerResponse.class);

        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE, PingSuperpeerMessage.class);

        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE, SendSuperpeersMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST, AskAboutBackupsRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE, AskAboutBackupsResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST, AskAboutSuccessorRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE, AskAboutSuccessorResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE, NotifyAboutNewPredecessorMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE, NotifyAboutNewSuccessorMessage.class);
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

        m_network.registerMessageType(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE,
                RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST, RecoverBackupRangeRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE,
                RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE, RecoverBackupRangeResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE,
                RecoveryMessages.SUBTYPE_REPLICATE_BACKUP_RANGE_REQUEST, ReplicateBackupRangeRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE,
                RecoveryMessages.SUBTYPE_REPLICATE_BACKUP_RANGE_RESPONSE, ReplicateBackupRangeResponse.class);
    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_JOIN_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_FINISHED_STARTUP_MESSAGE,
                this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_REQUEST,
                this);
        m_network
                .register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRIES_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_MIGRATE_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_REQUEST,
                this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_UPDATE_METADATA_AFTER_RECOVERY_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_REPLACE_BACKUP_PEER_REQUEST,
                this);
        m_network
                .register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_NODE_JOIN_EVENT_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE, this);

        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_ALLOC_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_FREE_REQUEST, this);
        m_network
                .register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_SIGN_ON_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_STATUS_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_REQUEST,
                this);

        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_CREATE_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_REQUEST,
                this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_ANON_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_REQUEST,
                this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_ANON_REQUEST, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_REMOVE_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_STATUS_REQUEST, this);

        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_METADATA_SUMMARY_REQUEST,
                this);
    }
}

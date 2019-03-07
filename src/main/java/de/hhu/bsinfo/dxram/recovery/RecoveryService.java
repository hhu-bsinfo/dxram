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

package de.hhu.bsinfo.dxram.recovery;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxlog.storage.diskaccess.HarddriveAccessMode;
import de.hhu.bsinfo.dxlog.storage.recovery.RecoveryMetadata;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.recovery.messages.RecoverBackupRangeRequest;
import de.hhu.bsinfo.dxram.recovery.messages.RecoverBackupRangeResponse;
import de.hhu.bsinfo.dxram.recovery.messages.RecoveryMessages;
import de.hhu.bsinfo.dxram.recovery.messages.ReplicateBackupRangeRequest;
import de.hhu.bsinfo.dxram.recovery.messages.ReplicateBackupRangeResponse;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.jni.JNIFileRaw;

/**
 * This service provides all recovery functionality.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.03.16
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class RecoveryService extends AbstractDXRAMService<DXRAMModuleConfig> implements MessageReceiver {
    // component dependencies
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private ChunkBackupComponent m_chunkBackup;
    private LogComponent m_log;
    private LookupComponent m_lookup;
    private NetworkComponent m_network;

    private String m_backupDirectory;
    private ReentrantLock m_recoveryLock;

    private ArrayList<FinishedRecovery> m_finishedRecoveries;
    private ReentrantLock m_replicationLock;

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST:
                        incomingRecoverBackupRangeRequest((RecoverBackupRangeRequest) p_message);
                        break;
                    case RecoveryMessages.SUBTYPE_REPLICATE_BACKUP_RANGE_REQUEST:
                        incomingReplicateBackupRangeRequest((ReplicateBackupRangeRequest) p_message);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunkBackup = p_componentAccessor.getComponent(ChunkBackupComponent.class);
        m_log = p_componentAccessor.getComponent(LogComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        registerNetworkMessages();
        registerNetworkMessageListener();

        if (!m_backup.isActiveAndAvailableForBackup()) {
            LOGGER.debug("Backup is not activated/available. Recovery service will not work!");
        }

        m_backupDirectory = m_backup.getConfig().getBackupDirectory();

        m_recoveryLock = new ReentrantLock(false);

        m_finishedRecoveries = new ArrayList<>();
        m_replicationLock = new ReentrantLock(false);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    /**
     * Recovers all Chunks of given node from log file on this node
     *
     * @param p_owner
     *         the NodeID of the node whose Chunks have to be restored
     */
    private void recoverLocallyFromFile(final short p_owner) {


        HarddriveAccessMode mode = HarddriveAccessMode.RANDOM_ACCESS_FILE;
        if (mode != HarddriveAccessMode.RAW_DEVICE) {
            String fileName;
            File folderToScan;
            File[] listOfFiles;
            AbstractChunk[] chunks;

            if (!m_backup.isActiveAndAvailableForBackup()) {

                LOGGER.warn("Backup is not activated. Cannot recover!");

            } else {
                folderToScan = new File(m_backupDirectory);
                listOfFiles = folderToScan.listFiles();
                assert listOfFiles != null;
                for (int i = 0; i < listOfFiles.length; i++) {
                    if (listOfFiles[i].isFile()) {
                        fileName = listOfFiles[i].getName();
                        if (fileName.contains("sec" + p_owner)) {
                            chunks = m_log.recoverBackupRangeFromFile(fileName, m_backupDirectory);

                            if (chunks == null) {

                                LOGGER.error("Cannot recover Chunks! Trying next file.");

                                continue;
                            }

                            LOGGER.info("Retrieved %d Chunks from file", chunks.length);

                            // Store recovered Chunks
                            //m_chunkBackup.putRecoveredChunks(chunks);

                            if (fileName.contains("M")) {
                                // Inform superpeers about new location of migrated Chunks (non-migrated Chunks are
                                // processed later)
                                for (AbstractChunk chunk : chunks) {
                                    if (ChunkID.getCreatorID(chunk.getID()) != p_owner) {
                                        // TODO: This might crash because there is no tree for creator of this chunk
                                        m_lookup.migrate(chunk.getID(), m_boot.getNodeId());
                                    }
                                }
                            }
                        }
                    }
                }

                // Inform superpeers about new location of non-migrated Chunks
            }
        } else {
            String fileName;
            String files;
            String[] listOfFiles;
            AbstractChunk[] chunks;

            if (!m_backup.isActiveAndAvailableForBackup()) {

                LOGGER.warn("Backup is not activated. Cannot recover!");

            } else {
                // Get list of all files in RAW device
                files = JNIFileRaw.getFileList();
                // split string to get filenames
                listOfFiles = files.split("[\\n]+");
                for (int i = 0; i < listOfFiles.length; i++) {
                    fileName = listOfFiles[i];
                    if (fileName.contains("sec" + p_owner)) {
                        chunks = m_log.recoverBackupRangeFromFile(fileName, m_backupDirectory);

                        if (chunks == null) {

                            LOGGER.error("Cannot recover Chunks! Trying next file.");

                            continue;
                        }

                        LOGGER.info("Retrieved %d Chunks from file", chunks.length);

                        // Store recovered Chunks
                        //m_chunkBackup.putRecoveredChunks(chunks);

                        if (fileName.contains("M")) {
                            // Inform superpeers about new location of migrated Chunks (non-migrated Chunks are
                            // processed later)
                            for (AbstractChunk chunk : chunks) {
                                if (ChunkID.getCreatorID(chunk.getID()) != p_owner) {
                                    // TODO: This might crash because there is no tree for creator of this chunk
                                    m_lookup.migrate(chunk.getID(), m_boot.getNodeId());
                                }
                            }
                        }
                    }
                }

                // Inform superpeers about new location of non-migrated Chunks
            }
        }
    }

    /**
     * Replicates all Chunks of given backup range
     *
     * @param p_owner
     *         the NodeID of the node whose Chunks have to be restored
     * @param p_backupRange
     *         the backup range
     * @return the recovery metadata
     */
    private RecoveryMetadata recoverBackupRange(final short p_owner, final BackupRange p_backupRange) {
        RecoveryMetadata ret;
        short rangeID;

        rangeID = p_backupRange.getRangeID();

        m_recoveryLock.lock();
        ret = m_log.recoverBackupRange(p_owner, rangeID);
        if (ret != null) {
            m_log.removeBackupRange(p_owner, rangeID);
        }
        m_recoveryLock.unlock();

        return ret;
    }

    /**
     * Handles an incoming RecoverBackupRangeRequest
     *
     * @param p_request
     *         the RecoverBackupRangeRequest
     */
    private void incomingRecoverBackupRangeRequest(final RecoverBackupRangeRequest p_request) {
        /*
         * Things to do to recover a backup range:
         *  1) It is a normal backup range:
         *      In LogComponent:
         *      a) Flush all data to secondary logs (first primary buffer, then specific secondary log buffer)
         *      b) Block reorganization thread
         *      c) Get MemoryManager write lock
         *      d) Create a version array:
         *          i) Number of chunks in backup range: last ChunkID - first ChunkID + 1
         *      e) Read all versions in array (do not write back!)
         *      f) For all not empty segments in log (backup range):
         *          i)   Read the whole segment from SSD
         *          ii)  Create an array for ChunkIDs, offsets in segment buffer and lengths for a batch of to be
         *               stored chunks
         *          iii) Iterate over all log entries. If an entry is valid and uncorrupted, store its ChunkID, offset
         *               length in the arrays in ChunkBackupComponent
         *          iv)  If batch size is reached or segment is fully iterated, store all chunks in memory management
         *               with one call
         *      g) Release MemoryManager write lock
         *      h) Unblock reorganization thread
         *      i) Remove backup range from log module:
         *          - Remove secondary log and secondary log buffer from log catalog
         *          - Close secondary log buffer
         *          - Close version buffer and delete version log from hard drive
         *          - Close secondary log and delete it from hard drive
         *      j)
         *
         *
         *  2) It is a migration backup range:
         *      In LogComponent:
         *      a) Flush all data to secondary logs (first primary buffer, then specific secondary log buffer)
         *      b) Block reorganization thread
         *      c) Get MemoryManager write lock
         *      d) Create a version hashtable:
         *          i) Number of chunks in backup range is unknown, at the beginning hashtable is large enough for
         *             an average chunk size of 32 bytes
         *      e) Read all versions in hashtable (do not write back!):
         *          i)   If there are more versions written to hard drive than fitting in hashtable, resize hashtable
         *               to number of written versions
         *          ii)  After reading all versions from hard drive: If there are too many new versions in version
         *               buffer, resize hashtable again
         *          iii) Hashtable is always sized for the worst case scenario to prevent rehashing during filling
         *      f) For all not empty segments in log (backup range):
         *          i)   Read the whole segment from SSD
         *          ii)  Create an array for ChunkIDs, offsets in segment buffer and lengths for a batch of to be
         *               stored chunks
         *          iii) Iterate over all log entries. If an entry is valid and uncorrupted, store its ChunkID, offset
         *              length in the arrays
         *          In ChunkBackupComponent
         *          iv)  If batch size is reached or segment is fully iterated, store all chunks in memory management
         *               with one call
         *      g) Release MemoryManager write lock
         *      h) Unblock reorganization thread
         *      i) Remove backup range from log module:
         *          - Remove secondary log and secondary log buffer from log catalog
         *          - Close secondary log buffer
         *          - Close version buffer and delete version log from hard drive
         *          - Close secondary log and delete it from hard drive
         *      j)
         */

        // Outsource recovery to another thread to avoid blocking a message handler
        Runnable task = () -> {
            short replacementBackupPeer;
            BackupRange backupRange = p_request.getBackupRange();

            // Recover all chunks of given backup range, store them in chunk module and remove log
            RecoveryMetadata recoveryMetadata = recoverBackupRange(p_request.getOwner(), backupRange);

            if (recoveryMetadata == null) {
                try {
                    m_network.sendMessage(new RecoverBackupRangeResponse(p_request, null, 0, null));
                } catch (final NetworkException ignored) {

                    LOGGER.error("RecoverBackupRangeResponse could not be sent!");

                }
            } else {
                // Initialize backup ranges in backup, lookup and log modules by joining recovered chunks with
                // migrated chunks
                replacementBackupPeer =
                        m_backup.registerRecoveredChunks(recoveryMetadata, backupRange, p_request.getOwner());

                // Store recovery metadata for replication of recovered backup range to be handled by another thread
                // after initialization by superpeer
                m_replicationLock.lock();
                m_finishedRecoveries.add(new FinishedRecovery(replacementBackupPeer, recoveryMetadata.getCIDRanges(),
                        recoveryMetadata.getNumberOfChunks(), backupRange.getRangeID()));

                m_replicationLock.unlock();

                try {
                    m_network.sendMessage(
                            new RecoverBackupRangeResponse(p_request, backupRange, recoveryMetadata.getNumberOfChunks(),
                                    recoveryMetadata.getCIDRanges()));
                } catch (final NetworkException ignored) {

                }
            }
        };
        new Thread(task).start();
    }

    /**
     * Recovers all Chunks of given backup range
     *
     * @param p_rangeID
     *         the backup range ID
     */
    private void replicateBackupRange(final short p_rangeID) {
        FinishedRecovery finishedRecovery = null;

        m_replicationLock.lock();
        for (FinishedRecovery recovery : m_finishedRecoveries) {
            if (recovery.getRangeID() == p_rangeID) {
                finishedRecovery = recovery;
                break;
            }
        }
        m_replicationLock.unlock();

        if (finishedRecovery != null) {
            // Send replicas to backup peers
            if (finishedRecovery.getReplacementBackupPeer() != NodeID.INVALID_ID) {
                m_chunkBackup.replicateBackupRange(finishedRecovery.getReplacementBackupPeer(),
                        finishedRecovery.getCIDRanges(), finishedRecovery.getNumberOfChunks(),
                        finishedRecovery.getRangeID());
            }
        }
    }

    /**
     * Handles an incoming ReplicateBackupRangeRequest
     *
     * @param p_request
     *         the ReplicateBackupRangeRequest
     */
    private void incomingReplicateBackupRangeRequest(final ReplicateBackupRangeRequest p_request) {
        replicateBackupRange(p_request.getRangeID());

        try {
            m_network.sendMessage(new ReplicateBackupRangeResponse(p_request));
        } catch (final NetworkException ignored) {

        }
    }

    // -----------------------------------------------------------------------------------

    /**
     * Register network messages we use in here.
     */
    private void registerNetworkMessages() {
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
        m_network.register(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE,
                RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST, this);
        m_network.register(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE,
                RecoveryMessages.SUBTYPE_REPLICATE_BACKUP_RANGE_REQUEST, this);
    }

}

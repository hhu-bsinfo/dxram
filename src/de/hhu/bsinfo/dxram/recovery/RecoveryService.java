package de.hhu.bsinfo.dxram.recovery;

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.recovery.messages.RecoverBackupRangeRequest;
import de.hhu.bsinfo.dxram.recovery.messages.RecoverBackupRangeResponse;
import de.hhu.bsinfo.dxram.recovery.messages.RecoverMessage;
import de.hhu.bsinfo.dxram.recovery.messages.RecoveryMessages;
import de.hhu.bsinfo.dxram.util.HarddriveAccessMode;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.JNIFileRaw;

/**
 * This service provides all recovery functionality.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.03.16
 */
public class RecoveryService extends AbstractDXRAMService implements MessageReceiver {

    private static final Logger LOGGER = LogManager.getFormatterLogger(RecoveryService.class.getSimpleName());

    // dependent components
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private ChunkBackupComponent m_chunk;
    private LogComponent m_log;
    private LookupComponent m_lookup;
    private NetworkComponent m_network;

    private String m_backupDirectory;
    private ReentrantLock m_recoveryLock;

    /**
     * Constructor
     */
    public RecoveryService() {
        super("recovery");
    }

    /**
     * Recovers all Chunks of given node
     *
     * @param p_owner
     *     the NodeID of the node whose Chunks have to be restored
     * @param p_dest
     *     the NodeID of the node where the Chunks have to be restored
     * @param p_useLiveData
     *     whether the recover should use current logs or log files
     * @return whether the operation was successful or not
     */
    public boolean recover(final short p_owner, final short p_dest, final boolean p_useLiveData) {
        boolean ret = true;

        if (p_dest == m_boot.getNodeID()) {
            if (p_useLiveData) {
                recoverLocally(p_owner);
            } else {
                recoverLocallyFromFile(p_owner);
            }
        } else {
            // #if LOGGER >= INFO
            LOGGER.info("Forwarding recovery to 0x%X", p_dest);
            // #endif /* LOGGER >= INFO */
            try {
                m_network.sendMessage(new RecoverMessage(p_dest, p_owner, p_useLiveData));
            } catch (final NetworkException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not forward command to 0x%X. Aborting recovery!", p_dest);
                // #endif /* LOGGER >= ERROR */
                ret = false;
            }
        }

        return ret;
    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case RecoveryMessages.SUBTYPE_RECOVER_MESSAGE:
                        incomingRecoverMessage((RecoverMessage) p_message);
                        break;
                    case RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST:
                        incomingRecoverBackupRangeRequest((RecoverBackupRangeRequest) p_message);
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
        m_chunk = p_componentAccessor.getComponent(ChunkBackupComponent.class);
        m_log = p_componentAccessor.getComponent(LogComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        registerNetworkMessages();
        registerNetworkMessageListener();

        // #if LOGGER >= WARN
        if (!m_backup.isActive()) {
            LOGGER.warn("Backup is not activated. Recovery service will not work!");
        }
        // #endif /* LOGGER >= WARN */
        m_backupDirectory = m_backup.getBackupDirectory();

        m_recoveryLock = new ReentrantLock(false);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    /**
     * Recovers all Chunks of given node on this node
     *
     * @param p_owner
     *     the NodeID of the node whose Chunks have to be restored
     */
    private void recoverLocally(final short p_owner) {
        int count;
        long firstChunkIDOrRangeID;
        BackupRange[] backupRanges;

        if (!m_backup.isActive()) {
            // #if LOGGER >= WARN
            LOGGER.warn("Backup is not activated. Cannot recover!");
            // #endif /* LOGGER >= WARN */
        } else {
            backupRanges = m_lookup.getAllBackupRanges(p_owner);
            if (backupRanges != null) {
                for (BackupRange backupRange : backupRanges) {
                    firstChunkIDOrRangeID = backupRange.getRangeID();

                    if (ChunkID.getCreatorID(firstChunkIDOrRangeID) == p_owner) {
                        count = m_log.recoverBackupRange(p_owner, firstChunkIDOrRangeID, (byte) -1);
                    } else {
                        count = m_log.recoverBackupRange(p_owner, -1, (byte) firstChunkIDOrRangeID);
                    }

                    // #if LOGGER >= INFO
                    LOGGER.info("Recovered %d Chunks", count);
                    // #endif /* LOGGER >= INFO */

                    // Inform superpeers about new location of migrated Chunks (non-migrated Chunks are processed later)
                    /*-for (Chunk chunk : chunks) {
                        if (ChunkID.getCreatorID(chunk.getID()) != p_owner) {
                            m_lookup.migrate(chunk.getID(), m_boot.getNodeID());
                        }
                    }*/
                }
                // Inform superpeers about new location of non-migrated Chunks
                m_lookup.setRestorerAfterRecovery(p_owner);
            }
        }
    }

    /**
     * Recovers all Chunks of given node from log file on this node
     *
     * @param p_owner
     *     the NodeID of the node whose Chunks have to be restored
     */
    private void recoverLocallyFromFile(final short p_owner) {

        // TODO: Read-in access mode
        HarddriveAccessMode mode = HarddriveAccessMode.RANDOM_ACCESS_FILE;
        if (mode != HarddriveAccessMode.RAW_DEVICE) {
            String fileName;
            File folderToScan;
            File[] listOfFiles;
            Chunk[] chunks;

            if (!m_backup.isActive()) {
                // #if LOGGER >= WARN
                LOGGER.warn("Backup is not activated. Cannot recover!");
                // #endif /* LOGGER >= WARN */
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
                                // #if LOGGER >= ERROR
                                LOGGER.error("Cannot recover Chunks! Trying next file.");
                                // #endif /* LOGGER >= ERROR */
                                continue;
                            }
                            // #if LOGGER >= INFO
                            LOGGER.info("Retrieved %d Chunks from file", chunks.length);
                            // #endif /* LOGGER >= INFO */

                            // Store recovered Chunks
                            m_chunk.putRecoveredChunks(chunks);

                            if (fileName.contains("M")) {
                                // Inform superpeers about new location of migrated Chunks (non-migrated Chunks are
                                // processed later)
                                for (Chunk chunk : chunks) {
                                    if (ChunkID.getCreatorID(chunk.getID()) != p_owner) {
                                        // TODO: This might crash because there is no tree for creator of this chunk
                                        m_lookup.migrate(chunk.getID(), m_boot.getNodeID());
                                    }
                                }
                            }
                        }
                    }
                }

                // Inform superpeers about new location of non-migrated Chunks
                // TODO: This might crash because there is no tree for recovered peer
                m_lookup.setRestorerAfterRecovery(p_owner);
            }
        } else {
            String fileName;
            String files;
            String[] listOfFiles;
            Chunk[] chunks = null;

            if (!m_backup.isActive()) {
                // #if LOGGER >= WARN
                LOGGER.warn("Backup is not activated. Cannot recover!");
                // #endif /* LOGGER >= WARN */
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
                            // #if LOGGER >= ERROR
                            LOGGER.error("Cannot recover Chunks! Trying next file.");
                            // #endif /* LOGGER >= ERROR */
                            continue;
                        }
                        // #if LOGGER >= INFO
                        LOGGER.info("Retrieved %d Chunks from file", chunks.length);
                        // #endif /* LOGGER >= INFO */

                        // Store recovered Chunks
                        m_chunk.putRecoveredChunks(chunks);

                        if (fileName.contains("M")) {
                            // Inform superpeers about new location of migrated Chunks (non-migrated Chunks are
                            // processed later)
                            for (Chunk chunk : chunks) {
                                if (ChunkID.getCreatorID(chunk.getID()) != p_owner) {
                                    // TODO: This might crash because there is no tree for creator of this chunk
                                    m_lookup.migrate(chunk.getID(), m_boot.getNodeID());
                                }
                            }
                        }
                    }
                }

                // Inform superpeers about new location of non-migrated Chunks
                // TODO: This might crash because there is no tree for recovered peer
                m_lookup.setRestorerAfterRecovery(p_owner);
            }
        }
    }

    /**
     * Recovers all Chunks of given node on this node
     *
     * @param p_owner
     *     the NodeID of the node whose Chunks have to be restored
     * @param p_firstChunkIDOrRangeID
     *     the RangeID or first ChunkID or range
     * @return the number of recovered chunks
     */
    private int recoverBackupRange(final short p_owner, final long p_firstChunkIDOrRangeID) {
        int ret;

        m_recoveryLock.lock();

        if (ChunkID.getCreatorID(p_firstChunkIDOrRangeID) == p_owner) {
            // Failed peer was the creator -> recover normal backup range
            ret = m_log.recoverBackupRange(p_owner, p_firstChunkIDOrRangeID, (byte) -1);
        } else {
            // Failed peer was not the creator -> recover migration backup range
            ret = m_log.recoverBackupRange(p_owner, -1, (byte) p_firstChunkIDOrRangeID);
        }

        if (ret == 0) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot recover Chunks locally");
            // #endif /* LOGGER >= ERROR */
        }

        m_recoveryLock.unlock();

        // TODO: Not complete!

        return ret;
    }

    /**
     * Handles an incoming RecoverMessage
     *
     * @param p_message
     *     the RecoverMessage
     */
    private void incomingRecoverMessage(final RecoverMessage p_message) {
        // Outsource recovery to another thread to avoid blocking a message handler
        Runnable task = () -> {
            if (p_message.useLiveData()) {
                recoverLocally(p_message.getOwner());
            } else {
                recoverLocallyFromFile(p_message.getOwner());
            }
        };
        new Thread(task).start();
    }

    /**
     * Handles an incoming GetChunkIDRequest
     *
     * @param p_request
     *     the RecoverBackupRangeRequest
     */
    private void incomingRecoverBackupRangeRequest(final RecoverBackupRangeRequest p_request) {
        // Outsource recovery to another thread to avoid blocking a message handler
        Runnable task = () -> {
            int recoveredChunks = recoverBackupRange(p_request.getOwner(), p_request.getFirstChunkIDOrRangeID());
            try {
                m_network.sendMessage(new RecoverBackupRangeResponse(p_request, recoveredChunks));
            } catch (final NetworkException ignored) {

            }
        };
        new Thread(task).start();
    }

    // -----------------------------------------------------------------------------------

    /**
     * Register network messages we use in here.
     */
    private void registerNetworkMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE, RecoveryMessages.SUBTYPE_RECOVER_MESSAGE, RecoverMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST,
            RecoverBackupRangeRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE,
            RecoverBackupRangeResponse.class);
    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(RecoverMessage.class, this);
        m_network.register(RecoverBackupRangeRequest.class, this);
    }

}

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

package de.hhu.bsinfo.dxram.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxmem.DXMem;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.operations.Recovery;
import de.hhu.bsinfo.dxnet.core.MessageHeader;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.log.messages.InitBackupRangeRequest;
import de.hhu.bsinfo.dxram.log.messages.InitBackupRangeResponse;
import de.hhu.bsinfo.dxram.log.messages.InitRecoveredBackupRangeRequest;
import de.hhu.bsinfo.dxram.log.messages.InitRecoveredBackupRangeResponse;
import de.hhu.bsinfo.dxram.log.storage.BackupRangeCatalog;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.Scheduler;
import de.hhu.bsinfo.dxram.log.storage.diskaccess.HarddriveAccessMode;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.header.ChecksumHandler;
import de.hhu.bsinfo.dxram.log.storage.logs.Log;
import de.hhu.bsinfo.dxram.log.storage.logs.LogHandler;
import de.hhu.bsinfo.dxram.log.storage.recovery.FileRecoveryHandler;
import de.hhu.bsinfo.dxram.log.storage.recovery.LogRecoveryHandler;
import de.hhu.bsinfo.dxram.log.storage.recovery.RecoveryMetadata;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionHandler;
import de.hhu.bsinfo.dxram.log.storage.writebuffer.BufferPool;
import de.hhu.bsinfo.dxram.log.storage.writebuffer.WriteBufferHandler;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.jni.JNIFileRaw;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;

/**
 * This service provides access to the backend storage system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public final class LogComponent extends AbstractDXRAMComponent<LogComponentConfig> {

    private static final TimePool SOP_LOG_BATCH = new TimePool(LogComponent.class, "LogBatch");
    private static final TimePool SOP_PUT_ENTRY_AND_HEADER = new TimePool(LogComponent.class, "PutEntryAndHeader");

    public static final boolean TWO_LEVEL_LOGGING_ACTIVATED = true;

    static {
        StatisticsManager.get().registerOperation(LogComponent.class, SOP_LOG_BATCH);
        StatisticsManager.get().registerOperation(LogComponent.class, SOP_PUT_ENTRY_AND_HEADER);
    }

    // component dependencies
    private NetworkComponent m_network;
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private ChunkComponent m_chunk;

    private Recovery m_dxmemRecoveryOp;

    // private state
    private LogHandler m_logHandler;
    private VersionHandler m_versionHandler;
    private WriteBufferHandler m_writeBufferHandler;
    private LogRecoveryHandler m_logRecoveryHandler;
    private BackupRangeCatalog m_backupRangeCatalog;

    private short m_nodeID;
    private boolean m_loggingIsActive;
    private long m_secondaryLogSize;
    private String m_backupDirectory;
    private HarddriveAccessMode m_mode;

    private long m_initTime;

    /**
     * Creates the log component
     */
    public LogComponent() {
        super(DXRAMComponentOrder.Init.LOG, DXRAMComponentOrder.Shutdown.LOG, LogComponentConfig.class);
    }

    /**
     * Returns the header size
     *
     * @param p_nodeID
     *         the NodeID
     * @param p_localID
     *         the LocalID
     * @param p_size
     *         the size of the Chunk
     * @return the header size
     */
    public short getApproxHeaderSize(final short p_nodeID, final long p_localID, final int p_size) {
        return AbstractSecLogEntryHeader.getApproxSecLogHeaderSize(m_boot.getNodeId() != p_nodeID, p_localID, p_size);
    }

    /**
     * Initializes a new backup range
     *
     * @param p_backupRange
     *         the backup range
     */
    public void initBackupRange(final BackupRange p_backupRange) {
        BackupPeer[] backupPeers;
        InitBackupRangeRequest request;
        InitBackupRangeResponse response;
        long time;

        backupPeers = p_backupRange.getBackupPeers();

        time = System.currentTimeMillis();
        if (backupPeers != null) {
            for (int i = 0; i < backupPeers.length; i++) {
                if (backupPeers[i] != null) {
                    // The last peer is new in a recovered backup range
                    request = new InitBackupRangeRequest(backupPeers[i].getNodeID(), p_backupRange.getRangeID());

                    try {
                        m_network.sendSync(request);
                    } catch (final NetworkException ignore) {
                        i--;
                        continue;
                    }

                    response = request.getResponse(InitBackupRangeResponse.class);

                    if (!response.getStatus()) {
                        i--;
                    }
                }
            }
        }

        LOGGER.trace("Time to initialize range: %d", System.currentTimeMillis() - time);

    }

    /**
     * Initializes a recovered backup range
     *
     * @param p_backupRange
     *         the backup range
     * @param p_oldBackupRange
     *         the old backup range on the failed peer
     * @param p_failedPeer
     *         the failed peer
     */
    public void initRecoveredBackupRange(final BackupRange p_backupRange, final short p_oldBackupRange,
            final short p_failedPeer, final short p_newBackupPeer) {
        BackupPeer[] backupPeers = p_backupRange.getBackupPeers();
        if (backupPeers != null) {
            for (int i = 0; i < backupPeers.length; i++) {
                if (backupPeers[i] != null) {
                    if (backupPeers[i].getNodeID() == p_newBackupPeer) {
                        initBackupRangeOnPeer(backupPeers[i].getNodeID(), p_backupRange.getRangeID(), p_oldBackupRange,
                                p_failedPeer, true);
                    } else {
                        initBackupRangeOnPeer(backupPeers[i].getNodeID(), p_backupRange.getRangeID(), p_oldBackupRange,
                                p_failedPeer, false);
                    }
                }
            }
        }
    }

    /**
     * Initializes a new backup range
     *
     * @param p_backupPeer
     *         the backup peer
     * @param p_rangeID
     *         the new range ID
     * @param p_originalRangeID
     *         the old range ID
     * @param p_originalOwner
     *         the failed peer
     * @param p_isNewPeer
     *         whether this backup range is new for given backup peer or already stored for failed peer
     */
    private void initBackupRangeOnPeer(final short p_backupPeer, final short p_rangeID, final short p_originalRangeID,
            final short p_originalOwner, final boolean p_isNewPeer) {
        InitRecoveredBackupRangeRequest request;
        InitRecoveredBackupRangeResponse response;
        long time;

        time = System.currentTimeMillis();
        request = new InitRecoveredBackupRangeRequest(p_backupPeer, p_rangeID, p_originalRangeID, p_originalOwner,
                p_isNewPeer);
        try {
            m_network.sendSync(request);
        } catch (final NetworkException ignore) {
        }

        response = request.getResponse(InitRecoveredBackupRangeResponse.class);

        if (response == null || !response.getStatus()) {

            LOGGER.error("Backup range could not be initialized on 0x%X!", p_backupPeer);

        }

        LOGGER.trace("Time to initialize range: %d", System.currentTimeMillis() - time);

    }

    /**
     * Removes the logs and buffers from given backup range.
     *
     * @param p_owner
     *         the owner of the backup range
     * @param p_rangeID
     *         the RangeID
     */
    public void removeBackupRange(final short p_owner, final short p_rangeID) {
        m_logHandler.removeBackupRange(p_owner, p_rangeID);
    }

    /**
     * Recovers all Chunks of given backup range
     *
     * @param p_owner
     *         the NodeID of the node whose Chunks have to be restored
     * @param p_rangeID
     *         the RangeID
     * @return the recovery metadata
     */
    public RecoveryMetadata recoverBackupRange(final short p_owner, final short p_rangeID) {
        return m_logRecoveryHandler.recoverBackupRange(p_owner, p_rangeID, m_dxmemRecoveryOp);
    }

    /**
     * Recovers all Chunks of given backup range
     *
     * @param p_fileName
     *         the file name
     * @param p_path
     *         the path of the folder the file is in
     * @return the recovered Chunks
     */
    public AbstractChunk[] recoverBackupRangeFromFile(final String p_fileName, final String p_path) {
        AbstractChunk[] ret = null;

        try {
            ret = FileRecoveryHandler
                    .recoverFromFile(p_fileName, p_path, getConfig().isUseChecksums(), m_secondaryLogSize,
                            (int) getConfig().getLogSegmentSize().getBytes());
        } catch (final IOException e) {

            LOGGER.error("Could not recover from file %s: %s", p_path, e);

        }

        return ret;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config, final DXRAMJNIManager p_jniManager) {

        applyConfiguration(getConfig());

        m_loggingIsActive = m_boot.getNodeRole() == NodeRole.PEER && m_backup.isActiveAndAvailableForBackup();
        if (m_loggingIsActive) {
            m_nodeID = m_boot.getNodeId();
            m_backupDirectory = m_backup.getConfig().getBackupDirectory();
            m_secondaryLogSize = m_backup.getConfig().getBackupRangeSize().getBytes() * 2;

            // Load jni modules
            if (!p_jniManager.loadJNIModule("JNINativeCRCGenerator")) {
                return false;
            }

            if (m_mode == HarddriveAccessMode.ODIRECT) {
                if (!p_jniManager.loadJNIModule(HarddriveAccessMode.getJNIFileName(m_mode))) {
                    return false;
                }
            } else if (m_mode == HarddriveAccessMode.RAW_DEVICE) {
                if (!p_jniManager.loadJNIModule(HarddriveAccessMode.getJNIFileName(m_mode))) {
                    return false;
                }

                if (JNIFileRaw.prepareRawDevice(getConfig().getRawDevicePath(), 0) == -1) {
                    LOGGER.debug("\n     * Steps to prepare a raw device:\n" + "     * 1) Use an empty partition\n" +
                            "     * 2) If executed in nspawn container: add \"--capability=CAP_SYS_MODULE " +
                            "--bind-ro=/lib/modules\" to systemd-nspawn command in boot script\n" +
                            "     * 3) Get root access\n" + "     * 4) mkdir /dev/raw\n" + "     * 5) cd /dev/raw/\n" +
                            "     * 6) mknod raw1 c 162 1\n" + "     * 7) modprobe raw\n" +
                            "     * 8) If /dev/raw/rawctl was not created: mknod /dev/raw/rawctl c 162 0\n" +
                            "     * 9) raw /dev/raw/raw1 /dev/*empty partition*\n" +
                            "     * 10) Execute DXRAM as root user (sudo -P for nfs)");
                    throw new RuntimeException("Raw device could not be prepared!");
                }
            }

            purgeLogDirectory(m_backupDirectory);

            createHandlers();

            // Get the recovery operation for recovery.
            m_dxmemRecoveryOp = m_chunk.getMemory().recovery();
        }

        return true;
    }

    /**
     * Init component. Used for component testing, only.
     *
     * @param p_config
     *         the log component configuration
     * @param p_backupDir
     *         the directory to store logs in
     * @param p_backupRangeSize
     *         the backup range size
     * @param p_kvss
     *         the key-value store size; -1 if memory management is not required
     */
    void initComponent(final LogComponentConfig p_config, final String p_backupDir, final int p_backupRangeSize,
            final long p_kvss) {
        setConfig(p_config);

        applyConfiguration(p_config);

        m_loggingIsActive = true;
        m_nodeID = (short) 1;
        m_backupDirectory = p_backupDir;
        m_secondaryLogSize = p_backupRangeSize * 2;

        // Load jni modules
        String cwd = System.getProperty("user.dir");
        String path = cwd + "/jni/libJNINativeCRCGenerator.so";
        System.load(path);

        if (m_mode == HarddriveAccessMode.ODIRECT) {
            path = cwd + "/jni/libJNIFileDirect.so";
            System.load(path);
        } else if (m_mode == HarddriveAccessMode.RAW_DEVICE) {
            path = cwd + "/jni/libJNIFileRaw.so";
            System.load(path);
            if (JNIFileRaw.prepareRawDevice(getConfig().getRawDevicePath(), 0) == -1) {
                LOGGER.debug("\n     * Steps to prepare a raw device:\n" + "     * 1) Use an empty partition\n" +
                        "     * 2) If executed in nspawn container: add \"--capability=CAP_SYS_MODULE " +
                        "--bind-ro=/lib/modules\" to systemd-nspawn command in boot script\n" +
                        "     * 3) Get root access\n" + "     * 4) mkdir /dev/raw\n" + "     * 5) cd /dev/raw/\n" +
                        "     * 6) mknod raw1 c 162 1\n" + "     * 7) modprobe raw\n" +
                        "     * 8) If /dev/raw/rawctl was not created: mknod /dev/raw/rawctl c 162 0\n" +
                        "     * 9) raw /dev/raw/raw1 /dev/*empty partition*\n" +
                        "     * 10) Execute DXRAM as root user (sudo -P for nfs)");
                throw new RuntimeException("Raw device could not be prepared!");
            }
        }

        purgeLogDirectory(m_backupDirectory);

        createHandlers();

        if (p_kvss != -1) {
            // Initialize DXMem and get the recovery operation for recovery.
            m_dxmemRecoveryOp = new DXMem(m_nodeID, p_kvss).recovery();
        }
    }

    /**
     * Apply configuration for static attributes. Do NOT change the order!
     *
     * @param p_config
     *         the configuration
     */
    private void applyConfiguration(final LogComponentConfig p_config) {
        m_mode = HarddriveAccessMode.convert(p_config.getHarddriveAccess());

        if (m_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
            DirectByteBufferWrapper.useNativeBuffers(false);
            ChecksumHandler.useNativeBuffers(false);
        } else {
            DirectByteBufferWrapper.useNativeBuffers(true);
            ChecksumHandler.useNativeBuffers(true);
        }
        DirectByteBufferWrapper.setPageSize((int) p_config.getFlashPageSize().getBytes());
        // Set the segment size. Needed for log entry header to split large chunks (must be called before the first log
        // entry header is created)
        AbstractLogEntryHeader.setSegmentSize((int) p_config.getLogSegmentSize().getBytes());
        // Set the log entry header tsp size (must be called before the first log entry header is created)
        AbstractLogEntryHeader.setTimestampSize(p_config.isUseTimestamps());
        // Set the log entry header crc size (must be called before the first log entry header is created)
        ChecksumHandler.setCRCSize(p_config.isUseChecksums());
        // Set the hard drive access mode (must be called before the first log is created)
        Log.setAccessMode(m_mode);

        m_initTime = System.currentTimeMillis();
    }

    /**
     * Purge all logs from directory.
     *
     * @param p_path
     *         the directory
     */
    private static void purgeLogDirectory(final String p_path) {
        File dir = new File(p_path);
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.isDirectory()) {
                    if (!file.delete()) {
                        // Ignore. Will be overwritten anyways
                    }
                }
            }
        }
    }

    /**
     * Create all handlers and the backup range catalog.
     */
    private void createHandlers() {
        m_backupRangeCatalog = new BackupRangeCatalog();

        Scheduler scheduler = new Scheduler();
        BufferPool bufferPool = new BufferPool((int) getConfig().getLogSegmentSize().getBytes());
        m_versionHandler = new VersionHandler(scheduler, m_backupRangeCatalog, m_secondaryLogSize);
        m_logHandler = new LogHandler(m_versionHandler, scheduler, m_backupRangeCatalog, bufferPool,
                getConfig().getPrimaryLogSize().getBytes(), m_secondaryLogSize,
                (int) getConfig().getSecondaryLogBufferSize().getBytes(),
                (int) getConfig().getLogSegmentSize().getBytes(), (int) getConfig().getFlashPageSize().getBytes(),
                getConfig().isUseChecksums(), getConfig().getUtilizationActivateReorganization(),
                getConfig().isUseTimestamps(), getConfig().getColdDataThresholdInSec(), m_backupDirectory, m_nodeID);
        m_writeBufferHandler = new WriteBufferHandler(m_logHandler, m_versionHandler, scheduler, bufferPool,
                (int) getConfig().getWriteBufferSize().getBytes(),
                (int) getConfig().getSecondaryLogBufferSize().getBytes(),
                (int) getConfig().getFlashPageSize().getBytes(), getConfig().isUseChecksums(),
                getConfig().isUseTimestamps(), m_initTime);
        scheduler.set(m_writeBufferHandler, m_logHandler);

        m_logRecoveryHandler =
                new LogRecoveryHandler(m_versionHandler, scheduler, m_backupRangeCatalog, m_secondaryLogSize,
                        (int) getConfig().getLogSegmentSize().getBytes(), getConfig().isUseChecksums());
    }

    @Override
    protected boolean shutdownComponent() {
        if (m_loggingIsActive) {
            m_writeBufferHandler.close();
            m_logHandler.close();
            m_versionHandler.close();
            m_logRecoveryHandler.close();

            m_backupRangeCatalog.closeLogsAndBuffers();
            m_backupRangeCatalog = null;
        }

        return true;
    }

    /**
     * Initializes a new backup range
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_owner
     *         the Chunks' owner
     * @return whether the operation was successful or not
     */
    boolean incomingInitBackupRange(final short p_rangeID, final short p_owner) {
        return m_logHandler.createBackupRange(p_rangeID, p_owner, m_secondaryLogSize,
                (int) getConfig().getLogSegmentSize().getBytes(),
                (int) getConfig().getSecondaryLogBufferSize().getBytes(),
                (int) getConfig().getFlashPageSize().getBytes(), getConfig().getUtilizationPromptReorganization(),
                getConfig().isUseChecksums(), getConfig().isUseTimestamps(), m_initTime, m_backupDirectory);
    }

    /**
     * Initializes a backup range after recovery (creating a new one or transferring the old)
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_owner
     *         the Chunks' owner
     * @return whether the backup range is initialized or not
     */
    boolean incomingInitRecoveredBackupRange(final short p_rangeID, final short p_owner, final short p_originalRangeID,
            final short p_originalOwner, final boolean p_isNewBackupRange) {
        return m_logHandler
                .createRecoveredBackupRange(p_rangeID, p_owner, p_originalRangeID, p_originalOwner, p_isNewBackupRange,
                        m_secondaryLogSize, (int) getConfig().getLogSegmentSize().getBytes(),
                        (int) getConfig().getSecondaryLogBufferSize().getBytes(),
                        (int) getConfig().getFlashPageSize().getBytes(),
                        getConfig().getUtilizationPromptReorganization(), getConfig().isUseChecksums(),
                        getConfig().isUseTimestamps(), m_initTime, m_backupDirectory);
    }

    /**
     * This is a special receiver message. To avoid creating and deserializing the message,
     * the message header is passed here directly (if complete, split messages are handled normally).
     *
     * @param p_messageHeader
     *         the message header (the payload is yet to be deserialized)
     */
    void incomingLogChunks(final MessageHeader p_messageHeader) {
        m_writeBufferHandler.postData(p_messageHeader);
    }

    /**
     * Logs a buffer with Chunks on SSD
     *
     * @param p_owner
     *         the Chunks' owner
     * @param p_rangeID
     *         the RangeID
     * @param p_numberOfDataStructures
     *         the number of data structures stored in p_buffer
     * @param p_buffer
     *         the Chunk buffer
     */
    void incomingLogChunks(final short p_owner, final short p_rangeID, final int p_numberOfDataStructures,
            final ByteBuffer p_buffer) {
        m_writeBufferHandler.postData(p_owner, p_rangeID, p_numberOfDataStructures, p_buffer);
    }

    /**
     * Removes Chunks from log
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_owner
     *         the Chunks' owner
     * @param p_chunkIDs
     *         the ChunkIDs of all to be deleted chunks
     */

    void incomingRemoveChunks(final short p_rangeID, final short p_owner, final long[] p_chunkIDs) {
        m_versionHandler.invalidateChunks(p_chunkIDs, p_owner, p_rangeID);
    }

    /**
     * Returns the current utilization of primary log and all secondary logs
     *
     * @return the current utilization
     */
    String getCurrentUtilization() {
        String ret;

        if (m_loggingIsActive) {
            ret = m_logHandler.getCurrentUtilization();
        } else {
            ret = "Backup is deactivated!\n";
        }

        return ret;
    }

}

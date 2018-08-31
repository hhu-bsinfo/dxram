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
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.MessageHeader;
import de.hhu.bsinfo.dxnet.core.MessageImporterDefault;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.log.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.ChecksumHandler;
import de.hhu.bsinfo.dxram.log.messages.InitBackupRangeRequest;
import de.hhu.bsinfo.dxram.log.messages.InitBackupRangeResponse;
import de.hhu.bsinfo.dxram.log.messages.InitRecoveredBackupRangeRequest;
import de.hhu.bsinfo.dxram.log.messages.InitRecoveredBackupRangeResponse;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.LogCatalog;
import de.hhu.bsinfo.dxram.log.storage.PrimaryLog;
import de.hhu.bsinfo.dxram.log.storage.PrimaryWriteBuffer;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLog;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLogBuffer;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLogsReorgThread;
import de.hhu.bsinfo.dxram.log.storage.TemporaryVersionsStorage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.recovery.RecoveryMetadata;
import de.hhu.bsinfo.dxram.util.HarddriveAccessMode;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.ByteBufferHelper;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.jni.JNIFileRaw;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;

/**
 * This service provides access to the backend storage system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class LogComponent extends AbstractDXRAMComponent<LogComponentConfig> {
    private static final TimePool SOP_LOG_BATCH = new TimePool(LogComponent.class, "LogBatch");
    private static final TimePool SOP_PUT_ENTRY_AND_HEADER = new TimePool(LogComponent.class, "PutEntryAndHeader");

    public static final boolean TWO_LEVEL_LOGGING_ACTIVATED = true;
    private static final int RECOVERY_THREADS = 4;

    static {
        StatisticsManager.get().registerOperation(LogComponent.class, SOP_LOG_BATCH);
        StatisticsManager.get().registerOperation(LogComponent.class, SOP_PUT_ENTRY_AND_HEADER);
    }

    private HarddriveAccessMode m_mode;

    // component dependencies
    private NetworkComponent m_network;
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private ChunkBackupComponent m_chunk;

    // private state
    private short m_nodeID;
    private boolean m_loggingIsActive;
    private long m_initTime;

    private long m_secondaryLogSize;

    private PrimaryWriteBuffer m_writeBuffer;
    private PrimaryLog m_primaryLog;
    private LogCatalog[] m_logCatalogs;

    private ReentrantReadWriteLock m_secondaryLogCreationLock;

    private SecondaryLogsReorgThread m_secondaryLogsReorgThread;

    private ReentrantLock m_flushLock;

    private String m_backupDirectory;

    private TemporaryVersionsStorage m_versionsForRecovery;
    private DirectByteBufferWrapper[] m_byteBuffersForRecovery;

    /**
     * Creates the log component
     */
    public LogComponent() {
        super(DXRAMComponentOrder.Init.LOG, DXRAMComponentOrder.Shutdown.LOG, LogComponentConfig.class);
    }

    /**
     * Get the segment size of the log
     *
     * @return Segment size of log in bytes
     */
    public int getSegmentSizeBytes() {
        return (int) getConfig().getLogSegmentSize().getBytes();
    }

    /**
     * Returns the Secondary Logs Reorganization Thread
     *
     * @return the instance of SecondaryLogsReorgThread
     */
    public SecondaryLogsReorgThread getReorganizationThread() {
        return m_secondaryLogsReorgThread;
    }

    /**
     * Returns the header size
     *
     * @param p_chunk
     *         the AbstractChunk
     * @return the header size
     */
    public short getApproxHeaderSize(final AbstractChunk p_chunk) {
        return getApproxHeaderSize(ChunkID.getCreatorID(p_chunk.getID()),
                ChunkID.getLocalID(p_chunk.getID()), p_chunk.sizeofObject());
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
     * Removes the secondary log and buffer for given backup range
     *
     * @param p_owner
     *         the owner of the backup range
     * @param p_rangeID
     *         the RangeID
     */
    public void removeBackupRange(final short p_owner, final short p_rangeID) {
        LogCatalog cat;

        // Can be executed by application/network thread or writer thread
        m_secondaryLogCreationLock.writeLock().lock();
        cat = m_logCatalogs[p_owner & 0xFFFF];

        if (cat != null) {
            try {
                cat.removeAndCloseBufferAndLog(p_rangeID);
            } catch (IOException e) {

                LOGGER.trace("Backup range could not be removed from hard drive.");

            }
        }
        m_secondaryLogCreationLock.writeLock().unlock();
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
        RecoveryMetadata ret = null;
        SecondaryLogBuffer secLogBuffer;
        SecondaryLog secLog;

        try {
            long start = System.currentTimeMillis();
            m_secondaryLogsReorgThread.block();
            long timeToGetLock = System.currentTimeMillis() - start;

            secLogBuffer = getSecondaryLogBuffer(p_owner, p_rangeID);
            secLog = getSecondaryLog(p_owner, p_rangeID);
            if (secLogBuffer != null && secLog != null) {
                // Read all versions for recovery (must be done before flushing)
                long time = System.currentTimeMillis();
                if (m_versionsForRecovery == null) {
                    m_versionsForRecovery = new TemporaryVersionsStorage(m_secondaryLogSize);
                } else {
                    m_versionsForRecovery.clear();
                }
                long lowestCID = secLog.getCurrentVersions(m_versionsForRecovery, false);
                long timeToReadVersions = System.currentTimeMillis() - time;

                flushDataToPrimaryLog();
                secLogBuffer.flushSecLogBuffer();
                ret = secLog.recoverFromLog(m_byteBuffersForRecovery, RECOVERY_THREADS, m_versionsForRecovery,
                        lowestCID, timeToGetLock, timeToReadVersions, m_chunk, true);
            } else {

                LOGGER.error("Backup range %d could not be recovered. Secondary log is missing!", p_rangeID);

            }
        } catch (final IOException | InterruptedException e) {

            LOGGER.error("Backup range recovery failed: %s", e);

        } finally {
            m_secondaryLogsReorgThread.unblock();
        }

        return ret;
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
            ret = SecondaryLog.recoverFromFile(p_fileName, p_path, getConfig().useChecksums(), m_secondaryLogSize,
                    (int) getConfig().getLogSegmentSize().getBytes(), m_mode);
        } catch (final IOException e) {

            LOGGER.error("Could not recover from file %s: %s", p_path, e);

        }

        return ret;
    }

    /**
     * Returns the secondary log buffer
     *
     * @param p_owner
     *         the owner NodeID
     * @param p_rangeID
     *         the RangeID
     * @return the secondary log buffer
     */
    public SecondaryLogBuffer getSecondaryLogBuffer(final short p_owner, final short p_rangeID) {
        SecondaryLogBuffer ret = null;
        LogCatalog cat;

        // Can be executed by application/network thread or writer thread
        m_secondaryLogCreationLock.readLock().lock();
        cat = m_logCatalogs[p_owner & 0xFFFF];

        if (cat != null) {
            ret = cat.getBuffer(p_rangeID);
        }
        m_secondaryLogCreationLock.readLock().unlock();

        return ret;
    }

    /**
     * Flushes all secondary log buffers
     *
     * @throws IOException
     *         if at least one secondary log could not be flushed
     * @throws InterruptedException
     *         if caller is interrupted
     */
    public void flushDataToSecondaryLogs() throws IOException, InterruptedException {
        LogCatalog cat;
        SecondaryLogBuffer[] buffers;

        if (m_flushLock.tryLock()) {
            try {
                for (int i = 0; i < Short.MAX_VALUE * 2 + 1; i++) {
                    cat = m_logCatalogs[i];
                    if (cat != null) {
                        buffers = cat.getAllBuffers();
                        for (int j = 0; j < buffers.length; j++) {
                            if (buffers[j] != null && !buffers[j].isBufferEmpty()) {
                                buffers[j].flushSecLogBuffer();
                            }
                        }
                    }
                }
            } finally {
                m_flushLock.unlock();
            }
        } else {
            // Another thread is flushing, wait until it is finished
            do {
                Thread.sleep(100);
            } while (m_flushLock.isLocked());
        }
    }

    /**
     * Returns all log catalogs
     *
     * @return the array of log catalogs
     */
    public LogCatalog[] getAllLogCatalogs() {
        return m_logCatalogs;
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
        m_chunk = p_componentAccessor.getComponent(ChunkBackupComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {

        applyConfiguration(getConfig());

        m_loggingIsActive = m_boot.getNodeRole() == NodeRole.PEER && m_backup.isActiveAndAvailableForBackup();
        if (m_loggingIsActive) {
            m_nodeID = m_boot.getNodeId();
            m_backupDirectory = m_backup.getConfig().getBackupDirectory();
            m_secondaryLogSize = m_backup.getConfig().getBackupRangeSize().getBytes() * 2;

            m_flushLock = new ReentrantLock(false);

            // Load jni modules
            DXRAMJNIManager.loadJNIModule("JNINativeCRCGenerator");
            if (m_mode == HarddriveAccessMode.ODIRECT) {
                DXRAMJNIManager.loadJNIModule(HarddriveAccessMode.getJNIFileName(m_mode));
            } else if (m_mode == HarddriveAccessMode.RAW_DEVICE) {
                DXRAMJNIManager.loadJNIModule(HarddriveAccessMode.getJNIFileName(m_mode));
                JNIFileRaw.prepareRawDevice(getConfig().getRawDevicePath(), 0);
            }

            purgeLogDirectory(m_backupDirectory);

            createLogsAndBuffers();

            createAndStartReorganizationThread(m_backup.getConfig().getBackupRangeSize().getBytes(),
                    getConfig().getUtilizationActivateReorganization());
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
     */
    void initComponent(final LogComponentConfig p_config, final String p_backupDir, final int p_backupRangeSize) {
        setConfig(p_config);

        applyConfiguration(p_config);

        m_loggingIsActive = true;
        m_nodeID = (short) 1;
        m_backupDirectory = p_backupDir;
        m_secondaryLogSize = p_backupRangeSize * 2;

        m_flushLock = new ReentrantLock(false);

        // Load jni modules
        final String cwd = System.getProperty("user.dir");
        String path = cwd + "/jni/libJNINativeCRCGenerator.so";
        System.load(path);

        if (m_mode == HarddriveAccessMode.ODIRECT) {
            path = cwd + "/jni/libJNIFileDirect.so";
            System.load(path);
        } else if (m_mode == HarddriveAccessMode.RAW_DEVICE) {
            path = cwd + "/jni/libJNIFileRaw.so";
            System.load(path);
            JNIFileRaw.prepareRawDevice(getConfig().getRawDevicePath(), 0);
        }

        purgeLogDirectory(m_backupDirectory);

        createLogsAndBuffers();

        createAndStartReorganizationThread(p_backupRangeSize, getConfig().getUtilizationActivateReorganization());
    }

    /**
     * Apply configuration.
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
        AbstractLogEntryHeader.setTimestampSize(p_config.useTimestamps());
        // Set the log entry header crc size (must be called before the first log entry header is created)
        ChecksumHandler.setCRCSize(p_config.useChecksums());

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
     * Create all logs and buffers.
     */
    private void createLogsAndBuffers() {
        if (getConfig().getSecondaryLogBufferSize().getBytes() == 0 || !TWO_LEVEL_LOGGING_ACTIVATED) {

            LOGGER.info("Two-level logging is disabled. Performance might be impaired!");

        } else {
            // Create primary log
            try {
                m_primaryLog =
                        new PrimaryLog(this, m_backupDirectory, m_nodeID, getConfig().getPrimaryLogSize().getBytes(),
                                getConfig().useChecksums(), getConfig().useTimestamps(),
                                (int) getConfig().getFlashPageSize().getBytes(), m_mode);
            } catch (final IOException e) {

                LOGGER.error("Primary log creation failed", e);

            }

            LOGGER.trace("Initialized primary log (%d)", (int) getConfig().getLogSegmentSize().getBytes());

        }

        // Create primary log buffer
        m_writeBuffer = new PrimaryWriteBuffer(this, m_primaryLog, (int) getConfig().getWriteBufferSize().getBytes(),
                (int) getConfig().getFlashPageSize().getBytes(),
                (int) getConfig().getSecondaryLogBufferSize().getBytes(),
                (int) getConfig().getLogSegmentSize().getBytes(), getConfig().useChecksums());

        // Create secondary log and secondary log buffer catalogs
        m_logCatalogs = new LogCatalog[Short.MAX_VALUE * 2 + 1];

        m_secondaryLogCreationLock = new ReentrantReadWriteLock(false);

        m_byteBuffersForRecovery = new DirectByteBufferWrapper[RECOVERY_THREADS + 1];
        for (int i = 0; i <= RECOVERY_THREADS; i++) {
            m_byteBuffersForRecovery[i] =
                    new DirectByteBufferWrapper((int) getConfig().getLogSegmentSize().getBytes(), true);
        }
    }

    /**
     * Create and start the secondary logs reorganization thread.
     *
     * @param p_backupRangeSize
     *         the backup range size
     * @param p_utilizationActivateReorganization
     *         the threshold to consider a log for reorganization
     */
    private void createAndStartReorganizationThread(final long p_backupRangeSize,
            final int p_utilizationActivateReorganization) {
        // Create reorganization thread for secondary logs
        m_secondaryLogsReorgThread = new SecondaryLogsReorgThread(this, p_backupRangeSize * 2,
                (int) getConfig().getLogSegmentSize().getBytes(), p_utilizationActivateReorganization);
        m_secondaryLogsReorgThread.setName("Logging: Reorganization Thread");

        // Start secondary logs reorganization thread
        m_secondaryLogsReorgThread.start();
    }

    @Override
    protected boolean shutdownComponent() {
        LogCatalog cat;

        if (m_loggingIsActive) {
            // Close write buffer
            m_writeBuffer.closeWriteBuffer();
            m_writeBuffer = null;

            // Stop reorganization thread
            m_secondaryLogsReorgThread.interrupt();
            m_secondaryLogsReorgThread.shutdown();
            try {
                m_secondaryLogsReorgThread.join();

                LOGGER.info("Shutdown of SecondaryLogsReorgThread successful");
            } catch (final InterruptedException ignored) {
                LOGGER.warn("Could not wait for reorganization thread to finish. Interrupted");
            }

            m_secondaryLogsReorgThread = null;

            // Close primary log
            if (m_primaryLog != null) {
                try {
                    m_primaryLog.close();
                } catch (final IOException ignored) {
                    LOGGER.warn("Could not close primary log!");
                }

                m_primaryLog = null;
            }

            // Clear secondary logs and buffers
            for (int i = 0; i < Short.MAX_VALUE * 2 + 1; i++) {
                try {
                    cat = m_logCatalogs[i];
                    if (cat != null) {
                        cat.closeLogsAndBuffers();
                    }
                } catch (final IOException ignored) {
                    LOGGER.warn("Could not close secondary log buffer %d", i);
                }
            }
            m_logCatalogs = null;
        }

        return true;
    }

    /**
     * Returns the current utilization of primary log and all secondary logs
     *
     * @return the current utilization
     */
    String getCurrentUtilization() {
        StringBuilder ret;
        long allBytesAllocated = 0;
        long allBytesOccupied = 0;
        long counterAllocated;
        long counterOccupied;
        long occupiedInRange;
        SecondaryLog[] secondaryLogs;
        SecondaryLogBuffer[] secLogBuffers;
        LogCatalog cat;

        if (m_loggingIsActive) {
            ret = new StringBuilder(
                    "***********************************************************************\n" + "*Primary log: " +
                            m_primaryLog.getOccupiedSpace() + " bytes\n" +
                            "***********************************************************************\n\n" +
                            "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n" +
                            "+Secondary logs:\n");

            for (int i = 0; i < m_logCatalogs.length; i++) {
                cat = m_logCatalogs[i];
                if (cat != null) {
                    counterAllocated = 0;
                    counterOccupied = 0;
                    ret.append("++Node ").append(NodeID.toHexString((short) i)).append(":\n");
                    secondaryLogs = cat.getAllLogs();
                    secLogBuffers = cat.getAllBuffers();
                    for (int j = 0; j < secondaryLogs.length; j++) {
                        if (secondaryLogs[j] != null) {
                            ret.append("+++Backup range ").append(j).append(": ");
                            if (secondaryLogs[j].isAccessed()) {
                                ret.append("#Active log# ");
                            }

                            counterAllocated +=
                                    secondaryLogs[j].getLogFileSize() + secondaryLogs[j].getVersionsFileSize();
                            occupiedInRange = secondaryLogs[j].getOccupiedSpace();
                            counterOccupied += occupiedInRange;

                            ret.append(occupiedInRange).append(" bytes (in buffer: ")
                                    .append(secLogBuffers[j].getOccupiedSpace()).append(" bytes)\n");
                            ret.append(secondaryLogs[j].getSegmentDistribution()).append('\n');
                        }
                    }
                    ret.append("++Bytes per node: allocated -> ").append(counterAllocated).append(", occupied -> ")
                            .append(counterOccupied).append('\n');
                    allBytesAllocated += counterAllocated;
                    allBytesOccupied += counterOccupied;
                }
            }
            ret.append("Complete size: allocated -> ").append(allBytesAllocated).append(", occupied -> ")
                    .append(allBytesOccupied).append('\n');
            ret.append("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        } else {
            ret = new StringBuilder("Backup is deactivated!\n");
        }

        return ret.toString();
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
        boolean ret = true;
        LogCatalog cat;
        SecondaryLog secLog;

        // Initialize a new backup range created by p_owner
        m_secondaryLogCreationLock.writeLock().lock();
        cat = m_logCatalogs[p_owner & 0xFFFF];
        if (cat == null) {
            cat = new LogCatalog();
            m_logCatalogs[p_owner & 0xFFFF] = cat;
        }
        try {
            if (!cat.exists(p_rangeID)) {
                // Create new secondary log
                secLog = new SecondaryLog(this, m_secondaryLogsReorgThread, p_owner, p_rangeID, m_backupDirectory,
                        m_secondaryLogSize, (int) getConfig().getFlashPageSize().getBytes(),
                        (int) getConfig().getLogSegmentSize().getBytes(),
                        getConfig().getUtilizationPromptReorganization(), getConfig().useChecksums(),
                        getConfig().useTimestamps(), m_initTime, getConfig().getColdDataThreshold(), m_mode);
                // Insert range in log catalog
                cat.insertRange(p_rangeID, secLog, (int) getConfig().getSecondaryLogBufferSize().getBytes(),
                        (int) getConfig().getLogSegmentSize().getBytes());
            }
        } catch (final IOException e) {

            LOGGER.error("Initialization of backup range %d failed: %s", p_rangeID, e);

            ret = false;
        }
        m_secondaryLogCreationLock.writeLock().unlock();

        return ret;
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
        boolean ret = true;
        LogCatalog cat;
        SecondaryLog secLog;

        if (p_isNewBackupRange) {
            // This is a new backup peer determined during recovery (replicas will be sent shortly by p_owner)
            flushDataToPrimaryLog();
            m_secondaryLogCreationLock.writeLock().lock();
            cat = m_logCatalogs[p_owner & 0xFFFF];
            if (cat == null) {
                cat = new LogCatalog();
                m_logCatalogs[p_owner & 0xFFFF] = cat;
            }

            try {
                if (!cat.exists(p_rangeID)) {
                    // Create new secondary log
                    secLog = new SecondaryLog(this, m_secondaryLogsReorgThread, p_owner, p_originalOwner, p_rangeID,
                            m_backupDirectory, m_secondaryLogSize, (int) getConfig().getFlashPageSize().getBytes(),
                            (int) getConfig().getLogSegmentSize().getBytes(),
                            getConfig().getUtilizationPromptReorganization(), getConfig().useChecksums(),
                            getConfig().useTimestamps(), m_initTime, getConfig().getColdDataThreshold(), m_mode);
                    // Insert range in log catalog
                    cat.insertRange(p_rangeID, secLog, (int) getConfig().getSecondaryLogBufferSize().getBytes(),
                            (int) getConfig().getLogSegmentSize().getBytes());
                } else {

                    LOGGER.warn("Transfer of backup range %d from 0x%X to 0x%X failed! Secondary log already exists!",
                            p_originalRangeID, p_originalOwner, p_owner);

                }
            } catch (final IOException e) {

                LOGGER.error("Transfer of backup range %d from 0x%X to 0x%X failed! %s", p_originalRangeID,
                        p_originalOwner, p_owner, e);

                ret = false;
            }
            m_secondaryLogCreationLock.writeLock().unlock();
        } else {
            // Transfer recovered backup range from p_originalOwner to p_owner
            flushDataToPrimaryLog();
            m_secondaryLogCreationLock.writeLock().lock();
            cat = m_logCatalogs[p_originalOwner & 0xFFFF];
            secLog = cat.getLog(p_originalRangeID);
            if (secLog != null) {
                // This is an old backup peer (it has the entire data already)
                try {
                    cat.getBuffer(p_originalRangeID).flushSecLogBuffer();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
                cat.removeBufferAndLog(p_originalRangeID);
                secLog.transferBackupRange(p_owner, p_rangeID);
                cat = m_logCatalogs[p_owner & 0xFFFF];
                if (cat == null) {
                    cat = new LogCatalog();
                    m_logCatalogs[p_owner & 0xFFFF] = cat;
                }

                if (!cat.exists(p_rangeID)) {
                    // Insert range in log catalog
                    cat.insertRange(p_rangeID, secLog, (int) getConfig().getSecondaryLogBufferSize().getBytes(),
                            (int) getConfig().getLogSegmentSize().getBytes());
                } else {

                    LOGGER.warn("Transfer of backup range %d from 0x%X to 0x%X failed! Secondary log already exists!",
                            p_originalRangeID, p_originalOwner, p_owner);

                }
            }
            m_secondaryLogCreationLock.writeLock().unlock();
        }

        return ret;
    }

    /**
     * This is a special receiver message. To avoid creating and deserializing the message,
     * the message header is passed here directly (if complete, split messages are handled normally).
     *
     * @param p_messageHeader
     *         the message header (the payload is yet to be deserialized)
     */
    void incomingLogChunks(final MessageHeader p_messageHeader) {
        long chunkID = ChunkID.INVALID_ID;
        int length = -1;

        MessageImporterDefault importer = new MessageImporterDefault();
        p_messageHeader.initExternalImporter(importer);

        short owner = p_messageHeader.getSource();
        short rangeID = importer.readShort((short) 0);
        int numberOfChunks = importer.readInt(0);

        SOP_LOG_BATCH.start();

        SecondaryLog secLog = getSecondaryLog(owner, rangeID);
        if (secLog == null) {

            LOGGER.error("Logging of chunks failed. SecondaryLog for range %s,%d is missing!", owner, rangeID);

            return;
        }

        int timestamp = 0;
        if (getConfig().useTimestamps()) {
            // Getting the same timestamp for all chunks to be logged
            // This might be a little inaccurate but currentTimeMillis is expensive
            timestamp = (int) ((System.currentTimeMillis() - m_initTime) / 1000);
        }

        short originalOwner = secLog.getOriginalOwner();
        for (int i = 0; i < numberOfChunks; i++) {
            chunkID = importer.readLong(chunkID);
            length = importer.readCompactNumber(length);

            assert length > 0;

            SOP_PUT_ENTRY_AND_HEADER.start();

            m_writeBuffer.putLogData(importer, chunkID, length, rangeID, owner, originalOwner, timestamp, secLog);

            SOP_PUT_ENTRY_AND_HEADER.stop();
        }

        SOP_LOG_BATCH.stop();
    }

    /**
     * Logs a buffer with Chunks on SSD
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_numberOfDataStructures
     *         the number of data structures stored in p_buffer
     * @param p_buffer
     *         the Chunk buffer
     * @param p_owner
     *         the Chunks' owner
     */
    void incomingLogChunks(final short p_rangeID, final int p_numberOfDataStructures, final ByteBuffer p_buffer,
            final short p_owner) {
        long chunkID = ChunkID.INVALID_ID;
        int length = -1;

        SOP_LOG_BATCH.start();

        SecondaryLog secLog = getSecondaryLog(p_owner, p_rangeID);
        if (secLog == null) {

            LOGGER.error("Logging of chunks failed. SecondaryLog for range %s,%d is missing!", p_owner, p_rangeID);

            return;
        }

        MessageImporterDefault importer = new MessageImporterDefault();
        importer.setBuffer(ByteBufferHelper.getDirectAddress(p_buffer), p_buffer.capacity(), 0);
        importer.setNumberOfReadBytes(0);

        short originalOwner = secLog.getOriginalOwner();
        for (int i = 0; i < p_numberOfDataStructures; i++) {
            chunkID = importer.readLong(chunkID);
            length = importer.readCompactNumber(length);

            assert length > 0;

            int timestamp = 0;
            if (getConfig().useTimestamps()) {
                timestamp = (int) ((System.currentTimeMillis() - m_initTime) / 1000);
            }

            SOP_PUT_ENTRY_AND_HEADER.start();

            m_writeBuffer.putLogData(importer, chunkID, length, p_rangeID, p_owner, originalOwner, timestamp, secLog);

            SOP_PUT_ENTRY_AND_HEADER.stop();
        }

        SOP_LOG_BATCH.stop();
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
        long chunkID;
        SecondaryLog secLog;

        for (int i = 0; i < p_chunkIDs.length; i++) {
            chunkID = p_chunkIDs[i];

            secLog = getSecondaryLog(p_owner, p_rangeID);
            if (secLog != null) {
                secLog.invalidateChunk(chunkID);
            } else {
                LOGGER.error("Removing of chunk 0x%X failed: %s. SecondaryLog is missing!", chunkID);
            }
        }
    }

    /**
     * Returns the secondary log
     *
     * @param p_owner
     *         the owner NodeID
     * @param p_rangeID
     *         the RangeID
     * @return the secondary log
     */
    private SecondaryLog getSecondaryLog(final short p_owner, final short p_rangeID) {
        SecondaryLog ret;
        LogCatalog cat;

        // Can be executed by application/network thread or writer thread
        m_secondaryLogCreationLock.readLock().lock();
        cat = m_logCatalogs[p_owner & 0xFFFF];

        if (cat == null) {
            LOGGER.error("Log catalog for peer 0x%X is empty!", p_owner);
            return null;
        }

        ret = cat.getLog(p_rangeID);
        m_secondaryLogCreationLock.readLock().unlock();

        return ret;
    }

    /**
     * Flushes the primary log write buffer
     */
    public void flushDataToPrimaryLog() {
        m_writeBuffer.initiatePriorityFlush();
    }

}

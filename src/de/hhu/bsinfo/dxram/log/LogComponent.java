package de.hhu.bsinfo.dxram.log;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.DefaultPrimLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.MigrationPrimLogEntryHeader;
import de.hhu.bsinfo.dxram.log.messages.InitRequest;
import de.hhu.bsinfo.dxram.log.messages.InitResponse;
import de.hhu.bsinfo.dxram.log.storage.LogCatalog;
import de.hhu.bsinfo.dxram.log.storage.PrimaryLog;
import de.hhu.bsinfo.dxram.log.storage.PrimaryWriteBuffer;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLog;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLogBuffer;
import de.hhu.bsinfo.dxram.log.storage.Version;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.utils.Tools;
import de.hhu.bsinfo.utils.unit.StorageUnit;

/**
 * This service provides access to the backend storage system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class LogComponent extends AbstractDXRAMComponent {

    private static final Logger LOGGER = LogManager.getFormatterLogger(LogService.class.getSimpleName());

    // Constants
    private static final AbstractLogEntryHeader DEFAULT_PRIM_LOG_ENTRY_HEADER = new DefaultPrimLogEntryHeader();
    private static final AbstractLogEntryHeader MIGRATION_PRIM_LOG_ENTRY_HEADER = new MigrationPrimLogEntryHeader();
    private static final int PAYLOAD_PRINT_LENGTH = 128;

    // configuration values
    @Expose
    private boolean m_useChecksum = true;
    @Expose
    private StorageUnit m_flashPageSize = new StorageUnit(4, StorageUnit.KB);
    @Expose
    private StorageUnit m_logSegmentSize = new StorageUnit(8, StorageUnit.MB);
    @Expose
    private StorageUnit m_primaryLogSize = new StorageUnit(256, StorageUnit.MB);
    @Expose
    private StorageUnit m_secondaryLogSize = new StorageUnit(512, StorageUnit.MB);
    @Expose
    private StorageUnit m_writeBufferSize = new StorageUnit(256, StorageUnit.MB);
    @Expose
    private StorageUnit m_secondaryLogBufferSize = new StorageUnit(128, StorageUnit.KB);
    @Expose
    private int m_reorgUtilizationThreshold = 70;
    @Expose
    private boolean m_sortBufferPooling = true;

    // dependent components
    private NetworkComponent m_network;
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private ChunkBackupComponent m_chunk;

    // private state
    private short m_nodeID;
    private boolean m_loggingIsActive;

    private PrimaryWriteBuffer m_writeBuffer;
    private PrimaryLog m_primaryLog;
    private LogCatalog[] m_logCatalogs;

    private ReentrantReadWriteLock m_secondaryLogCreationLock;

    private SecondaryLogsReorgThread m_secondaryLogsReorgThread;

    private ReentrantLock m_flushLock;

    private String m_backupDirectory;

    /**
     * Creates the log component
     */
    public LogComponent() {
        super(DXRAMComponentOrder.Init.LOG, DXRAMComponentOrder.Shutdown.LOG);
    }

    /**
     * Returns the current utilization of primary log and all secondary logs
     *
     * @return the current utilization
     */
    String getCurrentUtilization() {
        String ret;
        long allBytes = 0;
        long counter;
        SecondaryLog[] secondaryLogs;
        SecondaryLogBuffer[] secLogBuffers;
        LogCatalog cat;

        if (m_loggingIsActive) {
            ret =
                "***********************************************************************\n" + "*Primary log: " + m_primaryLog.getOccupiedSpace() + " bytes\n" +
                    "***********************************************************************\n\n" +
                    "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n" + "+Secondary logs:\n";

            for (int i = 0; i < m_logCatalogs.length; i++) {
                cat = m_logCatalogs[i];
                if (cat != null) {
                    counter = 0;
                    ret += "++Node " + (short) i + ":\n";
                    secondaryLogs = cat.getAllCreatorLogs();
                    secLogBuffers = cat.getAllCreatorBuffers();
                    for (int j = 0; j < secondaryLogs.length; j++) {
                        ret += "+++Creator backup range " + j + ": ";
                        if (secondaryLogs[j] != null) {
                            if (secondaryLogs[j].isAccessed()) {
                                ret += "#Active log# ";
                            }
                            ret += secondaryLogs[j].getOccupiedSpace() + " bytes (in buffer: " + secLogBuffers[j].getOccupiedSpace() + " bytes)\n";
                            ret += secondaryLogs[j].getSegmentDistribution() + '\n';
                            counter += secondaryLogs[j].getLogFileSize() + secondaryLogs[j].getVersionsFileSize();
                        }
                    }
                    secondaryLogs = cat.getAllMigrationLogs();
                    secLogBuffers = cat.getAllMigrationBuffers();
                    for (int j = 0; j < secondaryLogs.length; j++) {
                        ret += "+++Migration backup range " + j + ": ";
                        if (secondaryLogs[j] != null) {
                            if (secondaryLogs[j].isAccessed()) {
                                ret += "#Active log# ";
                            }
                            ret += secondaryLogs[j].getOccupiedSpace() + " bytes (in buffer: " + secLogBuffers[j].getOccupiedSpace() + " bytes)\n";
                            ret += secondaryLogs[j].getSegmentDistribution() + '\n';
                            counter += secondaryLogs[j].getLogFileSize() + secondaryLogs[j].getVersionsFileSize();
                        }
                    }
                    ret += "++Bytes per node: " + counter + '\n';
                    allBytes += counter;
                }
            }
            ret += "Complete size: " + allBytes + '\n';
            ret += "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
        } else {
            ret = "Backup is deactivated!\n";
        }

        return ret;
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
     * Returns all log catalogs
     *
     * @return the array of log catalogs
     */
    LogCatalog[] getAllLogCatalogs() {
        return m_logCatalogs;
    }

    /**
     * Prints the metadata of one log entry
     *
     * @param p_nodeID
     *     the NodeID
     * @param p_localID
     *     the LocalID
     * @param p_payload
     *     buffer with payload
     * @param p_offset
     *     offset within buffer
     * @param p_length
     *     length of payload
     * @param p_version
     *     version of chunk
     * @param p_index
     *     index of log entry
     * @param p_logEntryHeader
     *     the log entry header
     */
    private static void printMetadata(final short p_nodeID, final long p_localID, final byte[] p_payload, final int p_offset, final int p_length,
        final Version p_version, final int p_index, final AbstractLogEntryHeader p_logEntryHeader) {
        final long chunkID = ((long) p_nodeID << 48) + p_localID;
        byte[] array;

        try {
            if (p_version.getVersion() != -1) {
                array = new String(Arrays.copyOfRange(p_payload, p_offset + p_logEntryHeader.getHeaderSize(p_payload, p_offset),
                    p_offset + p_logEntryHeader.getHeaderSize(p_payload, p_offset) + PAYLOAD_PRINT_LENGTH)).trim().getBytes();

                if (Tools.looksLikeUTF8(array)) {
                    System.out.println(
                        "Log Entry " + p_index + ": \t ChunkID - " + chunkID + '(' + p_nodeID + ", " + (int) p_localID + ") \t Length - " + p_length +
                            "\t Version - " + p_version.getEpoch() + ',' + p_version.getVersion() + " \t Payload - " + new String(array, "UTF-8"));
                } else {
                    System.out.println(
                        "Log Entry " + p_index + ": \t ChunkID - " + chunkID + '(' + p_nodeID + ", " + (int) p_localID + ") \t Length - " + p_length +
                            "\t Version - " + p_version.getEpoch() + ',' + p_version.getVersion() + " \t Payload is no String");
                }
            } else {
                System.out.println(
                    "Log Entry " + p_index + ": \t ChunkID - " + chunkID + '(' + p_nodeID + ", " + (int) p_localID + ") \t Length - " + p_length +
                        "\t Version - " + p_version.getEpoch() + ',' + p_version.getVersion() + " \t Tombstones have no payload");
            }
        } catch (final UnsupportedEncodingException | IllegalArgumentException ignored) {
            System.out.println(
                "Log Entry " + p_index + ": \t ChunkID - " + chunkID + '(' + p_nodeID + ", " + (int) p_localID + ") \t Length - " + p_length + "\t Version - " +
                    p_version.getEpoch() + ',' + p_version.getVersion() + " \t Payload is no String");
        }
        // p_localID: -1 can only be printed as an int
    }

    /**
     * Returns the header size
     *
     * @param p_nodeID
     *     the NodeID
     * @param p_localID
     *     the LocalID
     * @param p_size
     *     the size of the Chunk
     * @return the header size
     */
    public short getAproxHeaderSize(final short p_nodeID, final long p_localID, final int p_size) {
        return AbstractLogEntryHeader.getApproxSecLogHeaderSize(m_boot.getNodeID() != p_nodeID, p_localID, p_size);
    }

    /**
     * Initializes a new backup range
     *
     * @param p_firstChunkIDOrRangeID
     *     the beginning of the range
     * @param p_backupPeers
     *     the backup peers
     */
    public void initBackupRange(final long p_firstChunkIDOrRangeID, final short[] p_backupPeers) {
        InitRequest request;
        InitResponse response;
        long time;

        time = System.currentTimeMillis();
        if (p_backupPeers != null) {
            for (int i = 0; i < p_backupPeers.length && p_backupPeers[i] != -1; i++) {
                if (ChunkID.getCreatorID(p_firstChunkIDOrRangeID) != -1) {
                    request = new InitRequest(p_backupPeers[i], p_firstChunkIDOrRangeID, ChunkID.getCreatorID(p_firstChunkIDOrRangeID));
                } else {
                    request = new InitRequest(p_backupPeers[i], p_firstChunkIDOrRangeID, m_boot.getNodeID());
                }

                try {
                    m_network.sendSync(request);
                } catch (final NetworkException ignore) {
                    i--;
                    continue;
                }

                response = request.getResponse(InitResponse.class);

                if (!response.getStatus()) {
                    i--;
                }
            }
        }
        // #if LOGGER == TRACE
        LOGGER.trace("Time to initialize range: %d", System.currentTimeMillis() - time);
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Recovers all Chunks of given backup range
     *
     * @param p_owner
     *     the NodeID of the node whose Chunks have to be restored
     * @param p_chunkID
     *     the ChunkID
     * @param p_rangeID
     *     the RangeID
     * @return the recovered Chunks
     */
    public int recoverBackupRange(final short p_owner, final long p_chunkID, final byte p_rangeID) {
        int ret = 0;
        SecondaryLogBuffer secondaryLogBuffer;

        try {
            flushDataToPrimaryLog();

            secondaryLogBuffer = getSecondaryLogBuffer(p_chunkID, p_owner, p_rangeID);
            if (secondaryLogBuffer != null) {
                secondaryLogBuffer.flushSecLogBuffer();
                ret = getSecondaryLog(p_chunkID, p_owner, p_rangeID).recoverFromLog(m_chunk, true);
            }
        } catch (final IOException | InterruptedException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Backup range recovery failed: %s", e);
            // #endif /* LOGGER >= ERROR */
        }

        return ret;
    }

    /**
     * Recovers all Chunks of given backup range
     *
     * @param p_fileName
     *     the file name
     * @param p_path
     *     the path of the folder the file is in
     * @return the recovered Chunks
     */
    public Chunk[] recoverBackupRangeFromFile(final String p_fileName, final String p_path) {
        Chunk[] ret = null;

        try {
            ret = SecondaryLog.recoverFromFile(p_fileName, p_path, m_useChecksum, m_secondaryLogSize.getBytes(), (int) m_logSegmentSize.getBytes());
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not recover from file %s: %s", p_path, e);
            // #endif /* LOGGER >= ERROR */
        }

        return ret;
    }

    /**
     * Returns the secondary log buffer
     *
     * @param p_chunkID
     *     the ChunkID
     * @param p_source
     *     the source NodeID
     * @param p_rangeID
     *     the RangeID for migrations or -1
     * @return the secondary log buffer
     * @throws IOException
     *     if the secondary log buffer could not be returned
     * @throws InterruptedException
     *     if the secondary log buffer could not be returned
     */
    public SecondaryLogBuffer getSecondaryLogBuffer(final long p_chunkID, final short p_source, final byte p_rangeID) throws IOException, InterruptedException {
        SecondaryLogBuffer ret = null;
        LogCatalog cat;

        // Can be executed by application/network thread or writer thread
        m_secondaryLogCreationLock.readLock().lock();
        if (p_rangeID == -1) {
            cat = m_logCatalogs[ChunkID.getCreatorID(p_chunkID) & 0xFFFF];
        } else {
            cat = m_logCatalogs[p_source & 0xFFFF];
        }

        if (cat != null) {
            ret = cat.getBuffer(p_chunkID, p_rangeID);
        }
        m_secondaryLogCreationLock.readLock().unlock();

        return ret;
    }

    /**
     * Returns the backup range
     *
     * @param p_chunkID
     *     the ChunkID
     * @return the first ChunkID of the range
     */
    public long getBackupRange(final long p_chunkID) {
        long ret;
        LogCatalog cat;

        // Can be executed by application/network thread or writer thread
        m_secondaryLogCreationLock.readLock().lock();

        cat = m_logCatalogs[ChunkID.getCreatorID(p_chunkID) & 0xFFFF];
        ret = cat.getRange(p_chunkID);
        m_secondaryLogCreationLock.readLock().unlock();

        return (p_chunkID & 0xFFFF000000000000L) + ret;
    }

    /**
     * Grant the writer thread access to write buffer
     */
    public void grantAccessToWriterThread() {
        m_writeBuffer.grantAccessToWriterThread();
    }

    /**
     * Flushes all secondary log buffers
     *
     * @throws IOException
     *     if at least one secondary log could not be flushed
     * @throws InterruptedException
     *     if caller is interrupted
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

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkBackupComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {

        m_loggingIsActive = m_boot.getNodeRole() == NodeRole.PEER && m_backup.isActive();
        if (m_loggingIsActive) {

            m_nodeID = m_boot.getNodeID();

            m_backupDirectory = m_backup.getBackupDirectory();

            // Set the log entry header crc size (must be called before the first log entry header is created)
            AbstractLogEntryHeader.setCRCSize(m_useChecksum);

            // Create primary log
            try {
                m_primaryLog = new PrimaryLog(this, m_backupDirectory, m_nodeID, m_primaryLogSize.getBytes(), (int) m_flashPageSize.getBytes());
            } catch (final IOException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Primary log creation failed", e);
                // #endif /* LOGGER >= ERROR */
            }
            // #if LOGGER == TRACE
            LOGGER.trace("Initialized primary log (%d)", m_primaryLogSize);
            // #endif /* LOGGER == TRACE */

            // Create reorganization thread for secondary logs
            m_secondaryLogsReorgThread = new SecondaryLogsReorgThread(this, m_secondaryLogSize.getBytes(), (int) m_logSegmentSize.getBytes());
            m_secondaryLogsReorgThread.setName("Logging: Reorganization Thread");

            // Create primary log buffer
            m_writeBuffer = new PrimaryWriteBuffer(this, m_primaryLog, (int) m_writeBufferSize.getBytes(), (int) m_flashPageSize.getBytes(),
                (int) m_secondaryLogBufferSize.getBytes(), (int) m_logSegmentSize.getBytes(), m_useChecksum, m_sortBufferPooling);

            // Create secondary log and secondary log buffer catalogs
            m_logCatalogs = new LogCatalog[Short.MAX_VALUE * 2 + 1];

            m_secondaryLogCreationLock = new ReentrantReadWriteLock(false);

            // Start secondary logs reorganization thread
            m_secondaryLogsReorgThread.start();

            m_flushLock = new ReentrantLock(false);
        }

        return true;
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
                // #if LOGGER >= INFO
                LOGGER.info("Shutdown of SecondaryLogsReorgThread successful");
                // #endif /* LOGGER >= INFO */
            } catch (final InterruptedException e1) {
                // #if LOGGER >= WARN
                LOGGER.warn("Could not wait for reorganization thread to finish. Interrupted");
                // #endif /* LOGGER >= WARN */
            }
            m_secondaryLogsReorgThread = null;

            // Close primary log
            if (m_primaryLog != null) {
                try {
                    m_primaryLog.closeLog();
                } catch (final IOException e) {
                    // #if LOGGER >= WARN
                    LOGGER.warn("Could not close primary log!");
                    // #endif /* LOGGER >= WARN */
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
                } catch (final IOException e) {
                    // #if LOGGER >= WARN
                    LOGGER.warn("Could not close secondary log buffer %d", i);
                    // #endif /* LOGGER >= WARN */
                }
            }
            m_logCatalogs = null;
        }

        return true;
    }

    /**
     * Removes Chunks from log
     *
     * @param p_firstChunkIDOrRangeID
     *     the first ChunkID of the range or the RangeID (for migrations)
     * @param p_owner
     *     the Chunks' owner
     * @return whether the operation was successful or not
     */
    boolean initBackupRange(final long p_firstChunkIDOrRangeID, final short p_owner) {
        boolean ret = true;
        LogCatalog cat;
        SecondaryLog secLog;

        m_secondaryLogCreationLock.writeLock().lock();
        cat = m_logCatalogs[p_owner & 0xFFFF];
        if (cat == null) {
            cat = new LogCatalog();
            m_logCatalogs[p_owner & 0xFFFF] = cat;
        }
        try {
            if (p_owner == ChunkID.getCreatorID(p_firstChunkIDOrRangeID)) {
                if (!cat.exists(p_firstChunkIDOrRangeID, (byte) -1)) {
                    // Create new secondary log
                    secLog =
                        new SecondaryLog(this, m_secondaryLogsReorgThread, p_owner, ChunkID.getLocalID(p_firstChunkIDOrRangeID), cat.getNewID(false), false,
                            m_backupDirectory, m_secondaryLogSize.getBytes(), (int) m_flashPageSize.getBytes(), (int) m_logSegmentSize.getBytes(),
                            m_reorgUtilizationThreshold, m_useChecksum);
                    // Insert range in log catalog
                    cat.insertRange(p_firstChunkIDOrRangeID, secLog, (int) m_secondaryLogBufferSize.getBytes(), (int) m_logSegmentSize.getBytes());
                }
            } else {
                if (!cat.exists(-1, (byte) p_firstChunkIDOrRangeID)) {
                    // Create new secondary log for migrations
                    secLog = new SecondaryLog(this, m_secondaryLogsReorgThread, p_owner, p_firstChunkIDOrRangeID, cat.getNewID(true), true, m_backupDirectory,
                        m_secondaryLogSize.getBytes(), (int) m_flashPageSize.getBytes(), (int) m_logSegmentSize.getBytes(), m_reorgUtilizationThreshold,
                        m_useChecksum);
                    // Insert range in log catalog
                    cat.insertRange(p_firstChunkIDOrRangeID, secLog, (int) m_secondaryLogBufferSize.getBytes(), (int) m_logSegmentSize.getBytes());
                }
            }
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Initialization of backup range %d failed: %s", p_firstChunkIDOrRangeID, e);
            // #endif /* LOGGER >= ERROR */
            ret = false;
        }
        m_secondaryLogCreationLock.writeLock().unlock();

        return ret;
    }

    /**
     * Logs a buffer with Chunks on SSD
     *
     * @param p_buffer
     *     the Chunk buffer
     * @param p_owner
     *     the Chunks' owner
     */
    void logChunks(final ByteBuffer p_buffer, final short p_owner) {
        long chunkID;
        int length;
        byte[] logEntryHeader;
        final byte rangeID = p_buffer.get();
        final int size = p_buffer.getInt();
        SecondaryLog secLog;

        for (int i = 0; i < size; i++) {
            chunkID = p_buffer.getLong();
            length = p_buffer.getInt();

            assert length > 0;

            try {
                secLog = getSecondaryLog(chunkID, p_owner, rangeID);
                if (rangeID == -1) {
                    logEntryHeader = DEFAULT_PRIM_LOG_ENTRY_HEADER.createLogEntryHeader(chunkID, length, secLog.getNextVersion(chunkID), (byte) -1, (short) -1);
                } else {
                    logEntryHeader = MIGRATION_PRIM_LOG_ENTRY_HEADER.createLogEntryHeader(chunkID, length, secLog.getNextVersion(chunkID), rangeID, p_owner);
                }

                m_writeBuffer.putLogData(logEntryHeader, p_buffer, length);
            } catch (final InterruptedException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Logging of chunk 0x%X failed: %s", chunkID, e);
                // #endif /* LOGGER >= ERROR */
            }
        }
    }

    /**
     * Removes Chunks from log
     *
     * @param p_buffer
     *     the ChunkID buffer
     * @param p_owner
     *     the Chunks' owner
     */
    void removeChunks(final ByteBuffer p_buffer, final short p_owner) {
        long chunkID;
        final byte rangeID = p_buffer.get();
        final int size = p_buffer.getInt();

        for (int i = 0; i < size; i++) {
            chunkID = p_buffer.getLong();

            getSecondaryLog(chunkID, p_owner, rangeID).invalidateChunk(chunkID);
        }
    }

    /**
     * Returns the secondary log
     *
     * @param p_chunkID
     *     the ChunkID
     * @param p_source
     *     the source NodeID
     * @param p_rangeID
     *     the RangeID for migrations or -1
     * @return the secondary log
     */
    private SecondaryLog getSecondaryLog(final long p_chunkID, final short p_source, final byte p_rangeID) {
        SecondaryLog ret;
        LogCatalog cat;

        // Can be executed by application/network thread or writer thread
        m_secondaryLogCreationLock.readLock().lock();
        if (p_rangeID == -1) {
            cat = m_logCatalogs[ChunkID.getCreatorID(p_chunkID) & 0xFFFF];
        } else {
            cat = m_logCatalogs[p_source & 0xFFFF];
        }
        ret = cat.getLog(p_chunkID, p_rangeID);
        m_secondaryLogCreationLock.readLock().unlock();

        return ret;
    }

    /**
     * Flushes the primary log write buffer
     */
    private void flushDataToPrimaryLog() {
        m_writeBuffer.signalWriterThreadAndFlushToPrimLog();
    }

    /**
     * Reads the local data of one log
     *
     * @param p_owner
     *     the NodeID
     * @param p_chunkID
     *     the ChunkID
     * @param p_rangeID
     *     the RangeID
     * @return the local data
     * @note for testing only
     */
    private byte[][] readBackupRange(final short p_owner, final long p_chunkID, final byte p_rangeID) {
        byte[][] ret = null;
        SecondaryLogBuffer secondaryLogBuffer;

        try {
            flushDataToPrimaryLog();
            flushDataToSecondaryLogs();

            secondaryLogBuffer = getSecondaryLogBuffer(p_chunkID, p_owner, p_rangeID);
            if (secondaryLogBuffer != null) {
                secondaryLogBuffer.flushSecLogBuffer();

                ret = getSecondaryLog(p_chunkID, p_owner, p_rangeID).readAllSegments();
            }
        } catch (final IOException | InterruptedException ignored) {
        }

        return ret;
    }

    /**
     * Prints the metadata of one node's log
     *
     * @param p_owner
     *     the NodeID
     * @param p_chunkID
     *     the ChunkID
     * @param p_rangeID
     *     the RangeID
     * @note for testing only
     */
    private void printBackupRange(final short p_owner, final long p_chunkID, final byte p_rangeID) {
        byte[][] segments;
        int i = 0;
        int j = 1;
        int readBytes;
        int length;
        int offset = 0;
        long chunkID;
        Version version;
        AbstractLogEntryHeader logEntryHeader;

        segments = readBackupRange(p_owner, p_chunkID, p_rangeID);
        if (segments != null) {
            System.out.println();
            System.out.println("NodeID: " + p_owner);
            while (segments[i] != null) {
                System.out.println("Segment " + i + ": " + segments[i].length);
                readBytes = offset;
                offset = 0;
                while (readBytes < segments[i].length) {
                    logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(segments[i], readBytes, p_owner != ChunkID.getCreatorID(p_chunkID));
                    chunkID = logEntryHeader.getCID(segments[i], readBytes);
                    length = logEntryHeader.getLength(segments[i], readBytes);
                    version = logEntryHeader.getVersion(segments[i], readBytes);
                    printMetadata(ChunkID.getCreatorID(p_chunkID), chunkID, segments[i], readBytes, length, version, j++, logEntryHeader);
                    readBytes += length + logEntryHeader.getHeaderSize(segments[i], readBytes);
                }
                i++;
            }
        }
    }

}
package de.hhu.bsinfo.dxram.log.storage.logs.secondarylog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkByteBuffer;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.log.storage.BackupRangeCatalog;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.Scheduler;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.header.ChecksumHandler;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.TemporaryVersionsStorage;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.Version;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionHandler;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionSorter;
import de.hhu.bsinfo.dxram.recovery.RecoveryMetadata;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * To recover a log with the help of online data structures (secondary log and secondary log buffer).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
public final class LogRecoveryHandler {

    private static final int RECOVERY_THREADS = 4;
    private static final boolean DO_CRC_CHECK = true;

    private static final Logger LOGGER = LogManager.getFormatterLogger(LogRecoveryHandler.class.getSimpleName());

    private static final TimePool SOP_DETERMINE_RANGES = new TimePool(LogRecoveryHandler.class, "DetermineRanges");
    private static final TimePool SOP_READ_SEGMENT = new TimePool(LogRecoveryHandler.class, "ReadSegments");
    private static final TimePool SOP_VALIDATE_CHUNKS = new TimePool(LogRecoveryHandler.class, "ValidateChunks");
    private static final TimePool SOP_PUT_REGULAR_CHUNKS = new TimePool(LogRecoveryHandler.class, "PutRegularChunks");
    private static final TimePool SOP_PUT_LARGE_CHUNKS = new TimePool(LogRecoveryHandler.class, "PutLargeChunks");
    private static final TimePool SOP_GET_ALL_VERSIONS = new TimePool(LogRecoveryHandler.class, "GetAllVersions");
    private static final ValuePool SOP_VERSIONS_FROM_ARRAY =
            new ValuePool(LogRecoveryHandler.class, "VersionsFromArray");
    private static final ValuePool SOP_VERSIONS_FROM_HASH_TABLE =
            new ValuePool(LogRecoveryHandler.class, "VersionsFromHashTable");
    private static final ValuePool SOP_LARGE_CHUNKS = new ValuePool(LogRecoveryHandler.class, "LargeChunks");

    static {
        StatisticsManager.get().registerOperation(LogRecoveryHandler.class, SOP_DETERMINE_RANGES);
        StatisticsManager.get().registerOperation(LogRecoveryHandler.class, SOP_READ_SEGMENT);
        StatisticsManager.get().registerOperation(LogRecoveryHandler.class, SOP_VALIDATE_CHUNKS);
        StatisticsManager.get().registerOperation(LogRecoveryHandler.class, SOP_PUT_REGULAR_CHUNKS);
        StatisticsManager.get().registerOperation(LogRecoveryHandler.class, SOP_PUT_LARGE_CHUNKS);
        StatisticsManager.get().registerOperation(LogRecoveryHandler.class, SOP_GET_ALL_VERSIONS);
        StatisticsManager.get().registerOperation(LogRecoveryHandler.class, SOP_VERSIONS_FROM_ARRAY);
        StatisticsManager.get().registerOperation(LogRecoveryHandler.class, SOP_VERSIONS_FROM_HASH_TABLE);
        StatisticsManager.get().registerOperation(LogRecoveryHandler.class, SOP_LARGE_CHUNKS);
    }

    private final VersionHandler m_versionHandler;
    private final Scheduler m_scheduler;
    private final BackupRangeCatalog m_backupRangeCatalog;

    private final long m_secondaryLogSize;
    private final boolean m_useChecksums;

    private final DirectByteBufferWrapper[] m_byteBuffersForRecovery;
    private TemporaryVersionsStorage m_versionsForRecovery;

    /**
     * Creates an instance of LogRecoveryHandler.
     *
     * @param p_versionHandler
     *         the version handler for getting current versions during the recovery
     * @param p_scheduler
     *         the scheduler to block the reorganization thread and to flush buffers before the recovery is started
     * @param p_backupRangeCatalog
     *         the backup range catalog for getting the corresponding secondary log buffer
     * @param p_secondaryLogSize
     *         the secondary log size
     * @param p_logSegmentSize
     *         the log segment size
     * @param p_useChecksums
     *         whether to use checksums or not
     */
    public LogRecoveryHandler(final VersionHandler p_versionHandler, final Scheduler p_scheduler,
            final BackupRangeCatalog p_backupRangeCatalog, final long p_secondaryLogSize, final int p_logSegmentSize,
            final boolean p_useChecksums) {
        m_versionHandler = p_versionHandler;
        m_scheduler = p_scheduler;
        m_backupRangeCatalog = p_backupRangeCatalog;

        m_secondaryLogSize = p_secondaryLogSize;
        m_useChecksums = p_useChecksums;

        m_byteBuffersForRecovery = new DirectByteBufferWrapper[RECOVERY_THREADS + 1];
        for (int i = 0; i <= RECOVERY_THREADS; i++) {
            m_byteBuffersForRecovery[i] = new DirectByteBufferWrapper(p_logSegmentSize, true);
        }
    }

    /**
     * Closes all recovery components.
     */
    public void close() {
        // Nothing to do here
    }

    /**
     * Recovers a backup range and puts all data to memory management.
     *
     * @param p_owner
     *         the owner
     * @param p_rangeID
     *         the range ID
     * @param p_chunkComponent
     *         the chunk component to access the memory management
     * @return recovery metadata (chunk ID ranges, number of recovered chunks, total size)
     */
    public RecoveryMetadata recoverBackupRange(final short p_owner, final short p_rangeID,
            final ChunkBackupComponent p_chunkComponent) {
        RecoveryMetadata ret = null;
        SecondaryLogBuffer secLogBuffer;

        try {
            m_scheduler.blockReorganizationThread();

            secLogBuffer = m_backupRangeCatalog.getSecondaryLogBuffer(p_owner, p_rangeID);
            if (secLogBuffer != null) {
                // Read all versions for recovery (must be done before flushing)

                SOP_GET_ALL_VERSIONS.start();

                if (m_versionsForRecovery == null) {
                    m_versionsForRecovery = new TemporaryVersionsStorage(m_secondaryLogSize);
                } else {
                    m_versionsForRecovery.clear();
                }
                long lowestCID = m_versionHandler.getCurrentVersions(p_owner, p_rangeID, m_versionsForRecovery, false);

                SOP_GET_ALL_VERSIONS.stop();

                // Flush all data to secondary log prior to the recovery
                m_scheduler.flushWriteBuffer(p_owner, p_rangeID);
                secLogBuffer.flushSecLogBuffer();

                ret = recoverFromLog(secLogBuffer.getLog(), m_byteBuffersForRecovery, m_versionsForRecovery, lowestCID,
                        p_chunkComponent);
            } else {

                LOGGER.error("Backup range %d could not be recovered. Secondary log is missing!", p_rangeID);

            }
        } catch (final IOException e) {
            LOGGER.error("Backup range recovery failed: %s", e);
        } finally {
            m_scheduler.unblockReorganizationThread();
        }

        return ret;
    }

    /**
     * Returns a list with all log entries wrapped in chunks
     *
     * @param p_wrappers
     *         the byte buffers for reading-in the segments (one per recovery thread)
     * @param p_versions
     *         all versions read from SSD
     * @param p_lowestCID
     *         the lowest CID in range
     * @param p_chunkComponent
     *         the ChunkBackupComponent to store recovered chunks
     * @return ChunkIDs of all recovered chunks, number of recovered chunks and bytes
     */
    private RecoveryMetadata recoverFromLog(final SecondaryLog p_secondaryLog,
            final DirectByteBufferWrapper[] p_wrappers, final TemporaryVersionsStorage p_versions,
            final long p_lowestCID, final ChunkBackupComponent p_chunkComponent) {
        SegmentHeader[] segmentHeaders = p_secondaryLog.getSegmentHeaders();
        byte[] index = new byte[segmentHeaders.length];
        ReentrantLock indexLock = new ReentrantLock(false);
        ReentrantLock largeChunkLock = new ReentrantLock(false);
        final RecoveryMetadata recoveryMetadata = new RecoveryMetadata();
        HashMap<Long, ChunkByteBuffer> largeChunks;

        short rangeID = p_secondaryLog.getRangeID();
        short owner = p_secondaryLog.getOwner();
        short originalOwner = p_secondaryLog.getOriginalOwner();

        // FIXME: Recovery fails if versions (partly only?) are stored in hashtable

        if (p_secondaryLog.getOccupiedSpace() == 0) {

            LOGGER.info("Backup range %d is empty. No need for recovery.", rangeID);

            return null;
        }

        if (DO_CRC_CHECK && !m_useChecksums) {

            LOGGER.warn("Unable to check for data corruption as no checksums are stored (configurable)!");
        }

        // HashMap to store large Chunks in
        largeChunks = new HashMap<>();

        if (owner == originalOwner) {
            LOGGER.info("Starting recovery of backup range %d of 0x%X", rangeID, owner);
        } else {
            LOGGER.info("Starting recovery of backup range %d of 0x%X. Original owner: 0x%x", rangeID, owner,
                    originalOwner);
        }

        long time = System.currentTimeMillis();

        // Write Chunks in parallel
        ChunkBackupComponent.RecoveryWriterThread writerThread = p_chunkComponent.initRecoveryThread();

        RecoveryHelperThread[] helperThreads = new RecoveryHelperThread[LogRecoveryHandler.RECOVERY_THREADS];
        for (int i = 0; i < LogRecoveryHandler.RECOVERY_THREADS; i++) {
            RecoveryHelperThread helperThread =
                    new RecoveryHelperThread(recoveryMetadata, p_secondaryLog, p_wrappers[i + 1], p_versions,
                            largeChunks, largeChunkLock, p_lowestCID, index, indexLock, p_chunkComponent);
            helperThread.setName("Recovery: Helper-Thread " + (i + 1));
            helperThread.start();
            helperThreads[i] = helperThread;
        }

        // Determine CID ranges
        SOP_DETERMINE_RANGES.start();
        recoveryMetadata.setChunkIDRanges(VersionSorter.determineRanges(p_versions, p_lowestCID));
        SOP_DETERMINE_RANGES.stop();

        // Recover segments
        int idx = 0;
        while (true) {
            indexLock.lock();
            while (idx < index.length && index[idx] == 1) {
                idx++;
            }
            if (idx == index.length) {
                indexLock.unlock();
                break;
            }
            index[idx] = 1;
            indexLock.unlock();

            if (segmentHeaders[idx] != null && !segmentHeaders[idx].isEmpty()) {
                recoverSegment(p_secondaryLog, idx, p_wrappers[0], p_versions, p_lowestCID, recoveryMetadata,
                        largeChunks, largeChunkLock, p_chunkComponent);
            }
            idx++;
        }

        try {
            for (int i = 0; i < LogRecoveryHandler.RECOVERY_THREADS; i++) {
                helperThreads[i].join();
            }
            while (!writerThread.finished()) {
                Thread.yield();
            }
            writerThread.interrupt();
            writerThread.join();
        } catch (InterruptedException e) {

            LOGGER.error("Interrupt: Could not wait for RecoveryHelperThread/RecoveryWriterThread to finish!");

        }

        SOP_PUT_LARGE_CHUNKS.start();

        if (!largeChunks.isEmpty()) {
            p_chunkComponent.putRecoveredChunks(recoveryMetadata, largeChunks.values().toArray(new ChunkByteBuffer[0]));
        }

        SOP_PUT_LARGE_CHUNKS.stop();

        LOGGER.info("Recovery of backup range finished: ");
        LOGGER.info("\t Recovered %d chunks in %d ms", recoveryMetadata.getNumberOfChunks(),
                System.currentTimeMillis() - time);
        StringBuilder ranges = new StringBuilder("\t ChunkID ranges: ");
        for (long chunkID : recoveryMetadata.getCIDRanges()) {
            ranges.append(ChunkID.toHexString(chunkID)).append(' ');
        }
        LOGGER.info(ranges.toString());

        return recoveryMetadata;
    }

    /**
     * Recover a segment from a normal secondary log
     *
     * @param p_segmentIndex
     *         the index of the segment
     * @param p_wrapper
     *         the byte buffer for reading-in the segments
     * @param p_allVersions
     *         all versions
     * @param p_lowestCID
     *         the lowest CID at the time the versions were read-in
     * @param p_recoveryMetadata
     *         a class to bundle recovery metadata
     * @param p_largeChunks
     *         a HashMap to store large, split chunks
     * @param p_chunkComponent
     *         the ChunkBackupComponent
     */
    static void recoverSegment(final SecondaryLog p_secondaryLog, final int p_segmentIndex,
            final DirectByteBufferWrapper p_wrapper, final TemporaryVersionsStorage p_allVersions,
            final long p_lowestCID, final RecoveryMetadata p_recoveryMetadata,
            final HashMap<Long, ChunkByteBuffer> p_largeChunks, final ReentrantLock p_largeChunkLock,
            final ChunkBackupComponent p_chunkComponent) {
        int headerSize;
        int readBytes = 0;
        int segmentLength;
        int payloadSize;
        int combinedSize = 0;
        long chunkID;
        ByteBuffer segmentData;
        Version currentVersion;
        Version entryVersion;
        AbstractSecLogEntryHeader logEntryHeader;

        short originalOwner = p_secondaryLog.getOriginalOwner();

        try {

            SOP_READ_SEGMENT.start();

            segmentLength = p_secondaryLog.readSegment(p_wrapper, p_segmentIndex);
            segmentData = p_wrapper.getBuffer();

            SOP_READ_SEGMENT.stop();

            if (segmentLength > 0) {

                SOP_VALIDATE_CHUNKS.start();

                int index = 0;
                int length = 100000;
                long[] chunkIDs = new long[length];
                int[] offsets = new int[length];
                int[] lengths = new int[length];

                while (readBytes < segmentLength) {
                    short type = (short) (segmentData.get(readBytes) & 0xFF);
                    logEntryHeader = AbstractSecLogEntryHeader.getHeader(type);
                    headerSize = logEntryHeader.getHeaderSize(type);
                    payloadSize = logEntryHeader.getLength(type, segmentData, readBytes);
                    chunkID = logEntryHeader.getCID(segmentData, readBytes);
                    entryVersion = logEntryHeader.getVersion(type, segmentData, readBytes);

                    // Get current version
                    if (logEntryHeader.isMigrated()) {
                        currentVersion = p_allVersions.get(chunkID);
                        SOP_VERSIONS_FROM_HASH_TABLE.inc();
                    } else {
                        chunkID = ((long) originalOwner << 48) + chunkID;
                        currentVersion = p_allVersions.get(chunkID, p_lowestCID);
                        SOP_VERSIONS_FROM_ARRAY.inc();
                    }

                    if (currentVersion == null || currentVersion.getVersion() == 0) {

                        LOGGER.error("Version unknown for chunk 0x%X! Secondary log: %s", chunkID, p_secondaryLog);

                    } else if (currentVersion.isEqual(entryVersion)) {
                        // Compare current version with element
                        // Create chunk only if log entry complete
                        if (DO_CRC_CHECK) {
                            if (ChecksumHandler
                                    .calculateChecksumOfPayload(p_wrapper, readBytes + headerSize, payloadSize) !=
                                    logEntryHeader.getChecksum(type, segmentData, readBytes)) {

                                LOGGER.error("Corrupt data. Could not recover 0x%X!", chunkID);

                                readBytes += headerSize + payloadSize;
                                continue;
                            }
                        }

                        if (logEntryHeader.isChained(type)) {
                            byte chainID = logEntryHeader.getChainID(type, segmentData, readBytes);
                            byte chainSize = logEntryHeader.getChainSize(type, segmentData, readBytes);
                            int maxLogEntrySize = AbstractLogEntryHeader.getMaxLogEntrySize();

                            p_largeChunkLock.lock();
                            ChunkByteBuffer chunk = p_largeChunks.get(chunkID);
                            if (chunk == null) {
                                // This is the first segment for this ChunkID -> create an array large
                                // enough for holding all data for this chunk
                                if (chainID == chainSize - 1) {
                                    // This is the last chain link -> complete size is known
                                    chunk = new ChunkByteBuffer(chunkID,
                                            (chainSize - 1) * maxLogEntrySize + payloadSize);
                                    p_largeChunks.put(chunkID, chunk);
                                } else {
                                    // This is another chain link -> maximum size is known, only
                                    // -> must be truncated later
                                    chunk = new ChunkByteBuffer(chunkID, chainSize * maxLogEntrySize);
                                    p_largeChunks.put(chunkID, chunk);
                                }

                                SOP_LARGE_CHUNKS.inc();
                            }

                            ByteBuffer buffer = chunk.getData();
                            if (chainID == chainSize - 1 && payloadSize != maxLogEntrySize) {
                                buffer.limit((chainSize - 1) * maxLogEntrySize + payloadSize);
                            }

                            buffer.position(chainID * maxLogEntrySize);
                            segmentData.position(readBytes + headerSize);
                            segmentData.limit(segmentData.position() + payloadSize);
                            buffer.put(segmentData);
                            p_largeChunkLock.unlock();

                        } else {
                            // Put chunks in memory
                            if (index < length) {
                                chunkIDs[index] = chunkID;
                                offsets[index] = readBytes + headerSize;
                                lengths[index] = payloadSize;
                                combinedSize += headerSize + payloadSize;

                                index++;
                            } else {

                                SOP_PUT_REGULAR_CHUNKS.start();

                                if (!p_chunkComponent
                                        .putRecoveredChunks(chunkIDs, p_wrapper.getAddress(), offsets, lengths,
                                                length)) {

                                    LOGGER.error("Memory management failure. Could not recover chunks!");

                                }

                                SOP_PUT_REGULAR_CHUNKS.stop();

                                p_recoveryMetadata.add(length, combinedSize);
                                combinedSize = 0;
                                chunkIDs = new long[length];
                                chunkIDs[0] = chunkID;
                                offsets = new int[length];
                                offsets[0] = readBytes + headerSize;
                                lengths = new int[length];
                                lengths[0] = payloadSize;
                                index = 1;
                            }
                        }
                    } else {
                        // Version, epoch and/or eon is different -> ignore entry
                    }
                    readBytes += headerSize + payloadSize;
                }

                SOP_VALIDATE_CHUNKS.stop();

                // Put other chunks in memory
                if (index != 0) {

                    SOP_PUT_REGULAR_CHUNKS.start();

                    if (!p_chunkComponent
                            .putRecoveredChunks(chunkIDs, p_wrapper.getAddress(), offsets, lengths, index)) {

                        LOGGER.error("Memory management failure. Could not recover chunks!");

                    }

                    SOP_PUT_REGULAR_CHUNKS.stop();

                    p_recoveryMetadata.add(index, combinedSize);
                }
            }
        } catch (final IOException e) {

            LOGGER.error("Recovery failed(%d): ", p_secondaryLog.getRangeID(), e);

        }
    }
}

/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkBackupComponent.RecoveryWriterThread;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DSByteBuffer;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.ChecksumHandler;
import de.hhu.bsinfo.dxram.recovery.RecoveryMetadata;
import de.hhu.bsinfo.dxram.util.ArrayListLong;
import de.hhu.bsinfo.dxram.util.HarddriveAccessMode;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.RandomUtils;
import de.hhu.bsinfo.dxutils.jni.JNIFileDirect;
import de.hhu.bsinfo.dxutils.jni.JNIFileRaw;

/**
 * This class implements the secondary log
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 23.10.2014
 */
public class SecondaryLog extends AbstractLog {

    private static final Logger LOGGER = LogManager.getFormatterLogger(SecondaryLog.class.getSimpleName());

    // Constants
    private static final String SECLOG_PREFIX_FILENAME = "sec";
    private static final String SECLOG_POSTFIX_FILENAME = ".log";
    private static final int RECOVERY_THREADS = 4;
    private static final int SORT_THRESHOLD = 100000;
    private final short m_originalOwner;
    private final short m_originalRangeID;
    private final long m_secondaryLogReorgThreshold;
    private final long m_secondaryLogSize;
    private final int m_logSegmentSize;
    private final boolean m_useChecksums;
    private final boolean m_useTimestamps;
    private final int m_coldDataThreshold;
    private final long m_creationTimestamp;
    // Attributes
    private short m_owner;
    private short m_rangeID;
    private VersionsBuffer m_versionsBuffer;
    private SecondaryLogsReorgThread m_reorganizationThread;
    private SegmentHeader[] m_segmentHeaders;
    private SegmentHeader m_activeSegment;
    private SegmentHeader m_reorgSegment;
    private ReentrantLock m_segmentAssignmentlock;
    private byte[] m_reorgVector;
    private int m_segmentReorgCounter;

    private volatile boolean m_isAccessedByReorgThread;
    private volatile boolean m_isClosed;

    // Constructors

    /**
     * Creates an instance of SecondaryLog with default configuration except
     * secondary log size
     *
     * @param p_logComponent
     *         the log component to enable calling access granting methods in VersionsBuffer
     * @param p_reorganizationThread
     *         the reorganization thread
     * @param p_owner
     *         the NodeID
     * @param p_rangeID
     *         the RangeID
     * @param p_backupDirectory
     *         the backup directory
     * @param p_secondaryLogSize
     *         the size of a secondary log
     * @param p_flashPageSize
     *         the flash page size
     * @param p_logSegmentSize
     *         the segment size
     * @param p_reorgUtilizationThreshold
     *         the threshold size for a secondary size to trigger reorganization
     * @param p_useChecksums
     *         whether checksums are used for recovery
     * @param p_useTimestamps
     *         whether timestamps are used for segment selection
     * @param p_coldDataThreshold
     *         the threshold for cold data detection
     * @param p_mode
     *         the HarddriveAccessMode
     * @throws IOException
     *         if secondary log could not be created
     */
    public SecondaryLog(final LogComponent p_logComponent, final SecondaryLogsReorgThread p_reorganizationThread, final short p_owner,
            final short p_originalOwner, final short p_rangeID, final String p_backupDirectory, final long p_secondaryLogSize, final int p_flashPageSize,
            final int p_logSegmentSize, final int p_reorgUtilizationThreshold, final boolean p_useChecksums, final boolean p_useTimestamps,
            final int p_coldDataThreshold, final HarddriveAccessMode p_mode) throws IOException {
        super(new File(p_backupDirectory + 'N' + NodeID.toHexString(p_owner) + '_' + SECLOG_PREFIX_FILENAME + NodeID.toHexString(p_owner) + '_' + p_rangeID +
                        (p_useChecksums ? "1" : "0") + '_' + (p_useTimestamps ? "1" : "0") + '_' + SECLOG_POSTFIX_FILENAME), p_secondaryLogSize, p_mode,
                p_logSegmentSize, p_flashPageSize);
        if (p_secondaryLogSize < p_flashPageSize) {
            throw new IllegalArgumentException("Error: Secondary log too small");
        }

        m_secondaryLogSize = p_secondaryLogSize;
        m_logSegmentSize = p_logSegmentSize;
        m_useChecksums = p_useChecksums;
        m_useTimestamps = p_useTimestamps;
        m_coldDataThreshold = p_coldDataThreshold;
        m_creationTimestamp = System.currentTimeMillis();

        m_segmentAssignmentlock = new ReentrantLock(false);

        m_owner = p_owner;
        m_rangeID = p_rangeID;
        m_originalOwner = p_originalOwner;
        m_originalRangeID = p_rangeID;

        m_versionsBuffer = new VersionsBuffer(m_originalOwner, p_logComponent, m_secondaryLogSize,
                p_backupDirectory + 'N' + NodeID.toHexString(p_owner) + '_' + SECLOG_PREFIX_FILENAME + NodeID.toHexString(p_owner) + '_' + p_rangeID + ".ver",
                p_mode);

        m_reorganizationThread = p_reorganizationThread;

        m_secondaryLogReorgThreshold = (int) (p_secondaryLogSize * ((double) p_reorgUtilizationThreshold / 100));
        m_segmentReorgCounter = 0;
        m_segmentHeaders = new SegmentHeader[(int) (p_secondaryLogSize / p_logSegmentSize)];
        m_reorgVector = new byte[(int) (p_secondaryLogSize / p_logSegmentSize)];

        m_isClosed = false;

        if (!createLogAndWriteHeader()) {
            throw new IOException("Error: Secondary log " + p_rangeID + " could not be created");
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Initialized secondary log (%d)", m_secondaryLogSize);
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Creates an instance of SecondaryLog with default configuration except
     * secondary log size
     *
     * @param p_logComponent
     *         the log component to enable calling access granting methods in VersionsBuffer
     * @param p_reorganizationThread
     *         the reorganization thread
     * @param p_owner
     *         the NodeID
     * @param p_rangeID
     *         the RangeID
     * @param p_backupDirectory
     *         the backup directory
     * @param p_secondaryLogSize
     *         the size of a secondary log
     * @param p_flashPageSize
     *         the flash page size
     * @param p_logSegmentSize
     *         the segment size
     * @param p_reorgUtilizationThreshold
     *         the threshold size for a secondary size to trigger reorganization
     * @param p_useChecksums
     *         whether checksums are used for recovery
     * @param p_useTimestamps
     *         whether timestamps are used for segment selection
     * @param p_coldDataThreshold
     *         the threshold for cold data detection
     * @param p_mode
     *         the HarddriveAccessMode
     * @throws IOException
     *         if secondary log could not be created
     */
    public SecondaryLog(final LogComponent p_logComponent, final SecondaryLogsReorgThread p_reorganizationThread, final short p_owner, final short p_rangeID,
            final String p_backupDirectory, final long p_secondaryLogSize, final int p_flashPageSize, final int p_logSegmentSize,
            final int p_reorgUtilizationThreshold, final boolean p_useChecksums, final boolean p_useTimestamps, final int p_coldDataThreshold,
            final HarddriveAccessMode p_mode) throws IOException {
        this(p_logComponent, p_reorganizationThread, p_owner, p_owner, p_rangeID, p_backupDirectory, p_secondaryLogSize, p_flashPageSize, p_logSegmentSize,
                p_reorgUtilizationThreshold, p_useChecksums, p_useTimestamps, p_coldDataThreshold, p_mode);
    }

    /**
     * Returns a list with all log entries in file wrapped in chunks
     *
     * @param p_fileName
     *         the file name of the secondary log
     * @param p_path
     *         the path of the directory the file is in
     * @param p_useChecksum
     *         whether checksums are used
     * @param p_secondaryLogSize
     *         the secondary log size
     * @param p_logSegmentSize
     *         the segment size
     * @param p_mode
     *         the harddrive access mode
     * @return ArrayList with all log entries as chunks
     * @throws IOException
     *         if the secondary log could not be read
     */
    public static DataStructure[] recoverFromFile(final String p_fileName, final String p_path, final boolean p_useChecksum, final long p_secondaryLogSize,
            final int p_logSegmentSize, final HarddriveAccessMode p_mode) throws IOException {
        short nodeID;
        int i = 0;
        int offset = 0;
        int logEntrySize;
        int payloadSize;
        int checksum = -1;
        long chunkID;
        boolean storesMigrations;
        DirectByteBufferWrapper[] segments;
        ByteBuffer payload;
        ByteBuffer segment;
        HashMap<Long, DataStructure> chunkMap;
        AbstractSecLogEntryHeader logEntryHeader;

        // FIXME

        nodeID = Short.parseShort(p_fileName.split("_")[0].substring(1));
        storesMigrations = p_fileName.contains("M");

        chunkMap = new HashMap<Long, DataStructure>();

        segments = readAllSegmentsFromFile(p_path + p_fileName, p_secondaryLogSize, p_logSegmentSize, p_mode);

        while (i < segments.length) {
            segment = segments[i].getBuffer();
            if (segment != null) {
                while (offset < segment.capacity() && segment.get(offset) != 0) {
                    // Determine header of next log entry
                    logEntryHeader = AbstractSecLogEntryHeader.getHeader(segment, offset);
                    if (storesMigrations) {
                        chunkID = logEntryHeader.getCID(segment, offset);
                    } else {
                        chunkID = ((long) nodeID << 48) + logEntryHeader.getCID(segment, offset);
                    }
                    payloadSize = logEntryHeader.getLength(segment, offset);
                    if (p_useChecksum) {
                        checksum = logEntryHeader.getChecksum(segment, offset);
                    }
                    logEntrySize = logEntryHeader.getHeaderSize(segment, offset) + payloadSize;

                    // Read payload and create chunk
                    if (offset + logEntrySize <= segment.capacity()) {
                        // Create chunk only if log entry complete
                        payload = ByteBuffer.allocate(payloadSize);
                        segment.position(offset + logEntryHeader.getHeaderSize(segment, offset));
                        segment.limit(segment.position() + payloadSize);
                        payload.put(segment);
                        if (p_useChecksum && ChecksumHandler.calculateChecksumOfPayload(segments[i], 0, payloadSize) != checksum) {
                            // Ignore log entry
                            offset += logEntrySize;
                            continue;
                        }
                        chunkMap.put(chunkID, new DSByteBuffer(chunkID, payload));
                    }
                    offset += logEntrySize;
                }
            }
            offset = 0;
            i++;
        }

        return chunkMap.values().toArray(new DataStructure[chunkMap.size()]);
    }

    /**
     * Returns all segments of secondary log
     *
     * @param p_path
     *         the path of the file
     * @param p_secondaryLogSize
     *         the secondary log size
     * @param p_logSegmentSize
     *         the segment size
     * @param p_mode
     *         the harddrive access mode
     * @return all data
     * @throws IOException
     *         if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    private static DirectByteBufferWrapper[] readAllSegmentsFromFile(final String p_path, final long p_secondaryLogSize, final int p_logSegmentSize,
            final HarddriveAccessMode p_mode) throws IOException {
        DirectByteBufferWrapper[] result;
        int numberOfSegments;

        numberOfSegments = (int) (p_secondaryLogSize / p_logSegmentSize);
        if (p_mode == HarddriveAccessMode.RANDOM_ACCESS_FILE) {
            RandomAccessFile randomAccessFile;
            randomAccessFile = new RandomAccessFile(new File(p_path), "r");
            result = new DirectByteBufferWrapper[numberOfSegments];
            for (int i = 0; i < numberOfSegments; i++) {
                result[i] = new DirectByteBufferWrapper(p_logSegmentSize, true);
                readFromSecondaryLogFile(result[i], p_logSegmentSize, i * p_logSegmentSize, randomAccessFile);
            }
            randomAccessFile.close();
        } else if (p_mode == HarddriveAccessMode.ODIRECT) {
            int fileID = JNIFileDirect.open(p_path, 1, 0);
            if (fileID < 0) {
                throw new IOException("JNI Error: Cannot open logfile.");
            }
            // Allocate buffers for reading
            result = new DirectByteBufferWrapper[numberOfSegments];
            for (int i = 0; i < numberOfSegments; i++) {
                result[i] = new DirectByteBufferWrapper(p_logSegmentSize, true);
                readFromSecondaryLogFile(result[i], p_logSegmentSize, i * p_logSegmentSize, fileID, p_mode);
            }
            JNIFileDirect.close(fileID);
        } else {
            File file = new File(p_path);
            int fileID = JNIFileRaw.openLog(file.getName());
            if (fileID < 0) {
                throw new IOException("JNI Error: Cannot open logfile.");
            }
            // Allocate buffers for reading
            result = new DirectByteBufferWrapper[numberOfSegments];
            for (int i = 0; i < numberOfSegments; i++) {
                result[i] = new DirectByteBufferWrapper(p_logSegmentSize, true);
                readFromSecondaryLogFile(result[i], p_logSegmentSize, i * p_logSegmentSize, fileID, p_mode);
            }
            JNIFileRaw.closeLog(fileID);
        }

        return result;
    }

    /**
     * Determines ChunkID ranges for recovery
     *
     * @param p_versionStorage
     *         all versions in array and hashtable
     * @param p_lowestCID
     *         the lowest ChunkID in versions array
     * @return all ChunkID ranges
     */
    private static long[] determineRanges(final TemporaryVersionsStorage p_versionStorage, final long p_lowestCID) {
        long[] ret;
        long[] localRanges = null;
        long[] otherRanges = null;

        if (p_versionStorage.getVersionsArray().size() > 0) {
            localRanges = getRanges(p_versionStorage.getVersionsArray(), p_lowestCID);
        }
        if (p_versionStorage.getVersionsHashTable().size() > 0) {
            System.out.println("Hashtable contains " + p_versionStorage.getVersionsHashTable().size() + " entries. " + ChunkID.toHexString(p_lowestCID));
            if (localRanges != null) {
                System.out.println("Local ranges: ");
                for (long chunkID : localRanges) {
                    System.out.print(ChunkID.toHexString(chunkID) + ' ');
                }
            }
            otherRanges = getRanges(p_versionStorage.getVersionsHashTable());
            System.out.println("Other ranges: ");
            for (long chunkID : otherRanges) {
                System.out.print(ChunkID.toHexString(chunkID) + ' ');
            }
        }

        if (localRanges == null) {
            ret = otherRanges;
        } else if (otherRanges == null) {
            ret = localRanges;
        } else {
            ret = new long[localRanges.length + otherRanges.length];
            System.arraycopy(localRanges, 0, ret, 0, localRanges.length);
            System.arraycopy(otherRanges, 0, ret, localRanges.length, otherRanges.length);
        }

        return ret;
    }

    /**
     * Determines all ChunkID ranges in versions array
     *
     * @param p_versionsArray
     *         the versions array
     * @return all ChunkID ranges in versions array
     */
    private static long[] getRanges(final VersionsArray p_versionsArray, final long p_lowestCID) {
        int currentIndex;
        int index = 0;
        long currentCID;
        ArrayListLong ranges = new ArrayListLong();

        while (index < p_versionsArray.capacity()) {
            if (p_versionsArray.getVersion(index, 0) == Version.INVALID_VERSION) {
                index++;
                continue;
            }

            currentCID = index + p_lowestCID;
            ranges.add(currentCID);
            currentIndex = 1;

            while (index + currentIndex < p_versionsArray.capacity() && p_versionsArray.getVersion(index + currentIndex, 0) != Version.INVALID_VERSION) {
                currentIndex++;
            }
            ranges.add(currentCID + currentIndex - 1);
            index += currentIndex;
        }

        return Arrays.copyOfRange(ranges.getArray(), 0, ranges.getSize());
    }

    // Setter

    /**
     * Determines all ChunkID ranges in versions hashtable
     *
     * @param p_versionsHT
     *         the versions hashtable
     * @return all ChunkID ranges in versions hashtable
     */
    private static long[] getRanges(final VersionsHashTable p_versionsHT) {
        int currentIndex;
        int index = 0;
        long currentCID;
        int[] table = p_versionsHT.getTable();
        ArrayListLong ranges = new ArrayListLong();

        // Sort table
        if (p_versionsHT.size() < SORT_THRESHOLD) {
            // There are only a few elements in table -> for a nearly sorted table insertion sort is much faster than quicksort
            insertionSort(table);
        } else {
            quickSort(table, table.length - 1);
        }

        while (index < table.length) {
            if (table[index] == 0) {
                index += 4;
                continue;
            }

            currentCID = (long) table[index] << 32 | table[index + 1] & 0xFFFFFFFFL;
            ranges.add(currentCID);
            currentIndex = 4;

            while (index + currentIndex < table.length &&
                    ((long) table[index + currentIndex] << 32 | table[index + currentIndex + 1] & 0xFFFFFFFFL) == currentCID + currentIndex / 4) {
                currentIndex += 4;
            }
            ranges.add(currentCID + currentIndex - 1);
            index += currentIndex;
        }

        return Arrays.copyOfRange(ranges.getArray(), 0, ranges.getSize());
    }

    // Methods

    /**
     * Sorts the versions hashtable with insertion sort; Used for a barely utilized hashtable as insertion sort is best for nearly sorted series
     *
     * @param p_table
     *         the array of the versions hashtable
     */
    private static void insertionSort(int[] p_table) {
        for (int i = 0; i < p_table.length / 4; i++) {
            for (int j = i; j > 0 && p_table[j * 4] < p_table[(j - 1) * 4]; j--) {
                swap(p_table, j, j - 1);
            }
        }
    }

    /**
     * Sorts the versions hashtable with quicksort (iterative!); Used for highly a utilized hashtable
     *
     * @param p_table
     *         the array of the versions hashtable
     * @param p_right
     *         the range index
     */
    private static void quickSort(int[] p_table, int p_right) {
        int left = 0;
        int right = p_right;

        int[] stack = new int[right - left + 1];
        int top = -1;

        stack[++top] = left;
        stack[++top] = right;

        while (top >= 0) {
            right = stack[top--];
            left = stack[top--];

            int pivot = partition(p_table, left, right);

            if (pivot - 1 > left) {
                stack[++top] = left;
                stack[++top] = pivot - 1;
            }

            if (pivot + 1 < right) {
                stack[++top] = pivot + 1;
                stack[++top] = right;
            }
        }
    }

    /**
     * Helper method for quicksort to partition the range
     *
     * @param p_table
     *         the array of the versions hashtable
     * @param p_left
     *         the index of the pivot element
     * @param p_right
     *         the range index
     * @return the partition index
     */
    private static int partition(int[] p_table, int p_left, int p_right) {
        int x = p_table[p_right * 4];
        int i = p_left - 1;

        for (int j = p_left; j <= p_right - 1; j++) {
            if (p_table[j * 4] <= x) {
                i++;
                swap(p_table, i, j);
            }
        }
        swap(p_table, i + 1, p_right);

        return i + 1;
    }

    /**
     * Helper method for quicksort and insertion sort to swap to elements
     *
     * @param p_table
     *         the array of the versions hashtable
     * @param p_index1
     *         the first index
     * @param p_index2
     *         the second index
     */
    private static void swap(int[] p_table, int p_index1, int p_index2) {
        int index1 = p_index1 * 4;
        int index2 = p_index2 * 4;

        if (p_table[index1] == 0) {
            if (p_table[index2] != 0) {
                p_table[index1] = p_table[index2];
                p_table[index1 + 1] = p_table[index2 + 1];
                p_table[index1 + 2] = p_table[index2 + 2];
                p_table[index1 + 3] = p_table[index2 + 3];

                p_table[index2] = 0;
                p_table[index2 + 1] = 0;
                p_table[index2 + 2] = 0;
                p_table[index2 + 3] = 0;
            }
        } else if (p_table[index2] == 0) {
            p_table[index2] = p_table[index1];
            p_table[index2 + 1] = p_table[index1 + 1];
            p_table[index2 + 2] = p_table[index1 + 2];
            p_table[index2 + 3] = p_table[index2 + 3];

            p_table[index1] = 0;
            p_table[index1 + 1] = 0;
            p_table[index1 + 2] = 0;
            p_table[index1 + 3] = 0;
        } else {
            int temp1 = p_table[index1];
            int temp2 = p_table[index1 + 1];
            int temp3 = p_table[index1 + 2];
            int temp4 = p_table[index1 + 3];

            p_table[index1] = p_table[index2];
            p_table[index1 + 1] = p_table[index2 + 1];
            p_table[index1 + 2] = p_table[index2 + 2];
            p_table[index1 + 3] = p_table[index2 + 3];

            p_table[index2] = temp1;
            p_table[index2 + 1] = temp2;
            p_table[index2 + 2] = temp3;
            p_table[index2 + 3] = temp4;
        }
    }

    /**
     * Returns the time since log creation in seconds (overflow occurs after 68+ years)
     *
     * @return the current time in seconds
     */
    private int getCurrentTimeInSec() {
        return (int) ((System.currentTimeMillis() - m_creationTimestamp) / 1000);
    }

    /**
     * Update owner and range ID after recovery
     *
     * @param p_restorer
     *         NodeID of the peer which recovered the backup range
     * @param p_newRangeID
     *         the new RangeID
     */
    public void transferBackupRange(final short p_restorer, final short p_newRangeID) {
        m_owner = p_restorer;
        m_rangeID = p_newRangeID;
    }

    /**
     * Returns the log size on disk
     *
     * @return the size
     */
    public final long getLogFileSize() {
        return getFileSize();
    }

    /**
     * Returns the versionsnl size on disk
     *
     * @return the size
     */
    public final long getVersionsFileSize() {
        return m_versionsBuffer.getFileSize();
    }

    /**
     * Returns the original owner
     *
     * @return the NodeID
     */
    public final short getOriginalOwner() {
        return m_originalOwner;
    }

    /**
     * Returns the RangeID
     *
     * @return the RangeID
     */
    public final short getRangeID() {
        return m_rangeID;
    }

    @Override
    public long getOccupiedSpace() {
        return determineLogSize();
    }

    /**
     * Returns all segment sizes
     *
     * @return all segment sizes
     */
    public final String getSegmentDistribution() {
        StringBuilder ret = new StringBuilder("++++Distribution: | ");
        SegmentHeader header;

        for (int i = 0; i < m_segmentHeaders.length; i++) {
            header = m_segmentHeaders[i];
            if (header != null) {
                ret.append(i).append(' ').append(header.getUsedBytes()).append(", u=").append(String.format("%.2f", header.getUtilization())).append(" | ");
            }
        }

        return ret.toString();
    }

    /**
     * Returns whether this secondary log is currently accessed by reorg. thread
     *
     * @return whether this secondary log is currently accessed by reorg. thread
     */
    public final boolean isAccessed() {
        return m_isAccessedByReorgThread;
    }

    /**
     * Closes the secondary log and frees all memory
     *
     * @throws IOException
     *         if closing the files fail
     */
    @Override
    public void closeAndRemove() throws IOException {
        // Free version buffer and version log
        m_versionsBuffer.closeAndRemove();

        // Free log on hard drive
        super.closeAndRemove();

        m_isClosed = true;
    }

    /**
     * Returns the next version for ChunkID
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the next version
     */
    public final Version getNextVersion(final long p_chunkID) {
        return m_versionsBuffer.getNext(p_chunkID);
    }

    /**
     * Invalidates a Chunk
     *
     * @param p_chunkID
     *         the ChunkID
     */
    public final void invalidateChunk(final long p_chunkID) {
        m_versionsBuffer.put(p_chunkID, Version.INVALID_VERSION);
    }

    @Override
    public final void appendData(final DirectByteBufferWrapper p_bufferWrapper, final int p_length) throws IOException, InterruptedException {
        int length = p_length;
        int logEntrySize;
        int rangeSize = 0;
        boolean isSignaled = false;
        SegmentHeader header;
        AbstractSecLogEntryHeader logEntryHeader;

        if (length <= 0 || length > m_secondaryLogSize) {
            throw new IllegalArgumentException("Error: Invalid data size (" + length + ')');
        }
        while (m_secondaryLogSize - determineLogSize() < length) {
            // #if LOGGER >= WARN
            LOGGER.warn("Secondary log for range %d of 0x%X is full. Initializing reorganization and awaiting execution", m_rangeID, m_owner);
            // #endif /* LOGGER >= WARN */
            signalReorganizationAndWait();
        }

        // Change epoch
        if (m_versionsBuffer.isThresholdReached()) {
            if (!m_isAccessedByReorgThread) {
                // Write versions buffer to SSD
                if (m_versionsBuffer.flush()) {
                    for (SegmentHeader segmentHeader : m_segmentHeaders) {
                        if (segmentHeader != null) {
                            segmentHeader.beginEon();
                        }
                    }
                }
            } else {
                // Force reorganization thread to flush all versions (even though it is reorganizing this log
                // currently -> high update rate)
                signalReorganization();
                isSignaled = true;
            }
        }

        /*
         * Appending data cases:
         * 1. This secondary log is accessed by the reorganization thread:
         * a. Put data in currently active segment
         * b. No active segment or buffer too large to fit in: Create (new) "active segment" with given data
         * 2.
         * a. Buffer is large (at least 90% of segment size): Create new segment and append it
         * b. Fill partly used segments and put the rest (if there is data left) in a new segment and append it
         */
        if (m_isAccessedByReorgThread) {
            // Reorganization thread is working on this secondary log -> only write in active segment
            if (m_activeSegment != null && m_activeSegment.getFreeBytes() >= length) {
                // Fill active segment
                writeToSecondaryLog(p_bufferWrapper, 0, (long) m_activeSegment.getIndex() * m_logSegmentSize + m_activeSegment.getUsedBytes(), length, true);
                m_activeSegment.updateUsedBytes(length);
                if (m_useTimestamps) {
                    // Modify segment age
                    int currentAge = m_activeSegment.getAge();
                    m_activeSegment.setAge(currentAge - currentAge * length / m_activeSegment.getUsedBytes() /* contains length already */);
                }
            } else {
                if (m_activeSegment != null) {
                    // There is not enough space in active segment to store the whole buffer -> first fill current one
                    header = m_segmentHeaders[m_activeSegment.getIndex()];
                    while (true) {
                        logEntryHeader = AbstractSecLogEntryHeader.getHeader(p_bufferWrapper.getBuffer(), rangeSize);
                        logEntrySize = logEntryHeader.getHeaderSize(p_bufferWrapper.getBuffer(), rangeSize) +
                                logEntryHeader.getLength(p_bufferWrapper.getBuffer(), rangeSize);
                        if (logEntrySize > header.getFreeBytes() - rangeSize) {
                            break;
                        } else {
                            rangeSize += logEntrySize;
                        }
                    }
                    if (rangeSize > 0) {
                        writeToSecondaryLog(p_bufferWrapper, 0, (long) header.getIndex() * m_logSegmentSize + header.getUsedBytes(), rangeSize, true);
                        header.updateUsedBytes(rangeSize);
                        if (m_useTimestamps) {
                            // Modify segment age
                            int currentAge = header.getAge();
                            header.setAge(currentAge - currentAge * rangeSize / header.getUsedBytes() /* contains rangeSize already */);
                        }
                        length -= rangeSize;
                    }
                }

                // There is no active segment or the active segment is full
                length = createNewSegmentAndFill(p_bufferWrapper, rangeSize, length, true);
                if (length > 0) {
                    // There is no free segment -> fill partly used segments
                    length = fillPartlyUsedSegments(p_bufferWrapper, rangeSize, length, true);

                    // #if LOGGER >= ERROR
                    if (length > 0) {
                        LOGGER.error("Secondary log is full!");
                    }
                    // #endif /* LOGGER >= ERROR */
                }
            }
        } else {
            if (m_activeSegment != null) {
                m_activeSegment = null;
            }

            if (length >= m_logSegmentSize * 0.9) {
                // Create new segment and fill it
                length = createNewSegmentAndFill(p_bufferWrapper, 0, length, false);
            }
            if (length > 0) {
                // Fill partly used segments if log iteration (remove task) is not in progress
                length = fillPartlyUsedSegments(p_bufferWrapper, 0, length, false);

                // #if LOGGER >= ERROR
                if (length > 0) {
                    LOGGER.error("Secondary log is full!");
                }
                // #endif /* LOGGER >= ERROR */
            }
        }

        if (determineLogSize() >= m_secondaryLogReorgThreshold && !isSignaled) {
            signalReorganization();
            // #if LOGGER >= INFO
            LOGGER.info("Threshold breached (%d) for secondary log %d of 0x%X. Initializing reorganization.", determineLogSize(), m_rangeID, m_owner);
            // #endif /* LOGGER >= INFO */
        }
    }

    /**
     * Returns a list with all log entries wrapped in chunks
     *
     * @param p_versions
     *         all versions read from SSD
     * @param p_lowestCID
     *         the lowest CID in range
     * @param p_timeToGetLock
     *         the time for acquiring recovery lock
     * @param p_timeToReadVersions
     *         the time for loading the versions
     * @param p_chunkComponent
     *         the ChunkBackupComponent to store recovered chunks
     * @param p_doCRCCheck
     *         whether to check the payload or not
     * @return ChunkIDs of all recovered chunks, number of recovered chunks and bytes
     */
    public final RecoveryMetadata recoverFromLog(final TemporaryVersionsStorage p_versions, final long p_lowestCID, final long p_timeToGetLock,
            final long p_timeToReadVersions, final ChunkBackupComponent p_chunkComponent, final boolean p_doCRCCheck) {
        int numberOfRecoveredLargeChunks = 0;
        long timeToPut = 0;
        boolean doCRCCheck = p_doCRCCheck;
        byte[] index = new byte[m_segmentHeaders.length];
        ReentrantLock indexLock = new ReentrantLock(false);
        ReentrantLock largeChunkLock = new ReentrantLock(false);
        final RecoveryMetadata recoveryMetadata = new RecoveryMetadata();
        HashMap<Long, DSByteBuffer> largeChunks;

        // FIXME: Recovery fails if versions (partly only?) are stored in hashtable

        if (determineLogSize() == 0) {
            // #if LOGGER >= INFO
            LOGGER.info("Backup range %d is empty. No need for recovery.", m_rangeID);
            // #endif /* LOGGER >= INFO */
            return null;
        }

        if (p_doCRCCheck && !m_useChecksums) {
            // #if LOGGER >= WARN
            LOGGER.warn("Unable do check for data corruption as no checksums are stored (configurable)!");
            // #endif /* LOGGER >= WARN */

            doCRCCheck = false;
        }

        Statistics statsCaller = new Statistics();

        // HashMap to store large Chunks in
        largeChunks = new HashMap<>();

        // #if LOGGER >= INFO
        if (m_owner == m_originalOwner) {
            LOGGER.info("Starting recovery of backup range %d of 0x%X", m_rangeID, m_owner);
        } else {
            LOGGER.info("Starting recovery of backup range %d of 0x%X. Original owner: 0x%x", m_rangeID, m_owner, m_originalOwner);
        }
        // #endif /* LOGGER >= INFO */

        long time = System.currentTimeMillis();

        // Write Chunks in parallel
        RecoveryWriterThread writerThread = p_chunkComponent.initRecoveryThread();

        // Determine ChunkID ranges in parallel
        RecoveryHelperThread[] helperThreads = new RecoveryHelperThread[RECOVERY_THREADS];
        for (int i = 0; i < RECOVERY_THREADS; i++) {
            RecoveryHelperThread helperThread =
                    new RecoveryHelperThread(recoveryMetadata, p_versions, largeChunks, largeChunkLock, p_lowestCID, index, indexLock, p_doCRCCheck,
                            p_chunkComponent);
            helperThread.setName("Recovery: Helper-Thread " + (i + 1));
            helperThread.start();
            helperThreads[i] = helperThread;
        }

        // Determine CID ranges
        long t = System.currentTimeMillis();
        recoveryMetadata.setChunkIDRanges(determineRanges(p_versions, p_lowestCID));
        statsCaller.m_timeToDetermineRanges = System.currentTimeMillis() - t;

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

            if (m_segmentHeaders[idx] != null && !m_segmentHeaders[idx].isEmpty()) {
                recoverSegment(idx, p_versions, p_lowestCID, recoveryMetadata, largeChunks, largeChunkLock, p_chunkComponent, doCRCCheck, statsCaller);
            }
            idx++;
        }

        try {
            for (int i = 0; i < RECOVERY_THREADS; i++) {
                helperThreads[i].join();
                statsCaller.merge(helperThreads[i].getStatistics());
            }
            while (!writerThread.finished()) {
                Thread.yield();
            }
            timeToPut = writerThread.getTimeToPut();
            writerThread.interrupt();
            writerThread.join();
        } catch (InterruptedException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Interrupt: Could not wait for RecoveryHelperThread/RecoveryWriterThread to finish!");
            // #endif /* LOGGER >= ERROR */
        }

        t = System.currentTimeMillis();
        if (!largeChunks.isEmpty()) {
            numberOfRecoveredLargeChunks =
                    p_chunkComponent.putRecoveredChunks(recoveryMetadata, largeChunks.values().toArray(new DSByteBuffer[largeChunks.size()]));
        }
        timeToPut += System.currentTimeMillis() - t;

        // #if LOGGER >= INFO
        LOGGER.info("Recovery of backup range finished: ");
        LOGGER.info("\t Recovered %d chunks (large: %d) in %d ms", recoveryMetadata.getNumberOfChunks(), numberOfRecoveredLargeChunks,
                System.currentTimeMillis() - time);
        StringBuilder ranges = new StringBuilder("\t ChunkID ranges: ");
        for (long chunkID : recoveryMetadata.getCIDRanges()) {
            ranges.append(ChunkID.toHexString(chunkID)).append(' ');
        }
        LOGGER.info(ranges.toString());
        LOGGER.info("\t Read versions from array: \t\t\t\t%.2f %%",
                (double) (statsCaller.m_readVersionsFromArray / (statsCaller.m_readVersionsFromArray + statsCaller.m_readVersionsFromHashTable) * 100));
        LOGGER.info("\t Time to acquire recovery lock: \t\t\t %d ms", p_timeToGetLock);
        LOGGER.info("\t Time to read versions from SSD: \t\t\t %d ms", p_timeToReadVersions);
        LOGGER.info("\t Time to determine ranges: \t\t\t\t %d ms", statsCaller.m_timeToDetermineRanges);
        LOGGER.info("\t Time to read segments from SSD (sequential): \t\t %d ms", statsCaller.m_timeToReadSegmentsFromDisk);
        LOGGER.info("\t Time to read headers, check versions and checksums: \t %d ms", statsCaller.m_timeToCheck);
        LOGGER.info("\t Time to create and put chunks in memory management: \t %d ms", timeToPut);
        // #endif /* LOGGER >= INFO */

        return recoveryMetadata;
    }

    @Override
    public String toString() {
        if (m_owner == m_originalOwner) {
            return "Owner: " + m_owner + " - RangeID: " + m_rangeID + " - Written bytes: " + determineLogSize();
        } else {
            return "Owner: " + m_owner + " - RangeID: " + m_rangeID + " - Original Owner: " + m_owner + " - Original RangeID: " + m_originalRangeID +
                    " - Written bytes: " + determineLogSize();
        }
    }

    /**
     * Sets the access flag
     *
     * @param p_flag
     *         the new status
     */
    final void setAccessFlag(final boolean p_flag) {
        m_isAccessedByReorgThread = p_flag;

        // Helpful for debugging, but may cause null pointer exception for writer thread
        /*-if (!p_flag) {
            m_activeSegment = null;//
        }*/
    }

    /**
     * Returns true if there are segments that were not yet reorganized in this eon and the eon has exceeded half time
     *
     * @return whether this log needs to be reorganized or not
     */
    final boolean needToBeReorganized() {
        boolean ret = false;

        if (m_versionsBuffer.getEpoch() > Math.pow(2, 14)) {
            for (SegmentHeader segmentHeader : m_segmentHeaders) {
                if (segmentHeader != null && !segmentHeader.isEmpty() && !segmentHeader.wasReorganized()) {
                    ret = true;
                    break;
                }
            }
        }

        return ret;
    }

    /**
     * Reorganizes all segments
     *
     * @param p_bufferWrapper
     *         aligned buffer to be filled with segment data (avoiding lots of allocations)
     * @param p_allVersions
     *         an array and a hash table (for migrations) with all versions for this secondary log
     * @param p_lowestLID
     *         the lowest LID at the time the versions are read-in
     */
    final void reorganizeAll(final DirectByteBufferWrapper p_bufferWrapper, final TemporaryVersionsStorage p_allVersions, final long p_lowestLID) {
        for (int i = 0; i < m_segmentHeaders.length; i++) {
            if (m_segmentHeaders[i] != null && !Thread.currentThread().isInterrupted()) {
                if (!reorganizeSegment(i, p_bufferWrapper, p_allVersions, p_lowestLID)) {
                    // Reorganization failed because of an I/O error -> abort
                    break;
                }
            }
        }
    }

    /**
     * Reorganizes one segment by choosing the segment with best cost-benefit ratio
     *
     * @param p_bufferWrapper
     *         aligned buffer to be filled with segment data (avoiding lots of allocations)
     * @param p_allVersions
     *         an array and a hash table (for migrations) with all versions for this secondary log
     * @param p_lowestLID
     *         the lowest LID at the time the versions are read-in
     */
    final boolean reorganizeIteratively(final DirectByteBufferWrapper p_bufferWrapper, final TemporaryVersionsStorage p_allVersions, final long p_lowestLID) {
        return reorganizeSegment(chooseSegment(), p_bufferWrapper, p_allVersions, p_lowestLID);
    }

    /**
     * Resets the current reorganization segment
     */
    final void resetReorgSegment() {
        m_reorgSegment = null;
    }

    /**
     * Gets current versions from log
     *
     * @param p_allVersions
     *         an array and hash table (for migrations) to store version numbers in
     * @return the lowest CID at the time the versions are read-in
     */
    public final long getCurrentVersions(final TemporaryVersionsStorage p_allVersions, final boolean p_writeBack) {
        if (p_writeBack) {
            Arrays.fill(m_reorgVector, (byte) 0);

            // Read versions from SSD and write back current view
            return m_versionsBuffer.readAll(p_allVersions, true);
        } else {
            return m_versionsBuffer.readAll(p_allVersions, false);
        }
    }

    /**
     * Returns the index of a free segment
     *
     * @return the index of a free segment
     */
    private byte getFreeSegment() {
        byte ret = -1;
        byte b = 0;

        while (b < m_segmentHeaders.length) {
            // Empty segment headers are null
            if (m_segmentHeaders[b] == null) {
                // Avoid reorganization segment
                if (m_reorgSegment == null || b != m_reorgSegment.getIndex()) {
                    ret = b;
                    break;
                }
            }
            b++;
        }

        return ret;
    }

    /**
     * Recover a segment from a normal secondary log
     *
     * @param p_segmentIndex
     *         the index of the segment
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
     * @param p_doCRCCheck
     *         whether to check the CRC checksum
     * @param p_stat
     *         timing statistics
     */
    private void recoverSegment(final int p_segmentIndex, final TemporaryVersionsStorage p_allVersions, final long p_lowestCID,
            final RecoveryMetadata p_recoveryMetadata, final HashMap<Long, DSByteBuffer> p_largeChunks, final ReentrantLock p_largeChunkLock,
            final ChunkBackupComponent p_chunkComponent, final boolean p_doCRCCheck, final Statistics p_stat) {
        int headerSize;
        int readBytes = 0;
        int segmentLength;
        int payloadSize;
        int combinedSize = 0;
        long chunkID;
        long time;
        DirectByteBufferWrapper bufferWrapper;
        ByteBuffer segmentData;
        Version currentVersion;
        Version entryVersion;
        AbstractSecLogEntryHeader logEntryHeader;

        try {
            time = System.currentTimeMillis();
            bufferWrapper = new DirectByteBufferWrapper(m_logSegmentSize, true);
            segmentLength = readSegment(bufferWrapper, p_segmentIndex);
            segmentData = bufferWrapper.getBuffer();

            p_stat.m_timeToReadSegmentsFromDisk += System.currentTimeMillis() - time;

            if (segmentLength > 0) {
                int index = 0;
                int length = 100000;
                long[] chunkIDs = new long[length];
                int[] offsets = new int[length];
                int[] lengths = new int[length];

                while (readBytes < segmentLength) {
                    time = System.currentTimeMillis();
                    logEntryHeader = AbstractSecLogEntryHeader.getHeader(segmentData, readBytes);
                    headerSize = logEntryHeader.getHeaderSize(segmentData, readBytes);
                    payloadSize = logEntryHeader.getLength(segmentData, readBytes);
                    chunkID = logEntryHeader.getCID(segmentData, readBytes);
                    entryVersion = logEntryHeader.getVersion(segmentData, readBytes);

                    // Get current version
                    if (logEntryHeader.isMigrated()) {
                        currentVersion = p_allVersions.get(chunkID);
                        p_stat.m_readVersionsFromHashTable++;
                    } else {
                        chunkID = ((long) m_originalOwner << 48) + chunkID;
                        currentVersion = p_allVersions.get(chunkID, p_lowestCID, p_stat);
                    }

                    if (currentVersion == null || currentVersion.getVersion() == 0) {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Version unknown for chunk 0x%X! Secondary log: %s", chunkID, this);
                        // #endif /* LOGGER >= ERROR */
                    } else if (currentVersion.isEqual(entryVersion)) {
                        // Compare current version with element
                        // Create chunk only if log entry complete
                        if (p_doCRCCheck) {
                            if (ChecksumHandler.calculateChecksumOfPayload(bufferWrapper, readBytes + headerSize, payloadSize) !=
                                    logEntryHeader.getChecksum(segmentData, readBytes)) {
                                // #if LOGGER >= ERROR
                                LOGGER.error("Corrupt data. Could not recover 0x%X!", chunkID);
                                // #endif /* LOGGER >= ERROR */

                                readBytes += headerSize + payloadSize;
                                continue;
                            }
                        }
                        p_stat.m_timeToCheck += System.currentTimeMillis() - time;

                        if (logEntryHeader.isChained(segmentData, readBytes)) {
                            byte chainID = logEntryHeader.getChainID(segmentData, readBytes);
                            byte chainSize = logEntryHeader.getChainSize(segmentData, readBytes);
                            int maxLogEntrySize = AbstractLogEntryHeader.getMaxLogEntrySize();

                            p_largeChunkLock.lock();
                            DSByteBuffer chunk = p_largeChunks.get(chunkID);
                            if (chunk == null) {
                                // This is the first segment for this ChunkID -> create an array large enough for holding all data for this chunk
                                if (chainID == chainSize - 1) {
                                    // This is the last chain link -> complete size is known
                                    chunk = new DSByteBuffer(chunkID, (chainSize - 1) * maxLogEntrySize + payloadSize);
                                    p_largeChunks.put(chunkID, chunk);
                                } else {
                                    // This is another chain link -> maximum size is known, only -> must be truncated later
                                    chunk = new DSByteBuffer(chunkID, chainSize * maxLogEntrySize);
                                    p_largeChunks.put(chunkID, chunk);
                                }
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
                                if (!p_chunkComponent.putRecoveredChunks(chunkIDs, bufferWrapper.getAddress(), offsets, lengths, length)) {
                                    // #if LOGGER >= ERROR
                                    LOGGER.error("Memory management failure. Could not recover chunks!");
                                    // #endif /* LOGGER >= ERROR */
                                }

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
                        p_stat.m_timeToCheck += System.currentTimeMillis() - time;
                    }
                    readBytes += headerSize + payloadSize;
                }

                // Put other chunks in memory
                if (index != 0) {
                    if (!p_chunkComponent.putRecoveredChunks(chunkIDs, bufferWrapper.getAddress(), offsets, lengths, index)) {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Memory management failure. Could not recover chunks!");
                        // #endif /* LOGGER >= ERROR */
                    }

                    p_recoveryMetadata.add(index, combinedSize);
                }
            }
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Recovery failed(%d): ", m_rangeID, e);
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Fills partly used segments
     *
     * @param p_bufferWrapper
     *         the buffer
     * @param p_offset
     *         the offset within the buffer
     * @param p_length
     *         the range length
     * @param p_isAccessed
     *         whether the reorganization thread is active on this log or not
     * @return the remained length
     * @throws IOException
     *         if the secondary log could not be read
     */

    private int fillPartlyUsedSegments(final DirectByteBufferWrapper p_bufferWrapper, final int p_offset, final int p_length, final boolean p_isAccessed)
            throws IOException {
        short segment;
        int offset = p_offset;
        int rangeSize;
        int logEntrySize;
        int length = p_length;
        SegmentHeader header;
        AbstractSecLogEntryHeader logEntryHeader;

        while (length > 0) {
            if (p_isAccessed) {
                m_segmentAssignmentlock.lock();
            }

            // Get the smallest used segment that has enough free space to store everything.
            // If there is no best fitting segment, choose an empty one.
            // If there is no empty one, return the segment with most free space.
            segment = getUsedSegment(length);
            header = m_segmentHeaders[segment];

            if (header == null) {
                // This segment is empty (there was no best fitting segment)
                header = new SegmentHeader(segment, length);
                m_segmentHeaders[segment] = header;

                if (p_isAccessed) {
                    // Set active segment. Must be synchronized.
                    m_activeSegment = header;
                    m_segmentAssignmentlock.unlock();
                }
                writeToSecondaryLog(p_bufferWrapper, offset, (long) segment * m_logSegmentSize, length, p_isAccessed);
                if (m_useTimestamps) {
                    // Modify segment age
                    int currentAge = header.getAge();
                    header.setAge(currentAge - currentAge * length / header.getUsedBytes() /* contains length already */);
                }
                length = 0;

                break;
            } else {
                if (p_isAccessed) {
                    // Set active segment. Must be synchronized.
                    m_activeSegment = header;
                    m_segmentAssignmentlock.unlock();
                }

                if (length <= header.getFreeBytes()) {
                    // All data fits in this segment
                    writeToSecondaryLog(p_bufferWrapper, offset, (long) segment * m_logSegmentSize + header.getUsedBytes(), length, p_isAccessed);
                    header.updateUsedBytes(length);
                    if (m_useTimestamps) {
                        // Modify segment age
                        int currentAge = header.getAge();
                        header.setAge(currentAge - currentAge * length / header.getUsedBytes() /* contains length already */);
                    }
                    length = 0;

                    break;
                } else {
                    // This is the largest left segment -> write as long as there is space left
                    rangeSize = 0;
                    while (true) {
                        logEntryHeader = AbstractSecLogEntryHeader.getHeader(p_bufferWrapper.getBuffer(), offset + rangeSize);
                        logEntrySize = logEntryHeader.getHeaderSize(p_bufferWrapper.getBuffer(), offset + rangeSize) +
                                logEntryHeader.getLength(p_bufferWrapper.getBuffer(), offset + rangeSize);
                        if (logEntrySize > header.getFreeBytes() - rangeSize) {
                            break;
                        } else {
                            rangeSize += logEntrySize;
                        }
                    }
                    if (rangeSize > 0) {
                        writeToSecondaryLog(p_bufferWrapper, offset, (long) segment * m_logSegmentSize + header.getUsedBytes(), rangeSize, p_isAccessed);
                        header.updateUsedBytes(rangeSize);
                        if (m_useTimestamps) {
                            // Modify segment age
                            int currentAge = header.getAge();
                            header.setAge(currentAge - currentAge * rangeSize / header.getUsedBytes() /* contains rangeSize already */);
                        }
                        length -= rangeSize;
                        offset += rangeSize;
                    }
                }
            }
        }

        return length;
    }

    /**
     * Creates a new segment and fills it
     *
     * @param p_bufferWrapper
     *         the buffer
     * @param p_offset
     *         the offset within the buffer
     * @param p_length
     *         the range length
     * @param p_isAccessed
     *         whether the reorganization thread is active on this log or not
     * @return the remained length
     * @throws IOException
     *         if the secondary log could not be read
     */
    private int createNewSegmentAndFill(final DirectByteBufferWrapper p_bufferWrapper, final int p_offset, final int p_length, final boolean p_isAccessed)
            throws IOException {
        int ret = p_length;
        short segment;
        SegmentHeader header;

        if (p_isAccessed) {
            m_segmentAssignmentlock.lock();
        }

        segment = getFreeSegment();
        if (segment != -1) {
            header = new SegmentHeader(segment, p_length);
            m_segmentHeaders[segment] = header;

            if (p_isAccessed) {
                // Set active segment. Must be synchronized.
                m_activeSegment = header;
                m_segmentAssignmentlock.unlock();
            }

            writeToSecondaryLog(p_bufferWrapper, p_offset, (long) segment * m_logSegmentSize, p_length, p_isAccessed);
            if (m_useTimestamps) {
                // Modify segment age
                int currentAge = header.getAge();
                header.setAge(currentAge - currentAge * p_length / header.getUsedBytes() /* contains p_length already */);
            }
            ret = 0;
        } else {
            if (p_isAccessed) {
                m_segmentAssignmentlock.unlock();
            }
        }

        return ret;
    }

    /**
     * Returns the sum of all segment sizes
     *
     * @return the sum of all segment sizes
     */
    private int determineLogSize() {
        int ret = 0;

        for (int i = 0; i < m_segmentHeaders.length; i++) {
            if (m_segmentHeaders[i] != null) {
                ret += m_segmentHeaders[i].getUsedBytes();
            }
        }

        return ret;
    }

    /**
     * Returns the index of the best-fitting segment
     *
     * @param p_length
     *         the length of the data
     * @return the index of the best-fitting segment
     */
    private short getUsedSegment(final int p_length) {
        short ret;
        short bestFitSegment = -1;
        short maxSegment = -1;
        short emptySegment = -1;
        int bestFit = Integer.MAX_VALUE;
        int max = 0;
        int freeBytes;

        for (short index = 0; index < m_segmentHeaders.length; index++) {
            if (m_segmentHeaders[index] == null) {
                // This is an empty segment. We need it if there is no best fit.
                if (emptySegment == -1) {
                    emptySegment = index;
                }
            } else {
                // Avoid reorganization segment
                if (!m_segmentHeaders[index].equals(m_reorgSegment)) {
                    freeBytes = m_segmentHeaders[index].getFreeBytes();
                    if (freeBytes >= p_length) {
                        if (freeBytes < bestFit) {
                            // In current segment is more space than needed but less than in every segment before ->
                            // current best fit
                            bestFit = freeBytes;
                            bestFitSegment = index;
                        }
                    } else if (freeBytes > max) {
                        // In current segment is less space than needed but more than in every segment before -> current
                        // maximum
                        max = freeBytes;
                        maxSegment = index;
                    }
                }
            }
        }

        // Choose segment with following order: 1. best fit 2. empty segment 3. max space
        if (bestFitSegment != -1) {
            ret = bestFitSegment;
        } else if (emptySegment != -1) {
            ret = emptySegment;
        } else {
            ret = maxSegment;
        }

        return ret;
    }

    /**
     * Returns given segment of secondary log
     *
     * @param p_bufferWrapper
     *         the aligned buffer to read data into
     * @param p_segmentIndex
     *         the segment
     * @return the segment's data
     * @throws IOException
     *         if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    private int readSegment(final DirectByteBufferWrapper p_bufferWrapper, final int p_segmentIndex) throws IOException {
        int ret = 0;
        SegmentHeader header;

        header = m_segmentHeaders[p_segmentIndex];
        if (header != null) {
            ret = header.getUsedBytes();
            p_bufferWrapper.getBuffer().clear();
            readFromSecondaryLog(p_bufferWrapper, ret, p_segmentIndex * m_logSegmentSize, true);
        }
        return ret;
    }

    /**
     * Updates log segment
     *
     * @param p_bufferWrapper
     *         the aligned buffer
     * @param p_length
     *         the segment length
     * @param p_segmentIndex
     *         the segment index
     * @throws IOException
     *         if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    private void updateSegment(final DirectByteBufferWrapper p_bufferWrapper, final int p_length, final int p_segmentIndex) throws IOException {
        SegmentHeader header;

        // Overwrite segment on log
        writeToSecondaryLog(p_bufferWrapper, 0, (long) p_segmentIndex * m_logSegmentSize, p_length + 1, true);

        // Update segment header
        header = m_segmentHeaders[p_segmentIndex];
        header.reset();
        header.updateUsedBytes(p_length);
        header.markSegmentAsReorganized();
    }

    /**
     * Frees segment
     *
     * @param p_segmentIndex
     *         the segment
     * @throws IOException
     *         if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    private void freeSegment(final int p_segmentIndex) throws IOException {

        // Mark the end of the segment (a log entry header cannot start with a zero)
        writeToSecondaryLog(null, 0, (long) p_segmentIndex * m_logSegmentSize, 1, true);
        m_segmentHeaders[p_segmentIndex] = null;
    }

    /**
     * Wakes up the reorganization thread
     *
     * @throws InterruptedException
     *         if caller is interrupted
     */
    private void signalReorganization() throws InterruptedException {
        m_reorganizationThread.setLogToReorgImmediately(this, false);
    }

    /**
     * Wakes up the reorganization thread and waits until reorganization is
     * finished
     *
     * @throws InterruptedException
     *         if caller is interrupted
     */
    private void signalReorganizationAndWait() throws InterruptedException {
        m_reorganizationThread.setLogToReorgImmediately(this, true);
    }

    /**
     * Reorganizes one given segment of a normal secondary log
     *
     * @param p_segmentIndex
     *         the segments index
     * @param p_bufferWrapper
     *         aligned buffer to be filled with segment data (avoiding lots of allocations)
     * @param p_allVersions
     *         a hash table and int array with all versions for this secondary log
     * @param p_lowestCID
     *         the lowest CID at the time the versions were read-in
     * @return whether the reorganization was successful or not
     */
    private boolean reorganizeSegment(final int p_segmentIndex, final DirectByteBufferWrapper p_bufferWrapper, final TemporaryVersionsStorage p_allVersions,
            final long p_lowestCID) {
        boolean ret = true;
        int length;
        int readBytes = 0;
        int writtenBytes = 0;
        int segmentLength;
        long chunkID;
        long ageAllBytes = 0;
        ByteBuffer segmentData;
        Version currentVersion;
        Version entryVersion;
        AbstractSecLogEntryHeader logEntryHeader;

        long start = System.currentTimeMillis();
        long assignment = 0;
        long read = 0;
        long beforeWrite = 0;
        long afterWrite = 0;
        if (p_segmentIndex != -1 && p_allVersions != null) {
            m_segmentAssignmentlock.lock();
            if (m_activeSegment == null || m_activeSegment.getIndex() != p_segmentIndex) {
                m_reorgSegment = m_segmentHeaders[p_segmentIndex];
                m_segmentAssignmentlock.unlock();
                assignment = System.currentTimeMillis();

                int count = -1;
                try {
                    segmentLength = readSegment(p_bufferWrapper, p_segmentIndex);
                    segmentData = p_bufferWrapper.getBuffer();
                    ByteBuffer writeCopy = segmentData.duplicate();
                    writeCopy.order(ByteOrder.LITTLE_ENDIAN);
                    read = System.currentTimeMillis();
                    if (segmentLength > 0) {
                        while (readBytes < segmentLength && !Thread.currentThread().isInterrupted()) {
                            count++;
                            logEntryHeader = AbstractSecLogEntryHeader.getHeader(segmentData, readBytes);
                            length = logEntryHeader.getHeaderSize(segmentData, readBytes) + logEntryHeader.getLength(segmentData, readBytes);
                            chunkID = logEntryHeader.getCID(segmentData, readBytes);
                            entryVersion = logEntryHeader.getVersion(segmentData, readBytes);

                            // Get current version
                            if (logEntryHeader.isMigrated()) {
                                currentVersion = p_allVersions.get(chunkID);
                            } else {
                                chunkID = ((long) m_originalOwner << 48) + chunkID;
                                currentVersion = p_allVersions.get(chunkID, p_lowestCID);
                            }
                            if (currentVersion == null || m_versionsBuffer.getEpoch() == entryVersion.getEpoch()) {
                                // There is no entry in hashtable or element is more current -> get latest version from cache
                                // (Epoch can only be 1 greater because there is no flushing during reorganization)
                                currentVersion = m_versionsBuffer.get(chunkID);
                            }

                            if (currentVersion == null || currentVersion.getVersion() == 0) {
                                System.out.println(count + "( " + readBytes + "): " + ChunkID.toHexString(chunkID) + ", " + length + ", " + entryVersion);
                                // #if LOGGER >= ERROR
                                LOGGER.error("Version unknown for chunk 0x%X! Distance to average CID: %d. Secondary log: %s, %d", chunkID,
                                        chunkID - p_lowestCID, this, AbstractSecLogEntryHeader.getMaximumNumberOfVersions(m_secondaryLogSize / 2, 256, false));
                                // #endif /* LOGGER >= ERROR */
                            } else if (currentVersion.isEqual(entryVersion)) {
                                // Compare current version with element
                                if (readBytes != writtenBytes) {
                                    int limit = segmentData.limit();
                                    segmentData.limit(readBytes + length);

                                    writeCopy.put(segmentData);

                                    segmentData.limit(limit);
                                    //System.arraycopy(p_segmentData, readBytes, p_segmentData, writtenBytes, length);
                                }
                                writtenBytes += length;

                                if (m_useTimestamps) {
                                    int entryAge = getCurrentTimeInSec() - logEntryHeader.getTimestamp(segmentData, readBytes);
                                    if (entryAge < m_coldDataThreshold) {
                                        // Do not consider cold data for calculation
                                        ageAllBytes += entryAge * length;
                                    }
                                }

                                if (currentVersion.getEon() != m_versionsBuffer.getEon()) {
                                    // Update eon in both versions
                                    logEntryHeader.flipEon(segmentData, writtenBytes - length);

                                    // Add to version buffer; all entries will get current eon during flushing
                                    m_versionsBuffer.tryPut(chunkID, currentVersion.getVersion());
                                }
                            } else {
                                // Version, epoch and/or eon is different -> remove entry
                            }
                            readBytes += length;
                        }
                        if (writtenBytes < readBytes && !Thread.currentThread().isInterrupted()) {
                            beforeWrite = System.currentTimeMillis();
                            if (writtenBytes > 0) {
                                updateSegment(p_bufferWrapper, writtenBytes, p_segmentIndex);
                                if (m_useTimestamps) {
                                    // Calculate current age of segment
                                    m_reorgSegment.setAge((int) (ageAllBytes / writtenBytes));
                                }
                            } else {
                                freeSegment(p_segmentIndex);
                            }
                            afterWrite = System.currentTimeMillis();
                        }
                    }
                } catch (final IOException e) {
                    if (m_isClosed) {
                        return false;
                    }
                    // #if LOGGER >= WARN
                    LOGGER.warn("Reorganization failed(log: %d): ", m_rangeID, e);
                    // #endif /* LOGGER >= WARN */
                    ret = false;
                }
            } else {
                m_segmentAssignmentlock.unlock();
            }

            if (!Thread.currentThread().isInterrupted()) {
                if (readBytes - writtenBytes > 0) {
                    // #if LOGGER >= INFO
                    LOGGER.info("Freed %d bytes during reorganization of segment %d in range 0x%X,%d\t total log size: %d", readBytes - writtenBytes,
                            p_segmentIndex, m_owner, m_rangeID, determineLogSize() / 1024 / 1024);
                    LOGGER.info("Time to assign: %d, read: %d, process: %d, write: %d, all: %d", assignment - start, read - assignment, beforeWrite - read,
                            afterWrite - beforeWrite, System.currentTimeMillis() - start);
                    // #endif /* LOGGER >= INFO */
                }
            } else {
                // #if LOGGER >= INFO
                LOGGER.info("Interrupted during reorganization of segment %d in range 0x%X,%d\t total log size: %d", p_segmentIndex, m_owner, m_rangeID,
                        determineLogSize() / 1024 / 1024);
                LOGGER.info("Time to assign: %d, read: %d, process: %d, write: %d, all: %d", assignment - start, read - assignment, beforeWrite - read,
                        afterWrite - beforeWrite, System.currentTimeMillis() - start);
                // #endif /* LOGGER >= INFO */
            }
        }

        return ret;
    }

    /**
     * Determines the next segment to reorganize
     *
     * @return the chosen segment
     */
    private int chooseSegment() {
        int ret = -1;
        int tries;
        double costBenefitRatio;
        double max = -1;
        SegmentHeader currentSegment;

        /*
         * Choose a segment based on the cost-benefit formula (the utilization does not contain an invalid counter).
         *
         * Every tenth segment is chosen randomly out of all segments that have not been reorganized in this eon.
         * Avoid segments that already have been reorganized within this epoch (-> m_reorgVector).
         */
        if (m_segmentReorgCounter++ == 10) {
            tries = (int) (m_secondaryLogSize / m_logSegmentSize * 2);
            while (true) {
                ret = RandomUtils.getRandomValue((int) (m_secondaryLogSize / m_logSegmentSize) - 1);
                if (m_segmentHeaders[ret] != null && !m_segmentHeaders[ret].wasReorganized() && m_reorgVector[ret] == 0 || --tries == 0) {
                    break;
                }
            }
            m_segmentReorgCounter = 0;
        }

        if (ret == -1 || m_segmentHeaders[ret] == null) {
            // Original cost-benefit ratio: ((1-u)*age)/(1+u)
            for (int i = 0; i < m_segmentHeaders.length; i++) {
                currentSegment = m_segmentHeaders[i];
                if (currentSegment != null && m_reorgVector[i] == 0) {
                    costBenefitRatio = currentSegment.getUtilization() * currentSegment.getAge();
                    if (costBenefitRatio > max) {
                        max = costBenefitRatio;
                        ret = i;
                    }
                }
            }
        }

        if (ret != -1) {
            // Mark segment as being reorganized in this epoch
            m_reorgVector[ret] = 1;
        }

        return ret;
    }

    // Classes

    /**
     * Helper class to store and print statistics for recovery
     */
    static final class Statistics {

        // Attributes
        long m_timeToDetermineRanges;
        long m_timeToReadSegmentsFromDisk;
        long m_timeToCheck;
        int m_readVersionsFromArray;
        int m_readVersionsFromHashTable;
        int m_recoveredLargeChunks;

        /**
         * Constructor
         */
        private Statistics() {
            m_timeToDetermineRanges = 0;
            m_timeToReadSegmentsFromDisk = 0;
            m_timeToCheck = 0;
            m_readVersionsFromArray = 0;
            m_readVersionsFromHashTable = 0;
            m_recoveredLargeChunks = 0;
        }

        void merge(final Statistics p_stats) {
            m_timeToDetermineRanges += p_stats.m_timeToDetermineRanges;
            m_timeToReadSegmentsFromDisk += p_stats.m_timeToReadSegmentsFromDisk;
            m_timeToCheck += p_stats.m_timeToCheck;
            m_readVersionsFromArray += p_stats.m_readVersionsFromArray;
            m_readVersionsFromHashTable += p_stats.m_readVersionsFromHashTable;
            m_recoveredLargeChunks += p_stats.m_recoveredLargeChunks;
        }
    }

    /**
     * SegmentHeader
     *
     * @author Kevin Beineke 07.11.2014
     */
    private final class SegmentHeader {

        // Attributes
        private int m_index;
        private int m_usedBytes;
        private int m_lastAccess;
        private int m_averageAge;
        private boolean m_reorgInCurrEon;

        // Constructors

        /**
         * Creates an instance of SegmentHeader
         *
         * @param p_usedBytes
         *         the number of used bytes
         * @param p_index
         *         the index within the log
         */
        private SegmentHeader(final int p_index, final int p_usedBytes) {
            m_index = p_index;
            m_usedBytes = p_usedBytes;
            m_lastAccess = 0;
            m_averageAge = 0;
            m_reorgInCurrEon = true;
        }

        // Getter

        /**
         * Returns the utilization
         *
         * @return the utilization
         */
        private double getUtilization() {
            return (double) m_logSegmentSize / m_usedBytes;
        }

        /**
         * Returns the index
         *
         * @return the index
         */
        private int getIndex() {
            return m_index;
        }

        /**
         * Returns whether this segment is empty or not
         *
         * @return true if segment is empty, false otherwise
         */
        private boolean isEmpty() {
            return m_usedBytes == 0;
        }

        /**
         * Returns number of used bytes
         *
         * @return number of used bytes
         */
        private int getUsedBytes() {
            return m_usedBytes;
        }

        /**
         * Returns number of used bytes
         *
         * @return number of used bytes
         */
        private int getFreeBytes() {
            return m_logSegmentSize - m_usedBytes;
        }

        /**
         * Returns the age of this segment
         *
         * @return the age of this segment
         */
        private int getAge() {
            if (m_useTimestamps) {
                return m_averageAge + m_lastAccess;
            } else {
                return getCurrentTimeInSec() - m_lastAccess;
            }
        }

        /**
         * Sets the age
         *
         * @param p_newAge
         *         the new calculated age
         */
        private void setAge(final int p_newAge) {
            m_averageAge = p_newAge;
        }

        /**
         * Returns whether this segment was reorganized in current eon
         *
         * @return whether this segment was reorganized in current eon
         */
        private boolean wasReorganized() {
            return m_reorgInCurrEon;
        }

        // Setter

        /**
         * Updates the number of used bytes
         *
         * @param p_writtenBytes
         *         the number of written bytes
         */
        private void updateUsedBytes(final int p_writtenBytes) {
            m_usedBytes += p_writtenBytes;
            m_lastAccess = getCurrentTimeInSec();
        }

        /**
         * Sets the reorganization status for current eon
         */
        private void markSegmentAsReorganized() {
            m_reorgInCurrEon = true;
        }

        /**
         * Resets the reorganization status for new eon
         */
        private void beginEon() {
            m_reorgInCurrEon = false;
        }

        /**
         * Resets the segment header
         */
        private void reset() {
            m_usedBytes = 0;
            m_lastAccess = getCurrentTimeInSec();
            m_averageAge = 0;
        }
    }

    /**
     * Recovery helper thread. Determines ChunkID ranges for to be recovered backup range and recovers segments as well.
     */
    private class RecoveryHelperThread extends Thread {

        private RecoveryMetadata m_recoveryMetadata;
        private TemporaryVersionsStorage m_versionsForRecovery;
        private HashMap<Long, DSByteBuffer> m_largeChunks;
        private ReentrantLock m_largeChunkLock;
        private long m_lowestCID;
        private byte[] m_index;
        private ReentrantLock m_indexLock;
        private boolean m_doCRCCheck;
        private Statistics m_stats;
        private ChunkBackupComponent m_chunkComponent;

        RecoveryHelperThread(final RecoveryMetadata p_metadata, final TemporaryVersionsStorage p_versionsForRecovery,
                final HashMap<Long, DSByteBuffer> p_largeChunks, final ReentrantLock p_largeChunkLock, final long p_lowestCID, final byte[] p_index,
                final ReentrantLock p_indexLock, final boolean p_doCRCCheck, final ChunkBackupComponent p_chunkComponent) {
            m_recoveryMetadata = p_metadata;
            m_versionsForRecovery = p_versionsForRecovery;
            m_largeChunks = p_largeChunks;
            m_largeChunkLock = p_largeChunkLock;
            m_lowestCID = p_lowestCID;
            m_index = p_index;
            m_indexLock = p_indexLock;
            m_doCRCCheck = p_doCRCCheck;
            m_stats = new Statistics();
            m_chunkComponent = p_chunkComponent;
        }

        Statistics getStatistics() {
            return m_stats;
        }

        @Override
        public void run() {
            int idx = 0;
            while (true) {
                m_indexLock.lock();
                while (idx < m_index.length && m_index[idx] == 1) {
                    idx++;
                }
                if (idx == m_index.length) {
                    m_indexLock.unlock();
                    break;
                }
                m_index[idx] = 1;
                m_indexLock.unlock();

                if (m_segmentHeaders[idx] != null && !m_segmentHeaders[idx].isEmpty()) {
                    recoverSegment(idx, m_versionsForRecovery, m_lowestCID, m_recoveryMetadata, m_largeChunks, m_largeChunkLock, m_chunkComponent, m_doCRCCheck,
                            m_stats);
                }
                idx++;
            }
        }
    }

}

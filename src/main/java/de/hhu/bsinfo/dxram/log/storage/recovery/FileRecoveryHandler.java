package de.hhu.bsinfo.dxram.log.storage.recovery;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkByteBuffer;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractSecLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.header.ChecksumHandler;
import de.hhu.bsinfo.dxram.log.storage.logs.Log;

/**
 * To recover a log from file. To be used for instance after a cluster shutdown.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
public final class FileRecoveryHandler {

    /**
     * Private constructor.
     */
    private FileRecoveryHandler() {
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
     * @return ArrayList with all log entries as chunks
     * @throws IOException
     *         if the secondary log could not be read
     */
    public static AbstractChunk[] recoverFromFile(final String p_fileName, final String p_path,
            final boolean p_useChecksum, final long p_secondaryLogSize, final int p_logSegmentSize) throws IOException {
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
        HashMap<Long, AbstractChunk> chunkMap;
        AbstractSecLogEntryHeader logEntryHeader;

        // FIXME: this is very old and has not been tested

        /*
         * IMPORTANT: do not use version log to identify most the recent version of a chunk as the version log might
         * have been ahead of the secondary log resulting in not recovering the chunk at all. Instead, cache all
         * chunks from all segments (in a hash table) and overwrite entry if version is higher. Use the version log
         * to determine deleted chunks, only.
         */

        nodeID = Short.parseShort(p_fileName.split("_")[0].substring(1));
        storesMigrations = p_fileName.contains("M");

        chunkMap = new HashMap<Long, AbstractChunk>();

        segments = readAllSegmentsFromFile(p_path + p_fileName, p_secondaryLogSize, p_logSegmentSize);

        while (i < segments.length) {
            segment = segments[i].getBuffer();
            if (segment != null) {
                while (offset < segment.capacity() && segment.get(offset) != 0) {
                    short type = (short) (segment.get(offset) & 0xFF);
                    // Determine header of next log entry
                    logEntryHeader = AbstractSecLogEntryHeader.getHeader(type);
                    if (storesMigrations) {
                        chunkID = logEntryHeader.getCID(segment, offset);
                    } else {
                        chunkID = ((long) nodeID << 48) + logEntryHeader.getCID(segment, offset);
                    }
                    payloadSize = logEntryHeader.getLength(type, segment, offset);
                    if (p_useChecksum) {
                        checksum = logEntryHeader.getChecksum(type, segment, offset);
                    }
                    logEntrySize = logEntryHeader.getHeaderSize(type) + payloadSize;

                    // Read payload and create chunk
                    if (offset + logEntrySize <= segment.capacity()) {
                        // Create chunk only if log entry complete
                        payload = ByteBuffer.allocate(payloadSize);
                        segment.position(offset + logEntryHeader.getHeaderSize(type));
                        segment.limit(segment.position() + payloadSize);
                        payload.put(segment);
                        if (p_useChecksum &&
                                ChecksumHandler.calculateChecksumOfPayload(segments[i], 0, payloadSize) != checksum) {
                            // Ignore log entry
                            offset += logEntrySize;
                            continue;
                        }
                        chunkMap.put(chunkID, new ChunkByteBuffer(chunkID, payload));
                    }
                    offset += logEntrySize;
                }
            }
            offset = 0;
            i++;
        }

        return chunkMap.values().toArray(new AbstractChunk[0]);
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
     * @return all data
     * @throws IOException
     *         if the secondary log could not be read
     * @note executed only by reorganization thread
     */
    private static DirectByteBufferWrapper[] readAllSegmentsFromFile(final String p_path, final long p_secondaryLogSize,
            final int p_logSegmentSize) throws IOException {
        DirectByteBufferWrapper[] result;
        int numberOfSegments;

        numberOfSegments = (int) (p_secondaryLogSize / p_logSegmentSize);
        Object log = Log.openLog(new File(p_path));
        result = new DirectByteBufferWrapper[numberOfSegments];
        for (int i = 0; i < numberOfSegments; i++) {
            result[i] = new DirectByteBufferWrapper(p_logSegmentSize, true);
            Log.readFromFile(log, result[i], p_logSegmentSize, i * p_logSegmentSize);
        }
        Log.closeLog(log);

        return result;
    }
}

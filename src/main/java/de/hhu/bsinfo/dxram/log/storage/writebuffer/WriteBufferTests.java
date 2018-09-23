package de.hhu.bsinfo.dxram.log.storage.writebuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractPrimLogEntryHeader;
import de.hhu.bsinfo.dxram.log.storage.header.ChecksumHandler;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.Version;

/**
 * Helper class for verifying the written data in write buffer.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
final class WriteBufferTests {

    private static final Logger LOGGER = LogManager.getFormatterLogger(WriteBufferTests.class.getSimpleName());

    /**
     * Private constructor.
     */
    private WriteBufferTests() {
    }

    /**
     * Checks the header after creation.
     *
     * @param p_header
     *         the created header
     * @param p_logEntryHeader
     *         the header access
     * @param p_chunkID
     *         the chunk ID
     * @param p_version
     *         the version
     * @param p_rangeID
     *         the range ID
     * @param p_owner
     *         the owner
     * @param p_timestamp
     *         the timestamp
     * @param p_numberOfHeaders
     *         the number of headers
     * @return whether the header is correct or not
     */
    static boolean checkHeader(final ByteBuffer p_header, final AbstractPrimLogEntryHeader p_logEntryHeader,
            final long p_chunkID, final Version p_version, final short p_rangeID, final short p_owner,
            final int p_timestamp, final int p_numberOfHeaders) {
        boolean ret = true;

        short type = (short) (p_header.get(0) & 0xFF);
        if (p_owner == ChunkID.getCreatorID(p_chunkID) ? (type & 0x1) != 0 : (type & 0x1) != 1) {
            LOGGER.error("Header field check: Migration field in type 0b%s is wrong.", Integer.toBinaryString(type));
            ret = false;
        }
        if (p_numberOfHeaders == 1 && (type & 0x2) != 0 || p_numberOfHeaders > 1 && (type & 0x2) != 2) {
            LOGGER.error("Header field check: Chaining field in type 0b%s is wrong.", Integer.toBinaryString(type));
            ret = false;
        }

        if (p_owner != p_logEntryHeader.getOwner(p_header, 0)) {
            LOGGER.error("Header field check: Different owner 0x%x != 0x%x in created header.", p_owner,
                    p_logEntryHeader.getOwner(p_header, 0));
            ret = false;
        }
        if (p_rangeID != p_logEntryHeader.getRangeID(p_header, 0)) {
            LOGGER.error("Header field check: Different range %d != %d in created header.", p_rangeID,
                    p_logEntryHeader.getRangeID(p_header, 0));
            ret = false;
        }
        if (p_chunkID != p_logEntryHeader.getCID(p_header, 0)) {
            LOGGER.error("Header field check: Different chunk ID 0x%x != 0x%x in created header.", p_chunkID,
                    p_logEntryHeader.getCID(p_header, 0));
            ret = false;
        }
        Version v = p_logEntryHeader.getVersion(p_header, 0);
        if (!p_version.isEqual(v)) {
            LOGGER.error("Header field check: Different version %d,%d != %d,%d in created header.",
                    p_version.getEpoch(), p_version.getVersion(), v.getEpoch(), v.getVersion());
            ret = false;
        }
        if (p_timestamp != -1 && p_timestamp != p_logEntryHeader.getTimestamp(p_header, 0)) {
            LOGGER.error("Header field check: Different chain size %d != %d in created header.", p_timestamp,
                    p_logEntryHeader.getChainSize(p_header, 0));
            ret = false;
        }

        // ChainID and checksum are added and tested later

        return ret;
    }

    /**
     * Checks the header and payload after writing to write buffer.
     *
     * @param p_bufferWrapper
     *         the buffer wrapper to access the write buffer
     * @param p_writePointer
     *         the write pointer before writing
     * @param p_writeSize
     *         the number of written bytes
     * @param p_byteUntilEnd
     *         the bytes until the end of the ring buffer or read pointer
     * @param p_header
     *         the created header
     * @param p_headerSize
     *         the header size
     * @param p_checksum
     *         the payload checksum
     * @param p_timestamp
     *         the timestamp
     * @param p_chainID
     *         the chain ID
     * @return whether the written data is correct or not
     */
    static boolean checkWriteAccess(final DirectByteBufferWrapper p_bufferWrapper, final int p_writePointer,
            final int p_writeSize, final int p_byteUntilEnd, final ByteBuffer p_header, final int p_headerSize,
            final int p_checksum, final int p_timestamp, final byte p_chainID) {
        boolean ret = true;
        int headerOffset = p_writePointer;
        short headerSize;
        ByteBuffer header;
        AbstractPrimLogEntryHeader logEntryHeader;

        ByteBuffer buffer = p_bufferWrapper.getBuffer().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        short type = (short) (buffer.get(p_writePointer) & 0xFF);

        // Check header
        logEntryHeader = AbstractPrimLogEntryHeader.getHeader();
        headerSize = logEntryHeader.getHeaderSize(type);
        if (headerSize != p_headerSize) {
            LOGGER.error(
                    "Header size check: Size of written header differs from given header. Different size %d != %d. " +
                            "Type: 0b%s != 0b%s.", headerSize, p_headerSize, Integer.toBinaryString(type),
                    Integer.toBinaryString(p_header.get(0)));
            ret = false;
        }

        if (headerSize <= p_byteUntilEnd) {
            header = buffer;
        } else {
            // Buffer overflow -> header is split
            if (buffer.isDirect()) {
                header = ByteBuffer.allocateDirect(headerSize);
            } else {
                header = ByteBuffer.allocate(headerSize);
            }
            header.order(ByteOrder.LITTLE_ENDIAN);

            buffer.position(p_writePointer);
            header.put(buffer);

            buffer.position(0);
            buffer.limit(headerSize - p_byteUntilEnd);
            header.put(buffer);
            buffer.limit(buffer.capacity());

            type = (short) (header.get(0) & 0xFF);
            headerOffset = 0;
        }

        if (logEntryHeader.getLength(type, header, headerOffset) != logEntryHeader.getLength(type, p_header, 0)) {
            LOGGER.error(
                    "Header size check: Written header differs from created header. Different payload length %d != %d.",
                    logEntryHeader.getLength(type, header, headerOffset), logEntryHeader.getLength(type, p_header, 0));
            ret = false;
        }

        if (type != (p_header.get(0) & 0xFF)) {
            LOGGER.error("Header field check: Written header differs from created header. Different type 0b%s != 0b%s.",
                    Integer.toBinaryString(type), Integer.toBinaryString(p_header.get(0) & 0xFF));
            ret = false;
        }
        if (logEntryHeader.getOwner(header, headerOffset) != logEntryHeader.getOwner(p_header, 0)) {
            LOGGER.error(
                    "Header field check: Written header differs from created header. Different owner 0x%x != 0x%x.",
                    logEntryHeader.getOwner(header, headerOffset), logEntryHeader.getOwner(p_header, 0));
            ret = false;
        }
        if (logEntryHeader.getRangeID(header, headerOffset) != logEntryHeader.getRangeID(p_header, 0)) {
            LOGGER.error("Header field check: Written header differs from created header. Different range %d != %d.",
                    logEntryHeader.getRangeID(header, headerOffset), logEntryHeader.getRangeID(p_header, 0));
            ret = false;
        }
        if (logEntryHeader.getCID(header, headerOffset) != logEntryHeader.getCID(p_header, 0)) {
            LOGGER.error("Header field check: Written header differs from created header. Different chunk ID 0x%x !=" +
                    " 0x%x.", logEntryHeader.getCID(header, headerOffset), logEntryHeader.getCID(p_header, 0));
            ret = false;
        }
        Version v1 = logEntryHeader.getVersion(header, headerOffset);
        Version v2 = logEntryHeader.getVersion(p_header, 0);
        if (!v1.isEqual(v2)) {
            LOGGER.error("Header field check: Written header differs from created header. Different version %d,%d " +
                    "!= %d,%d.", v1.getEpoch(), v1.getVersion(), v2.getEpoch(), v2.getVersion());
            ret = false;
        }
        if (logEntryHeader.isChained(type) && logEntryHeader.getChainID(header, headerOffset) != p_chainID) {
            LOGGER.error(
                    "Header field check: Written header differs from created header. Different chain ID %d !=" + " %d.",
                    logEntryHeader.getChainID(header, headerOffset), p_chainID);
            ret = false;
        }
        if (logEntryHeader.isChained(type) &&
                logEntryHeader.getChainSize(header, headerOffset) != logEntryHeader.getChainSize(p_header, 0)) {
            LOGGER.error(
                    "Header field check: Written header differs from created header. Different chain size %d " + "!=" +
                            " %d.", logEntryHeader.getChainSize(header, headerOffset),
                    logEntryHeader.getChainSize(p_header, 0));
            ret = false;
        }
        if (ChecksumHandler.getCRCSize() != 0 && logEntryHeader.getChecksum(type, header, headerOffset) != p_checksum) {
            LOGGER.error(
                    "Header field check: Written header differs from created header. Different checksum %d !=" + " %d.",
                    logEntryHeader.getChecksum(type, header, headerOffset), p_checksum);
            ret = false;
        }
        if (p_timestamp != -1 && logEntryHeader.getTimestamp(header, headerOffset) != p_timestamp) {
            LOGGER.error(
                    "Header field check: Written header differs from created header. Different chain size %d " + "!=" +
                            " %d.", logEntryHeader.getChainSize(header, headerOffset), p_timestamp);
            ret = false;
        }

        // Check payload
        int payloadOffset = (p_writePointer + headerSize) % buffer.capacity();
        if (ChecksumHandler.getCRCSize() > 0) {
            if (p_writeSize + headerSize < p_byteUntilEnd || headerSize >= p_byteUntilEnd) {
                int checksum = ChecksumHandler.calculateChecksumOfPayload(p_bufferWrapper, payloadOffset, p_writeSize);
                if (checksum != p_checksum) {
                    LOGGER.error(
                            "Payload check #1: Written payload differs from given payload. Different checksum %d " +
                                    "!=" + " %d.", checksum, p_checksum);
                    ret = false;
                }
            } else {
                // Buffer overflow -> payload is split
                DirectByteBufferWrapper tempWrapper = new DirectByteBufferWrapper(p_writeSize, false);

                ByteBuffer temp = tempWrapper.getBuffer();
                buffer.position(payloadOffset);
                temp.put(buffer);

                buffer.position(0);
                buffer.limit(p_writeSize - (p_byteUntilEnd - headerSize));
                temp.put(buffer);

                int checksum = ChecksumHandler.calculateChecksumOfPayload(tempWrapper, 0, p_writeSize);
                if (checksum != p_checksum) {
                    LOGGER.error(
                            "Payload check #2: Written payload differs from given payload. Different checksum %d " +
                                    "!=" + " %d.", checksum, p_checksum);
                    ret = false;
                }
            }
        }

        if (!ret) {
            LOGGER.error("\t WriteBuffer: %s, PayloadOffset: %d, WriteSize: %d", p_bufferWrapper.getBuffer(),
                    payloadOffset, p_writeSize);
        }

        return ret;
    }

}

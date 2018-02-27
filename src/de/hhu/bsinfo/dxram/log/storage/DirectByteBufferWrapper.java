package de.hhu.bsinfo.dxram.log.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import de.hhu.bsinfo.dxutils.ByteBufferHelper;

/**
 * Wrapper class for ByteBuffer and off-heap address
 */
public class DirectByteBufferWrapper {

    private static int ms_pageSize;
    private static boolean ms_native;

    private ByteBuffer m_buffer;
    private long m_addr;

    /**
     * Creates an instance of DirectBufferWrapper
     *
     * @param p_size
     *         the buffer size
     */
    DirectByteBufferWrapper(final int p_size) {

        if (ms_native) {
            // Allocate direct byte buffer page-aligned and with (at least) one overlapping page at the front.
            // 1. Allocate direct byte buffer with two (if size is page-aligned) additional flash pages
            // 2a. If byte buffer is already page-aligned, hide one additional flash page by slicing
            // 2b. If not, move position to beginning of second next page and slice
            int size = p_size + 2 * ms_pageSize;
            if (p_size % ms_pageSize != 0) {
                // If p_size is not page-aligned, we need to get the entire last page as well
                size += ms_pageSize - p_size % ms_pageSize;
            }
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            long address = ByteBufferHelper.getDirectAddress(buffer);

            if (address % ms_pageSize == 0) {
                // Allocated ByteBuffer is already page-aligned -> move beginning to second page
                buffer.position(ms_pageSize);
                // Move end of to be sliced ByteBuffer to p_size (we two overlapping pages at the end)
                buffer.limit(ms_pageSize + p_size);
                // Slice buffer to hide alignment
                m_buffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
                // Address points on first byte to write
                m_addr = address + ms_pageSize;
            } else {
                // Allocated ByteBuffer is NOT page-aligned -> move beginning to third page
                int newPosition = (int) (ms_pageSize - address % ms_pageSize + ms_pageSize);
                buffer.position(newPosition);
                buffer.limit(newPosition + p_size);
                m_buffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
                m_addr = address + newPosition;
            }
        } else {
            m_buffer = ByteBuffer.allocate(p_size);
            m_buffer.order(ByteOrder.LITTLE_ENDIAN);
            m_addr = -1;
        }
    }

    /**
     * Creates an instance of DirectBufferWrapper
     */
    DirectByteBufferWrapper() {
        // Single zeroed page
        ByteBuffer buffer = ByteBuffer.allocateDirect(3 * ms_pageSize);
        long address = ByteBufferHelper.getDirectAddress(buffer);

        if (address % ms_pageSize == 0) {
            buffer.position(0);
            buffer.limit(ms_pageSize);
            m_buffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
            m_addr = address;
        } else {
            // Allocated ByteBuffer is NOT page-aligned -> move beginning
            int newPosition = (int) (ms_pageSize + ms_pageSize - address % ms_pageSize);
            buffer.position(newPosition);
            buffer.limit(newPosition + ms_pageSize);
            m_buffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
            m_addr = address + newPosition;
        }
    }

    /**
     * Set page size. Must be called before logging is started.
     *
     * @param p_pageSize
     *         the flash page size
     */
    public static void setPageSize(final int p_pageSize) {
        ms_pageSize = p_pageSize;
    }

    /**
     * Whether to use native ByteBuffers or heap ByteBuffers for reading/writing from/to files.
     *
     * @param p_useNativeBuffers
     *         whether to use native ByteBuffers (true) or heap ByteBuffers (false)
     */
    public static void useNativeBuffers(final boolean p_useNativeBuffers) {
        ms_native = p_useNativeBuffers;
    }

    /**
     * Get the ByteBuffer
     *
     * @return the ByteBuffer
     */
    public ByteBuffer getBuffer() {
        return m_buffer;
    }

    /**
     * Get the off-heap address
     *
     * @return the address
     */
    public long getAddress() {
        return m_addr;
    }
}

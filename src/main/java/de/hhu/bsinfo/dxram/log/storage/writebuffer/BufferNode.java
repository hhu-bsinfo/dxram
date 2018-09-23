/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating
 * Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.storage.writebuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.header.AbstractPrimLogEntryHeader;

/**
 * Stores all buffers of one backup range to be written to disk during flushing of the write buffer.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 11.08.2014
 */
final class BufferNode {

    private final ArrayList<DirectByteBufferWrapper> m_segments;
    private final boolean m_convert;

    private int m_currentSegment;

    /**
     * Creates an instance of BufferNode
     *
     * @param p_length
     *         the buffer length (the length might change after converting the headers and fitting the data into
     *         segments)
     * @param p_convert
     *         whether the log entry headers have to be converted or not
     * @param p_logSegmentSize
     *         the size of a log segment to estimate the number of required segments (might be higher eventually)
     * @param p_bufferPool
     *         the buffer pool for acquiring buffers to store
     */
    BufferNode(final int p_length, final boolean p_convert, final int p_logSegmentSize, final BufferPool p_bufferPool) {
        int length = p_length;

        m_currentSegment = 0;
        m_convert = p_convert;

        m_segments = new ArrayList<DirectByteBufferWrapper>((int) Math.ceil((double) length / p_logSegmentSize));

        for (int i = 0; length > 0; i++) {
            m_segments.add(p_bufferPool.getBuffer(length, false));
            int size = m_segments.get(i).getBuffer().capacity();
            length -= size;
        }
    }

    /**
     * Returns whether this buffer is for secondary log or not (primary log)
     *
     * @return whether the entries have been converted or not
     */
    boolean readyForSecLog() {
        return m_convert;
    }

    /**
     * Returns the number of written bytes per segment
     *
     * @param p_index
     *         the index
     * @return the number of written bytes per segment
     */
    int getSegmentLength(final int p_index) {
        int ret = 0;

        if (p_index < m_segments.size()) {
            ret = m_segments.get(p_index).getBuffer().position();
        }

        return ret;
    }

    /**
     * Returns the buffer
     *
     * @param p_index
     *         the index
     * @return the buffer
     */
    DirectByteBufferWrapper getSegmentWrapper(final int p_index) {
        DirectByteBufferWrapper ret = null;

        if (p_index < m_segments.size()) {
            ret = m_segments.get(p_index);
        }

        return ret;
    }

    /**
     * Appends data to node buffer
     *
     * @param p_primaryWriteBuffer
     *         the buffer
     * @param p_offset
     *         the offset within the buffer
     * @param p_logEntrySize
     *         the log entry size
     * @param p_bytesUntilEnd
     *         the number of bytes until end
     * @param p_conversionOffset
     *         the conversion offset
     * @param p_bufferPool
     *         the buffer pool for acquiring buffers to write to
     */
    void appendToBuffer(final ByteBuffer p_primaryWriteBuffer, final int p_offset, final int p_logEntrySize,
            final int p_bytesUntilEnd, final short p_conversionOffset, final BufferPool p_bufferPool) {
        int logEntrySize;
        ByteBuffer segment;

        if (m_convert) {
            logEntrySize = p_logEntrySize - (p_conversionOffset - 1);
        } else {
            logEntrySize = p_logEntrySize;
        }

        segment = m_segments.get(m_currentSegment).getBuffer();
        if (logEntrySize > segment.remaining()) {
            if (m_currentSegment + 1 == m_segments.size()) {
                // We need another segment because of fragmentation
                m_segments.add(p_bufferPool.getBuffer(logEntrySize, false));
            }
            segment = m_segments.get(++m_currentSegment).getBuffer();

            if (segment.remaining() < logEntrySize) {
                // Chunk is larger than current segment -> remove this and all following segments and get
                // buffer that is large enough
                for (int i = m_currentSegment; i < m_segments.size(); i++) {
                    p_bufferPool.returnBuffer(m_segments.remove(i));
                }
                DirectByteBufferWrapper wrapper = p_bufferPool.getBuffer(logEntrySize, true);
                m_segments.add(wrapper);
                segment = wrapper.getBuffer();
            }
        }

        if (m_convert) {
            // More secondary log buffer size for this node: Convert primary log entry header to secondary
            // log header and append entry to node buffer
            AbstractPrimLogEntryHeader
                    .convertAndPut(p_primaryWriteBuffer, p_offset, segment, p_logEntrySize, p_bytesUntilEnd,
                            p_conversionOffset);
        } else {
            // Less secondary log buffer size for this node: Just append entry to node buffer without
            // converting the log entry header
            if (p_logEntrySize <= p_bytesUntilEnd) {
                p_primaryWriteBuffer.position(p_offset);
                p_primaryWriteBuffer.limit(p_offset + p_logEntrySize);
                segment.put(p_primaryWriteBuffer);
            } else {
                p_primaryWriteBuffer.position(p_offset);
                segment.put(p_primaryWriteBuffer);

                p_primaryWriteBuffer.position(0);
                p_primaryWriteBuffer.limit(p_logEntrySize - p_bytesUntilEnd);
                segment.put(p_primaryWriteBuffer);
            }
        }
        p_primaryWriteBuffer.limit(p_primaryWriteBuffer.capacity());
    }

}

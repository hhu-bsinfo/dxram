/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.recovery;

/**
 * Object to hand metadata over recovered chunks
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 08.11.2016
 */
public class RecoveryMetadata {

    private long[] m_chunkIDRanges;
    private int m_numberOfChunks;
    private int m_size;

    /**
     * Constructor
     */
    public RecoveryMetadata() {
        m_chunkIDRanges = null;
        m_numberOfChunks = 0;
        m_size = 0;
    }

    /**
     * Constructor
     *
     * @param p_chunkIDRanges
     *     the ChunkIDs of all recovered chunks arranged in ranges
     * @param p_numberOfChunks
     *     the actual number of ChunkIDs in array
     * @param p_size
     *     the number of bytes recovered
     */
    public RecoveryMetadata(final long[] p_chunkIDRanges, final int p_numberOfChunks, final int p_size) {
        m_chunkIDRanges = p_chunkIDRanges;
        m_numberOfChunks = p_numberOfChunks;
        m_size = p_size;
    }

    /**
     * Sets the ChunkID ranges
     * @param p_chunkIDRanges
     * the ChunkID ranges of all recovered chunks
     */
    public void setChunkIDRanges(final long[] p_chunkIDRanges) {
        m_chunkIDRanges = p_chunkIDRanges;
    }

    /**
     * Registers chunk
     *
     * @param p_size
     *     the size of the chunk (header + payload)
     */
    public void add(final int p_size) {
        m_numberOfChunks++;
        m_size += p_size;
    }

    /**
     * Returns the number of chunks
     *
     * @return the number of chunks
     */
    public int getNumberOfChunks() {
        return m_numberOfChunks;
    }

    /**
     * Returns the size of all recovered chunks
     *
     * @return the size
     */
    public int getSizeInBytes() {
        return m_size;
    }

    /**
     * Returns all ChunkID ranges
     *
     * @return the ChunkID ranges
     */
    public long[] getCIDRanges() {
        return m_chunkIDRanges;
    }

}

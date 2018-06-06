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

package de.hhu.bsinfo.dxram.recovery;

/**
 * Object to store metadata of a finished recovery to replicate data later
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.06.2017
 */
public class FinishedRecovery {

    private short m_rangeID;
    private short m_replacementBackupPeer;
    private long[] m_chunkIDRanges;
    private int m_numberOfChunks;

    /**
     * Constructor
     */
    public FinishedRecovery(final short p_replacementBackupPeer, final long[] p_chunkIDRanges, final int p_numberOfChunks, final short p_rangeID) {
        m_replacementBackupPeer = p_replacementBackupPeer;
        m_chunkIDRanges = p_chunkIDRanges;
        m_numberOfChunks = p_numberOfChunks;
        m_rangeID = p_rangeID;
    }

    /**
     * Returns the replacement backup peer (gets all data)
     *
     * @return the NodeID
     */
    public short getReplacementBackupPeer() {
        return m_replacementBackupPeer;
    }

    /**
     * Returns all ChunkID ranges
     *
     * @return the ChunkID ranges
     */
    public long[] getCIDRanges() {
        return m_chunkIDRanges;
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
     * Returns the RangeID
     *
     * @return the RangeID
     */
    public short getRangeID() {
        return m_rangeID;
    }

}

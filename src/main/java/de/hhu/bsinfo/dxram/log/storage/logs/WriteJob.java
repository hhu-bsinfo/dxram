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

package de.hhu.bsinfo.dxram.log.storage.logs;

import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;

/**
 * This class is used to return a job for writing to a log.
 * There is only one instance of this class as the writer thread is the only user.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 15.09.2018
 */
final class WriteJob {
    private static final WriteJob INSTANCE = new WriteJob();

    private WriterJobQueue.JobID m_jobID;
    private int m_entrySize;
    private DirectByteBufferWrapper m_bufferWrapper;
    private int m_combinedRangeID;

    /**
     * Private constructor.
     */
    private WriteJob() {
    }

    /**
     * Sets the attributes and returns the instance.
     *
     * @param p_jobID
     *         the job ID
     * @param p_entrySize
     *         the total number of bytes to write
     * @param p_bufferWrapper
     *         the buffer to write
     * @param p_combinedRangeID
     *         the RangeID and owner NodeID
     * @return the instance
     */
    static WriteJob getInstance(final WriterJobQueue.JobID p_jobID, final int p_entrySize,
            final DirectByteBufferWrapper p_bufferWrapper, final int p_combinedRangeID) {
        INSTANCE.m_jobID = p_jobID;
        INSTANCE.m_entrySize = p_entrySize;
        INSTANCE.m_bufferWrapper = p_bufferWrapper;
        INSTANCE.m_combinedRangeID = p_combinedRangeID;

        return INSTANCE;
    }

    /**
     * Returns the job ID.
     *
     * @return the job ID
     */
    WriterJobQueue.JobID getJobID() {
        return m_jobID;
    }

    /**
     * Returns the number of bytes to write.
     *
     * @return the entry size
     */
    int getEntrySize() {
        return m_entrySize;
    }

    /**
     * Returns buffer to write.
     *
     * @return the byte buffer wrapper
     */
    DirectByteBufferWrapper getBuffer() {
        return m_bufferWrapper;
    }

    /**
     * Returns the RangeID and the owner NodeID.
     *
     * @return the combined range ID
     */
    int getCombinedRangeID() {
        return m_combinedRangeID;
    }
}

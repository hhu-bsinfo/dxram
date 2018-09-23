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

import de.hhu.bsinfo.dxnet.core.NetworkRuntimeException;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxutils.UnsafeHandler;

/**
 * The WriterJobQueue stores jobs in order to write to disk.
 * Uses a ring-buffer implementation.
 * One producer (network thread) and one consumer (message creation coordinator).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 23.02.2018
 */
final class WriterJobQueue {

    // Must be a power of two to work with wrap around
    // Do not enlarge this value if you don't have a good reason
    // If you change this value, consider changing the buffer pool defaults as well
    private static final int SIZE = 4;

    private final JobID[] m_jobIDs;
    private final int[] m_entrySizes;
    private final DirectByteBufferWrapper[] m_bufferWrappers;
    private final int[] m_combinedRangeIDs;

    // single producer, single consumer lock free queue (posBack and posFront are synchronized
    // with fences and byte counter)
    private int m_posBack; // 31 bits used (see incrementation)
    private int m_posFront; // 31 bits used (see incrementation)

    /**
     * Creates an instance of WriterJobQueue.
     */
    WriterJobQueue() {
        if ((SIZE & SIZE - 1) != 0) {
            throw new NetworkRuntimeException("Buffer queue size must be a power of 2!");
        }

        m_jobIDs = new JobID[SIZE];
        m_entrySizes = new int[SIZE];
        m_bufferWrappers = new DirectByteBufferWrapper[SIZE];
        m_combinedRangeIDs = new int[SIZE];

        m_posBack = 0;
        m_posFront = 0;
    }

    /**
     * Returns the capacity.
     *
     * @return the capacity
     */
    static int getCapacity() {
        return SIZE;
    }

    /**
     * Removes an job from queue.
     */
    WriteJob popJob() {

        UnsafeHandler.getInstance().getUnsafe().loadFence();
        if (m_posBack == m_posFront) {
            // Empty
            return null;
        }

        int posBack = m_posBack % SIZE;
        JobID jobID = m_jobIDs[posBack];
        DirectByteBufferWrapper bufferWrapper = m_bufferWrappers[posBack];
        int entrySize = m_entrySizes[posBack];
        int combinedRangeID = m_combinedRangeIDs[posBack];
        UnsafeHandler.getInstance().getUnsafe().storeFence();

        // & 0x7FFFFFFF kill sign
        m_posBack = m_posBack + 1 & 0x7FFFFFFF;
        UnsafeHandler.getInstance().getUnsafe().storeFence();

        return WriteJob.getInstance(jobID, entrySize, bufferWrapper, combinedRangeID);
    }

    /**
     * Adds a job to the end of the ring buffer.
     *
     * @param p_jobID
     *         the job ID
     * @param p_bufferWrapper
     *         the aligned byte buffer
     * @param p_logEntrySize
     *         the length
     * @param p_combinedRangeID
     *         the RangeID and owner NodeID
     */
    void pushJob(final JobID p_jobID, final DirectByteBufferWrapper p_bufferWrapper, final int p_logEntrySize,
            final int p_combinedRangeID) {
        int front;

        UnsafeHandler.getInstance().getUnsafe().loadFence();
        if ((m_posBack + SIZE & 0x7FFFFFFF) == m_posFront) {
            // Queue is full -> wait
            while ((m_posBack + SIZE & 0x7FFFFFFF) == m_posFront) {
                //LockSupport.parkNanos(100);
                Thread.yield();
                UnsafeHandler.getInstance().getUnsafe().loadFence();
            }
        }

        front = m_posFront % SIZE;

        m_jobIDs[front] = p_jobID;
        m_entrySizes[front] = p_logEntrySize;
        m_bufferWrappers[front] = p_bufferWrapper;
        m_combinedRangeIDs[front] = p_combinedRangeID;

        // & 0x7FFFFFFF kill sign
        m_posFront = m_posFront + 1 & 0x7FFFFFFF;
        UnsafeHandler.getInstance().getUnsafe().storeFence();
    }

    /**
     * Enumeration for write jobs.
     *
     * @author Kevin Beineke, kevin.beineke@hhu.de, 23.02.2018
     */
    enum JobID {
        SEC_LOG_RETURN_BUFFER, SEC_LOG, PRIM_LOG
    }
}

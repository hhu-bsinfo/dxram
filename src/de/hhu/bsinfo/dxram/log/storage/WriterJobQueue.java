/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import java.io.IOException;

import de.hhu.bsinfo.dxnet.core.NetworkRuntimeException;
import de.hhu.bsinfo.dxutils.UnsafeHandler;

/**
 * The WriterJobQueue stores jobs in order to write to disk.
 * Uses a ring-buffer implementation.
 * One producer (network thread) and one consumer (message creation coordinator).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 23.02.2018
 */
class WriterJobQueue {

    // Must be a power of two to work with wrap around
    // If you change this value, consider changing the buffer pool defaults as well
    private static final int SIZE = 4;

    private WriterThread m_writerThread;

    private byte[] m_jobIDs;
    private SecondaryLogBuffer[] m_secLogBuffers;
    private DirectByteBufferWrapper[] m_bufferWrappers;
    private int[] m_entrySizes;

    private PrimaryWriteBuffer m_primaryWriteBuffer;
    private PrimaryLog m_primaryLog;

    // single producer, single consumer lock free queue (posBack and posFront are synchronized with fences and byte counter)
    private int m_posBack; // 31 bits used (see incrementation)
    private int m_posFront; // 31 bits used (see incrementation)

    /**
     * Creates an instance of WriterJobQueue
     */
    WriterJobQueue(final PrimaryWriteBuffer p_primaryWriteBuffer, final PrimaryLog p_primaryLog) {
        if ((SIZE & SIZE - 1) != 0) {
            throw new NetworkRuntimeException("Buffer queue size must be a power of 2!");
        }

        m_primaryWriteBuffer = p_primaryWriteBuffer;
        m_primaryLog = p_primaryLog;

        m_jobIDs = new byte[SIZE];
        m_secLogBuffers = new SecondaryLogBuffer[SIZE];
        m_bufferWrappers = new DirectByteBufferWrapper[SIZE];
        m_entrySizes = new int[SIZE];

        m_posBack = 0;
        m_posFront = 0;

        m_writerThread = new WriterThread();
        m_writerThread.setName("Logging: Writer Thread");
        m_writerThread.start();
    }

    /**
     * Shuts down the writer thread.
     */
    void shutdown() {
        m_writerThread.shutdown();
    }

    /**
     * Removes an job from queue and executes it.
     */
    private boolean popAndExecuteJob() throws IOException, InterruptedException {

        UnsafeHandler.getInstance().getUnsafe().loadFence();
        if (m_posBack == m_posFront) {
            // Empty
            return false;
        }

        int posBack = m_posBack % SIZE;
        byte jobID = m_jobIDs[posBack];
        SecondaryLogBuffer secLogBuffer = m_secLogBuffers[posBack];
        DirectByteBufferWrapper bufferWrapper = m_bufferWrappers[posBack];
        int entrySize = m_entrySizes[posBack];

        // & 0x7FFFFFFF kill sign
        m_posBack = m_posBack + 1 & 0x7FFFFFFF;

        if (jobID == 0) {
            secLogBuffer.flushAllDataToSecLog(bufferWrapper, entrySize);
            m_primaryWriteBuffer.returnBuffer(bufferWrapper);
        } else if (jobID == 1) {
            m_primaryLog.appendData(bufferWrapper, bufferWrapper.getBuffer().position());
        }

        return true;
    }

    /**
     * Adds a job to the end of the ring buffer.
     *
     * @param p_jobID
     *         the job ID (0: write to secondary log, 1: write to primary log)
     * @param p_secLogBuffer
     *         the SecondaryLogBuffer
     * @param p_bufferWrapper
     *         the aligned byte buffer
     * @param p_logEntrySize
     *         the length
     */
    void pushJob(final byte p_jobID, final SecondaryLogBuffer p_secLogBuffer, final DirectByteBufferWrapper p_bufferWrapper, final int p_logEntrySize) {
        int front;

        if ((m_posBack + SIZE & 0x7FFFFFFF) == m_posFront) {
            // Queue is full -> wait
            while ((m_posBack + SIZE & 0x7FFFFFFF) == m_posFront) {
                //LockSupport.parkNanos(100);
                Thread.yield();
            }
        }

        front = m_posFront % SIZE;

        m_jobIDs[front] = p_jobID;
        m_bufferWrappers[front] = p_bufferWrapper;
        if (p_jobID == 0) {
            m_secLogBuffers[front] = p_secLogBuffer;
            m_entrySizes[front] = p_logEntrySize;
        }

        // & 0x7FFFFFFF kill sign
        m_posFront = m_posFront + 1 & 0x7FFFFFFF;
    }

    /**
     * Writer thread. Pops jobs from job queue and writes containing data to disk.
     */
    private class WriterThread extends Thread {

        private volatile boolean m_run = true;

        /**
         * Constructor
         */
        WriterThread() {
        }

        /**
         * Shut down.
         */
        public void shutdown() {
            m_run = false;

            try {
                join();
            } catch (InterruptedException ignored) {
            }
        }

        @Override
        public void run() {

            while (m_run) {
                try {
                    if (!popAndExecuteJob()) {
                        //LockSupport.parkNanos(1);
                        Thread.yield();
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

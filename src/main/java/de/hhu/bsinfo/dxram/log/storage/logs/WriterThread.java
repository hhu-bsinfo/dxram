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

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.storage.BackupRangeCatalog;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.logs.secondarylog.SecondaryLogBuffer;
import de.hhu.bsinfo.dxram.log.storage.writebuffer.BufferPool;

/**
 * Writer thread. Pops jobs from job queue and writes containing data to disk.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.06.2017
 */
final class WriterThread extends Thread {

    private static final Logger LOGGER = LogManager.getFormatterLogger(WriterThread.class.getSimpleName());

    private final PrimaryLog m_primaryLog;
    private final BackupRangeCatalog m_backupRangeCatalog;

    private final WriterJobQueue m_jobQueue;
    private final BufferPool m_bufferPool;

    private volatile boolean m_isShuttingDown;

    /**
     * Creates an instance of WriterThread
     *
     * @param p_primaryLog
     *         the primary log
     * @param p_backupRangeCatalog
     *         the backup range catalog to find corresponding secondary log buffers
     * @param p_jobQueue
     *         the job queue
     * @param p_bufferPool
     *         the buffer pool to return buffers after writing
     */
    WriterThread(final PrimaryLog p_primaryLog, final BackupRangeCatalog p_backupRangeCatalog,
            final WriterJobQueue p_jobQueue, final BufferPool p_bufferPool) {
        m_primaryLog = p_primaryLog;
        m_backupRangeCatalog = p_backupRangeCatalog;

        m_jobQueue = p_jobQueue;
        m_bufferPool = p_bufferPool;
    }

    /**
     * Shuts down the writer thread.
     */
    public void shutdown() {
        m_isShuttingDown = true;

        try {
            join();
        } catch (InterruptedException e) {
            LOGGER.warn(e);
        }
    }

    @Override
    public void run() {
        while (!m_isShuttingDown) {
            try {
                if (!popAndExecuteJob()) {
                    //LockSupport.parkNanos(1);
                    Thread.yield();
                }
            } catch (final IOException e) {
                LOGGER.error(e);
            }
        }
    }

    /**
     * Removes an job from queue and executes it.
     */
    private boolean popAndExecuteJob() throws IOException {
        WriteJob job = m_jobQueue.popJob();

        if (job != null) {
            DirectByteBufferWrapper bufferWrapper = job.getBuffer();
            if (job.getJobID() == WriterJobQueue.JobID.SEC_LOG ||
                    job.getJobID() == WriterJobQueue.JobID.SEC_LOG_RETURN_BUFFER) {
                SecondaryLogBuffer secLogBuffer = m_backupRangeCatalog
                        .getSecondaryLogBuffer((short) (job.getCombinedRangeID() >> 16),
                                (short) job.getCombinedRangeID());
                if (secLogBuffer == null) {
                    LOGGER.error("Could not execute job as backup range does not exist");
                } else {
                    secLogBuffer.flushAllDataToSecLog(bufferWrapper, job.getEntrySize());
                }

                if (job.getJobID() == WriterJobQueue.JobID.SEC_LOG_RETURN_BUFFER) {
                    m_bufferPool.returnBuffer(job.getBuffer());
                }
            } else {
                m_primaryLog.postData(bufferWrapper, bufferWrapper.getBuffer().position());
            }

            return true;
        }
        return false;
    }
}

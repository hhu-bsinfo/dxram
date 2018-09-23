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

package de.hhu.bsinfo.dxram.log.storage.logs;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.log.storage.BackupRangeCatalog;
import de.hhu.bsinfo.dxram.log.storage.DirectByteBufferWrapper;
import de.hhu.bsinfo.dxram.log.storage.logs.secondarylog.SecondaryLogBuffer;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.TimePool;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * For writing log entries to disk (primary log).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 13.06.2014
 */
public final class PrimaryLog extends Log {

    private static final Logger LOGGER = LogManager.getFormatterLogger(PrimaryLog.class.getSimpleName());

    private static final TimePool SOP_WRITE_PRIMARY_LOG_TIME = new TimePool(PrimaryLog.class, "WritePrimaryLogTime");
    private static final ValuePool SOP_WRITE_PRIMARY_LOG_SIZE = new ValuePool(PrimaryLog.class, "WritePrimaryLogSize");

    static {
        StatisticsManager.get().registerOperation(PrimaryLog.class, SOP_WRITE_PRIMARY_LOG_TIME);
        StatisticsManager.get().registerOperation(PrimaryLog.class, SOP_WRITE_PRIMARY_LOG_SIZE);
    }

    private final BackupRangeCatalog m_backupRangeCatalog;

    private long m_writePos;
    private long m_numberOfBytes;

    /**
     * Creates an instance of PrimaryLog with user specific configuration
     *
     * @param p_backupRangeCatalog
     *         the backup range catalog used to find secondary log buffers for flushing
     * @param p_fileName
     *         the file name including the backup directory
     * @param p_primaryLogSize
     *         the size of a primary log
     * @param p_flashPageSize
     *         the size of flash page
     * @throws IOException
     *         if primary log could not be created
     */
    PrimaryLog(final BackupRangeCatalog p_backupRangeCatalog, final String p_fileName, final long p_primaryLogSize,
            final int p_flashPageSize) throws IOException {
        super(new File(p_fileName), p_primaryLogSize);

        m_backupRangeCatalog = p_backupRangeCatalog;

        m_writePos = 0;
        m_numberOfBytes = 0;

        if (m_logSize < p_flashPageSize) {
            throw new IllegalArgumentException("Error: primary log too small");
        }

        try {
            createLog();
        } catch (final IOException e) {
            throw new IOException("Error: primary log could not be created");
        }
    }

    /**
     * Posts data to be written to primary log.
     *
     * @param p_data
     *         the data
     * @param p_length
     *         the number of bytes to write
     * @throws IOException
     *         if data could not be written to disk
     */
    void postData(final DirectByteBufferWrapper p_data, final int p_length) throws IOException {
        if (m_logSize - m_numberOfBytes < p_length) {
            // Not enough free space in primary log -> flush to secondary logs and reset primary log
            clearPrimaryBuffer();
        }

        m_writePos = appendToLog(p_data, 0, p_length);
        m_numberOfBytes += p_length;
    }

    /**
     * Flushes all secondary log buffers in order to clear the primary log (everything written to secondary log
     * can be deleted from primary log)
     */
    private void clearPrimaryBuffer() {
        SecondaryLogBuffer[] buffers;

        for (int i = 0; i < Short.MAX_VALUE * 2 + 1; i++) {
            buffers = m_backupRangeCatalog.getAllSecondaryLogBuffers((short) i);
            if (buffers != null) {
                for (int j = 0; j < buffers.length; j++) {
                    if (buffers[j] != null && !buffers[j].isBufferEmpty()) {
                        try {
                            buffers[j].flushSecLogBuffer();
                        } catch (final IOException e) {
                            LOGGER.error("Secondary log buffer could not be flushed. Data loss possible!", e);
                        }
                    }
                }
            }
        }

        m_numberOfBytes = 0;
        m_writePos = 0;
    }

    @Override
    public long getOccupiedSpace() {
        return m_numberOfBytes;
    }

    @Override
    public final long appendToLog(final DirectByteBufferWrapper p_bufferWrapper, final int p_bufferOffset, int p_length)
            throws IOException {
        long ret;

        SOP_WRITE_PRIMARY_LOG_SIZE.add(p_length);
        SOP_WRITE_PRIMARY_LOG_TIME.start();

        if (p_bufferWrapper == null) {
            throw new IOException("Error writing to log. Buffer wrapper is null");
        }

        assert m_writePos + p_length <= m_logSize;
        assert p_bufferWrapper.getBuffer().position() == 0;

        // Mark the end of the write access
        p_bufferWrapper.getBuffer().put(p_length, (byte) 0);

        ms_logAccess.write(m_log, p_bufferWrapper, 0, m_writePos, p_length + 1, false);

        ret = m_writePos + p_length;

        SOP_WRITE_PRIMARY_LOG_TIME.stop();

        return ret;
    }

}

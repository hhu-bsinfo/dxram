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

package de.hhu.bsinfo.dxram.log.storage;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Log catalog: Bundles all logs and buffers for one node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 22.05.2015
 */
public final class LogCatalog {

    private static final Logger LOGGER = LogManager.getFormatterLogger(LogCatalog.class.getSimpleName());
    private static final int CHUNK_SIZE = 10;

    // Attributes
    private SecondaryLog[] m_logs;
    private SecondaryLogBuffer[] m_buffers;
    private int m_numberOfLogs = 0;

    // Constructors

    /**
     * Creates an instance of LogCatalog
     */
    public LogCatalog() {
        m_logs = new SecondaryLog[CHUNK_SIZE];
        m_buffers = new SecondaryLogBuffer[CHUNK_SIZE];
    }

    // Getter

    /**
     * Gets all secondary logs from this node
     *
     * @return the secondary log array
     */
    public SecondaryLog[] getAllLogs() {
        return m_logs;
    }

    /**
     * Gets all secondary log buffers from this node
     *
     * @return the secondary log buffer array
     */
    public SecondaryLogBuffer[] getAllBuffers() {
        return m_buffers;
    }

    /**
     * Returns whether there is already a secondary log with given identifier
     *
     * @param p_rangeID
     *         the RangeID
     * @return whether there is already a secondary log with given identifier or not
     */
    public boolean exists(final short p_rangeID) {

        return p_rangeID < m_logs.length && m_logs[p_rangeID & 0xFFFF] != null;
    }

    /**
     * Gets the corresponding secondary log
     *
     * @param p_rangeID
     *         the RangeID
     * @return the secondary log
     */
    public SecondaryLog getLog(final short p_rangeID) {
        SecondaryLog ret;

        ret = m_logs[p_rangeID & 0xFFFF];

        if (ret == null) {
            LOGGER.error("There is no secondary log for RID=%d", p_rangeID);
        }


        return ret;
    }

    /**
     * Gets the corresponding secondary log buffer
     *
     * @param p_rangeID
     *         the RangeID for migrations or -1
     * @return the secondary log buffer
     */
    public SecondaryLogBuffer getBuffer(final short p_rangeID) {
        SecondaryLogBuffer ret;

        ret = m_buffers[p_rangeID & 0xFFFF];


        if (ret == null) {
            LOGGER.error("There is no secondary log buffer for RID=%d", p_rangeID);
        }


        return ret;
    }

    /**
     * Inserts a new range
     *
     * @param p_rangeID
     *         the RangeID
     * @param p_log
     *         the new secondary log to link
     * @param p_secondaryLogBufferSize
     *         the secondary log buffer size
     * @param p_logSegmentSize
     *         the segment size
     */
    public void insertRange(short p_rangeID, final SecondaryLog p_log, final int p_secondaryLogBufferSize, final int p_logSegmentSize) {
        SecondaryLogBuffer buffer;

        if (p_rangeID >= m_logs.length) {
            int newSize = p_rangeID / CHUNK_SIZE * CHUNK_SIZE + CHUNK_SIZE;

            SecondaryLog[] temp1 = new SecondaryLog[newSize];
            System.arraycopy(m_logs, 0, temp1, 0, m_logs.length);
            m_logs = temp1;

            SecondaryLogBuffer[] temp2 = new SecondaryLogBuffer[newSize];
            System.arraycopy(m_buffers, 0, temp2, 0, m_buffers.length);
            m_buffers = temp2;
        }
        m_logs[p_rangeID & 0xFFFF] = p_log;

        // Create new secondary log buffer
        buffer = new SecondaryLogBuffer(p_log, p_secondaryLogBufferSize, p_logSegmentSize);
        m_buffers[p_rangeID & 0xFFFF] = buffer;

        m_numberOfLogs++;
    }

    /**
     * Removes buffer and secondary log for given range
     */
    public void removeBufferAndLog(final short p_rangeID) {
        m_buffers[p_rangeID & 0xFFFF] = null;
        m_logs[p_rangeID & 0xFFFF] = null;

        m_numberOfLogs--;
    }

    /**
     * Removes buffer and secondary log for given range and closes both
     */
    public void removeAndCloseBufferAndLog(final short p_rangeID) throws IOException {
        SecondaryLog secondaryLog;
        SecondaryLogBuffer secondaryLogBuffer;

        secondaryLogBuffer = m_buffers[p_rangeID & 0xFFFF];
        m_buffers[p_rangeID & 0xFFFF] = null;
        secondaryLogBuffer.close();
        secondaryLog = m_logs[p_rangeID & 0xFFFF];
        m_logs[p_rangeID & 0xFFFF] = null;
        secondaryLog.closeAndRemove();

        m_numberOfLogs--;
    }

    /**
     * Closes all logs and buffers from this node
     *
     * @throws IOException
     *         if the log could not be closed
     */
    public void closeLogsAndBuffers() throws IOException {
        for (int i = 0; i < m_logs.length; i++) {
            if (m_buffers[i] != null) {
                m_buffers[i].close();
            }
            if (m_logs[i] != null) {
                m_logs[i].close();
            }
        }
        m_buffers = null;
        m_logs = null;
        m_numberOfLogs = 0;
    }

    // Methods

    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder("Cat:[");

        for (int i = 0; i < m_logs.length; i++) {
            if (m_logs[i] == null) {
                ret.append("null\n     ");
            } else {
                ret.append(m_logs[i]).append("\n     ");
            }
        }

        return ret.toString() + ']';
    }

    /**
     * Gets the number of logs in this catalog
     *
     * @return the number of logs
     */
    int getNumberOfLogs() {
        return m_numberOfLogs;
    }

}

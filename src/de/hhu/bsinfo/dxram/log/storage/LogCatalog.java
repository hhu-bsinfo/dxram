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

package de.hhu.bsinfo.dxram.log.storage;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Log catalog: Bundles all logs and buffers for one node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 22.05.2015
 */
public final class LogCatalog {

    private static final Logger LOGGER = LogManager.getFormatterLogger(LogCatalog.class.getSimpleName());

    // Attributes
    private ArrayList<SecondaryLog> m_logs;
    private ArrayList<SecondaryLogBuffer> m_buffers;

    // FIXME: Replace ArrayList because of oob exceptions

    // Constructors

    /**
     * Creates an instance of SecondaryLogsReorgThread
     */
    public LogCatalog() {
        m_logs = new ArrayList<>();
        m_buffers = new ArrayList<>();
    }

    // Getter

    /**
     * Gets all secondary logs from this node
     *
     * @return the secondary log array
     */
    public SecondaryLog[] getAllLogs() {
        return m_logs.toArray(new SecondaryLog[m_logs.size()]);
    }

    /**
     * Gets all secondary log buffers from this node
     *
     * @return the secondary log buffer array
     */
    public SecondaryLogBuffer[] getAllBuffers() {
        return m_buffers.toArray(new SecondaryLogBuffer[m_buffers.size()]);
    }

    /**
     * Removes buffer and secondary log for given range
     */
    public void removeBufferAndLog(final short p_rangeID) throws IOException {
        SecondaryLog secondaryLog;
        SecondaryLogBuffer secondaryLogBuffer;

        secondaryLogBuffer = m_buffers.set(p_rangeID, null);
        secondaryLogBuffer.close();
        secondaryLog = m_logs.set(p_rangeID, null);
        secondaryLog.closeAndRemove();
    }

    /**
     * Returns whether there is already a secondary log with given identifier
     *
     * @param p_rangeID
     *     the RangeID
     * @return whether there is already a secondary log with given identifier or not
     */
    public boolean exists(final short p_rangeID) {

        return p_rangeID < m_logs.size() && m_logs.get(p_rangeID) != null;
    }

    /**
     * Gets the corresponding secondary log
     *
     * @param p_rangeID
     *     the RangeID
     * @return the secondary log
     */
    public SecondaryLog getLog(final short p_rangeID) {
        SecondaryLog ret;

        ret = m_logs.get(p_rangeID);
        // #if LOGGER >= ERROR
        if (ret == null) {
            LOGGER.error("There is no secondary log for RID=%d", p_rangeID);
        }
        // #endif /* LOGGER >= ERROR */

        return ret;
    }

    /**
     * Gets the corresponding secondary log buffer
     *
     * @param p_rangeID
     *     the RangeID for migrations or -1
     * @return the secondary log buffer
     */
    public SecondaryLogBuffer getBuffer(final short p_rangeID) {
        SecondaryLogBuffer ret;
        int rangeID;

        ret = m_buffers.get(p_rangeID);

        // #if LOGGER >= ERROR
        if (ret == null) {
            LOGGER.error("There is no secondary log buffer for RID=%d", p_rangeID);
        }
        // #endif /* LOGGER >= ERROR */

        return ret;
    }

    /**
     * Inserts a new range
     *
     * @param p_rangeID
     *     the RangeID
     * @param p_log
     *     the new secondary log to link
     * @param p_secondaryLogBufferSize
     *     the secondary log buffer size
     * @param p_logSegmentSize
     *     the segment size
     */
    public void insertRange(short p_rangeID, final SecondaryLog p_log, final int p_secondaryLogBufferSize, final int p_logSegmentSize) {
        SecondaryLogBuffer buffer;

        m_logs.add(p_rangeID, p_log);

        // Create new secondary log buffer
        buffer = new SecondaryLogBuffer(p_log, p_secondaryLogBufferSize, p_logSegmentSize);
        m_buffers.add(p_rangeID, buffer);
    }

    /**
     * Closes all logs and buffers from this node
     *
     * @throws IOException
     *     if the log could not be closed
     */
    public void closeLogsAndBuffers() throws IOException {
        for (int i = 0; i < m_logs.size(); i++) {
            if (m_buffers.get(i) != null) {
                m_buffers.get(i).close();
            }
            if (m_logs.get(i) != null) {
                m_logs.get(i).close();
            }
        }
        m_buffers = null;
        m_logs = null;
    }

    // Methods

    @Override
    public String toString() {
        String ret = "Cat:[";

        for (SecondaryLog log : m_logs) {
            ret += log + "\n     ";
        }

        return ret + ']';
    }

    /**
     * Gets the number of logs in this catalog
     *
     * @return the number of logs
     */
    int getNumberOfLogs() {
        return m_logs.size();
    }

}

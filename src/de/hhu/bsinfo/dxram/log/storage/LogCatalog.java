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

import de.hhu.bsinfo.dxram.data.ChunkID;

/**
 * Log catalog: Bundles all logs and buffers for one node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 22.05.2015
 */
public final class LogCatalog {

    private static final Logger LOGGER = LogManager.getFormatterLogger(LogCatalog.class.getSimpleName());

    // Attributes
    private ArrayList<SecondaryLog> m_creatorLogs;
    private ArrayList<SecondaryLogBuffer> m_creatorBuffers;
    private ArrayList<Long> m_creatorBackupRanges;

    private ArrayList<SecondaryLog> m_migrationLogs;
    private ArrayList<SecondaryLogBuffer> m_migrationBuffers;
    private int m_currentRangeID;

    // Constructors

    /**
     * Creates an instance of SecondaryLogsReorgThread
     */
    public LogCatalog() {
        m_creatorLogs = new ArrayList<>();
        m_creatorBuffers = new ArrayList<>();
        m_creatorBackupRanges = new ArrayList<>();

        m_migrationLogs = new ArrayList<>();
        m_migrationBuffers = new ArrayList<>();
        m_currentRangeID = 0;
    }

    // Getter

    /**
     * Gets the number of logs in this catalog
     *
     * @return the number of logs
     */
    public int getNumberOfLogs() {
        return m_creatorLogs.size() + m_migrationLogs.size();
    }

    /**
     * Gets all secondary logs from this node
     *
     * @return the secondary log array
     */
    public SecondaryLog[] getAllLogs() {
        SecondaryLog[] ret;
        SecondaryLog[] creatorLogs;
        SecondaryLog[] migrationLogs;

        creatorLogs = m_creatorLogs.toArray(new SecondaryLog[m_creatorLogs.size()]);
        migrationLogs = m_migrationLogs.toArray(new SecondaryLog[m_migrationLogs.size()]);

        ret = new SecondaryLog[creatorLogs.length + migrationLogs.length];
        System.arraycopy(creatorLogs, 0, ret, 0, creatorLogs.length);
        System.arraycopy(migrationLogs, 0, ret, creatorLogs.length, migrationLogs.length);

        return ret;
    }

    /**
     * Gets all creator secondary logs from this node
     *
     * @return the creator secondary log array
     */
    public SecondaryLog[] getAllCreatorLogs() {
        return m_creatorLogs.toArray(new SecondaryLog[m_creatorLogs.size()]);
    }

    /**
     * Gets all migration secondary logs from this node
     *
     * @return the migration secondary log array
     */
    public SecondaryLog[] getAllMigrationLogs() {
        return m_migrationLogs.toArray(new SecondaryLog[m_migrationLogs.size()]);
    }

    /**
     * Gets all secondary log buffers from this node
     *
     * @return the secondary log buffer array
     */
    public SecondaryLogBuffer[] getAllBuffers() {
        SecondaryLogBuffer[] ret;
        SecondaryLogBuffer[] creatorBuffers;
        SecondaryLogBuffer[] migrationBuffers;

        creatorBuffers = m_creatorBuffers.toArray(new SecondaryLogBuffer[m_creatorBuffers.size()]);
        migrationBuffers = m_migrationBuffers.toArray(new SecondaryLogBuffer[m_migrationBuffers.size()]);

        ret = new SecondaryLogBuffer[creatorBuffers.length + migrationBuffers.length];
        System.arraycopy(creatorBuffers, 0, ret, 0, creatorBuffers.length);
        System.arraycopy(migrationBuffers, 0, ret, creatorBuffers.length, migrationBuffers.length);

        return ret;
    }

    /**
     * Gets all creator secondary log buffers from this node
     *
     * @return the creator secondary log buffer array
     */
    public SecondaryLogBuffer[] getAllCreatorBuffers() {
        return m_creatorBuffers.toArray(new SecondaryLogBuffer[m_creatorBuffers.size()]);
    }

    /**
     * Gets all migration secondary log buffers from this node
     *
     * @return the migration secondary log buffer array
     */
    public SecondaryLogBuffer[] getAllMigrationBuffers() {
        return m_migrationBuffers.toArray(new SecondaryLogBuffer[m_migrationBuffers.size()]);
    }

    /**
     * Gets the unique identification for the next backup range
     *
     * @param p_isMigration
     *     whether the next backup range is for migrations or not
     * @return the secondary log
     */
    public String getNewID(final boolean p_isMigration) {
        String ret;

        if (!p_isMigration) {
            ret = "C" + m_creatorLogs.size();
        } else {
            ret = "M" + (m_currentRangeID + 1);
        }

        return ret;
    }

    /**
     * Returns whether there is already a secondary log with given identifier
     *
     * @param p_chunkID
     *     the ChunkID
     * @param p_rangeID
     *     the RangeID for migrations or -1
     * @return whether there is already a secondary log with given identifier or not
     */
    public boolean exists(final long p_chunkID, final byte p_rangeID) {
        boolean ret = false;

        if (p_rangeID != -1) {
            if (m_migrationLogs.size() > p_rangeID) {
                ret = true;
            }
        } else {
            if (!m_creatorBackupRanges.isEmpty() && m_creatorBackupRanges.get(m_creatorBackupRanges.size() - 1) >= ChunkID.getLocalID(p_chunkID)) {
                ret = true;
            }
        }

        return ret;
    }

    /**
     * Gets the corresponding secondary log
     *
     * @param p_chunkID
     *     the ChunkID
     * @param p_rangeID
     *     the RangeID for migrations or -1
     * @return the secondary log
     */
    public SecondaryLog getLog(final long p_chunkID, final byte p_rangeID) {
        SecondaryLog ret;
        int rangeID;

        if (p_rangeID != -1) {
            ret = m_migrationLogs.get(p_rangeID);
        } else {
            rangeID = getRangeID(p_chunkID);
            ret = m_creatorLogs.get(rangeID);
        }
        // #if LOGGER >= ERROR
        if (ret == null) {
            LOGGER.error("There is no secondary log for CID=0x%X and RID=%d", p_chunkID, p_rangeID);
        }
        // #endif /* LOGGER >= ERROR */

        return ret;
    }

    /**
     * Gets the corresponding secondary log buffer
     *
     * @param p_chunkID
     *     the ChunkID
     * @param p_rangeID
     *     the RangeID for migrations or -1
     * @return the secondary log buffer
     */
    public SecondaryLogBuffer getBuffer(final long p_chunkID, final byte p_rangeID) {
        SecondaryLogBuffer ret;
        int rangeID;

        if (p_rangeID != -1) {
            ret = m_migrationBuffers.get(p_rangeID);
        } else {
            rangeID = getRangeID(p_chunkID);
            ret = m_creatorBuffers.get(rangeID);
        }
        // #if LOGGER >= ERROR
        if (ret == null) {
            LOGGER.error("There is no secondary log buffer for CID=0x%X and RID=%d", p_chunkID, p_rangeID);
        }
        // #endif /* LOGGER >= ERROR */

        return ret;
    }

    /**
     * Gets the corresponding range
     *
     * @param p_chunkID
     *     the ChunkID
     * @return the first ChunkID of the range
     */
    public long getRange(final long p_chunkID) {
        long ret = -1;

        for (int i = m_creatorLogs.size() - 1; i >= 0; i--) {
            if (m_creatorBackupRanges.get(i) <= ChunkID.getLocalID(p_chunkID)) {
                ret = m_creatorBackupRanges.get(i);
                break;
            }
        }

        return ret;
    }

    // Setter

    /**
     * Inserts a new range
     *
     * @param p_firstChunkIDOrRangeID
     *     the first ChunkID of the range
     * @param p_log
     *     the new secondary log to link
     * @param p_secondaryLogBufferSize
     *     the secondary log buffer size
     * @param p_logSegmentSize
     *     the segment size
     */
    public void insertRange(final long p_firstChunkIDOrRangeID, final SecondaryLog p_log, final int p_secondaryLogBufferSize, final int p_logSegmentSize) {
        SecondaryLogBuffer buffer;
        int rangeID;

        if (ChunkID.getCreatorID(p_firstChunkIDOrRangeID) != -1) {
            rangeID = m_creatorBackupRanges.size();
            m_creatorLogs.add(rangeID, p_log);

            // Create new secondary log buffer
            buffer = new SecondaryLogBuffer(p_log, p_secondaryLogBufferSize, p_logSegmentSize);
            m_creatorBuffers.add(rangeID, buffer);

            // Insert range
            m_creatorBackupRanges.add(ChunkID.getLocalID(p_firstChunkIDOrRangeID));
        } else {
            m_migrationLogs.add(m_currentRangeID, p_log);

            // Create new secondary log buffer
            buffer = new SecondaryLogBuffer(p_log, p_secondaryLogBufferSize, p_logSegmentSize);
            m_migrationBuffers.add(m_currentRangeID, buffer);

            m_currentRangeID++;
        }
    }

    // Methods

    /**
     * Closes all logs and buffers from this node
     *
     * @throws IOException
     *     if the log could not be closed
     */
    public void closeLogsAndBuffers() throws IOException {
        for (int i = 0; i < m_creatorLogs.size(); i++) {
            m_creatorBuffers.get(i).close();
            m_creatorLogs.get(i).closeLog();
        }
        m_creatorBuffers = null;
        m_creatorLogs = null;
        m_creatorBackupRanges = null;

        for (int i = 0; i < m_migrationLogs.size(); i++) {
            m_migrationBuffers.get(i).close();
            m_migrationLogs.get(i).closeLog();
        }
        m_migrationBuffers = null;
        m_migrationLogs = null;
    }

    @Override
    public String toString() {
        return "Creator logs: " + m_creatorLogs + "\n Migration logs: " + m_migrationLogs;
    }

    /**
     * Determines the corresponding range
     *
     * @param p_chunkID
     *     the ChunkID
     * @return the RangeID
     */
    private int getRangeID(final long p_chunkID) {
        int ret = 0;
        final long localID = ChunkID.getLocalID(p_chunkID);

        for (int i = m_creatorLogs.size() - 1; i >= 0; i--) {
            if (m_creatorBackupRanges.get(i) <= localID) {
                ret = i;
                break;
            }
        }

        return ret;
    }
}

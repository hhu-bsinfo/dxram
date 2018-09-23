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

import de.hhu.bsinfo.dxram.log.storage.logs.secondarylog.SecondaryLogBuffer;
import de.hhu.bsinfo.dxram.log.storage.versioncontrol.VersionBuffer;
import de.hhu.bsinfo.dxutils.RandomUtils;

/**
 * Bundles buffers (including logs) for one backup peer.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.09.2018
 */
public final class BackupRangeCatalog {

    private static final Logger LOGGER = LogManager.getFormatterLogger(BackupRangeCatalog.class.getSimpleName());
    private static final int CHUNK_SIZE = 10;

    private final SecondaryLogBuffer[][] m_secondaryLogBuffers;
    private final VersionBuffer[][] m_versionBuffers;

    private int m_numberOfNodes = 0;
    private int m_numberOfRanges = 0;

    /**
     * Creates an instance of BackupRangeCatalog.
     */
    public BackupRangeCatalog() {
        m_secondaryLogBuffers = new SecondaryLogBuffer[Short.MAX_VALUE * 2 + 1][];
        m_versionBuffers = new VersionBuffer[Short.MAX_VALUE * 2 + 1][];
    }

    /**
     * Gets all secondary log buffers from given peer.
     *
     * @param p_owner
     *         the peer
     * @return the secondary log buffer array
     */
    public SecondaryLogBuffer[] getAllSecondaryLogBuffers(final short p_owner) {
        return m_secondaryLogBuffers[p_owner & 0xFFFF];
    }

    /**
     * Gets all version buffers from given peer.
     *
     * @param p_owner
     *         the peer
     * @return the version buffer array
     */
    public VersionBuffer[] getAllVersionBuffers(final short p_owner) {
        return m_versionBuffers[p_owner & 0xFFFF];
    }

    /**
     * Returns whether there is already a secondary log with given identifier.
     *
     * @param p_owner
     *         the peer
     * @param p_range
     *         the RangeID
     * @return whether there is already a secondary log with given identifier or not
     */
    public boolean exists(final short p_owner, final short p_range) {
        SecondaryLogBuffer[] buffers = m_secondaryLogBuffers[p_owner & 0xFFFF];

        if (buffers == null) {
            return false;
        }

        return p_range < buffers.length && buffers[p_range & 0xFFFF] != null;
    }

    /**
     * Gets the corresponding secondary log buffer.
     *
     * @param p_owner
     *         the peer
     * @param p_range
     *         the RangeID for migrations or -1
     * @return the secondary log buffer
     */
    public SecondaryLogBuffer getSecondaryLogBuffer(final short p_owner, final short p_range) {
        SecondaryLogBuffer[] buffers = m_secondaryLogBuffers[p_owner & 0xFFFF];

        if (buffers == null) {
            LOGGER.error("There is no secondary log buffer for NID=%d and RID=%d", p_owner, p_range);
            return null;
        }

        SecondaryLogBuffer ret = buffers[p_range & 0xFFFF];

        if (ret == null) {
            LOGGER.error("There is no secondary log buffer for NID=%d and RID=%d", p_owner, p_range);
        }

        return ret;
    }

    /**
     * Gets the corresponding version buffer.
     *
     * @param p_owner
     *         the peer
     * @param p_range
     *         the RangeID for migrations or -1
     * @return the version buffer
     */
    public VersionBuffer getVersionBuffer(final short p_owner, final short p_range) {
        VersionBuffer[] versionBuffers = m_versionBuffers[p_owner & 0xFFFF];

        if (versionBuffers == null) {
            LOGGER.error("There is no version buffer for NID=%d and RID=%d", p_owner, p_range);
            return null;
        }

        VersionBuffer ret = versionBuffers[p_range & 0xFFFF];

        if (ret == null) {
            LOGGER.error("There is no version buffer for NID=%d and RID=%d", p_owner, p_range);
        }

        return ret;
    }

    /**
     * Inserts a new range.
     *
     * @param p_owner
     *         the peer
     * @param p_range
     *         the RangeID
     * @param p_buffer
     *         the new secondary log buffer to link
     * @param p_versionBuffer
     *         the new version buffer to link
     */
    public void insertRange(final short p_owner, short p_range, final SecondaryLogBuffer p_buffer,
            final VersionBuffer p_versionBuffer) {
        if (m_secondaryLogBuffers[p_owner & 0xFFFF] == null) {
            m_secondaryLogBuffers[p_owner & 0xFFFF] = new SecondaryLogBuffer[CHUNK_SIZE];
            m_versionBuffers[p_owner & 0xFFFF] = new VersionBuffer[CHUNK_SIZE];

            m_numberOfNodes++;
        }

        if (p_range >= m_secondaryLogBuffers[p_owner & 0xFFFF].length) {
            int newSize = p_range / CHUNK_SIZE * CHUNK_SIZE + CHUNK_SIZE;

            SecondaryLogBuffer[] temp2 = new SecondaryLogBuffer[newSize];
            System.arraycopy(m_secondaryLogBuffers[p_owner & 0xFFFF], 0, temp2, 0,
                    m_secondaryLogBuffers[p_owner & 0xFFFF].length);
            m_secondaryLogBuffers[p_owner & 0xFFFF] = temp2;

            VersionBuffer[] temp3 = new VersionBuffer[newSize];
            System.arraycopy(m_versionBuffers[p_owner & 0xFFFF], 0, temp3, 0,
                    m_versionBuffers[p_owner & 0xFFFF].length);
            m_versionBuffers[p_owner & 0xFFFF] = temp3;
        }

        m_secondaryLogBuffers[p_owner & 0xFFFF][p_range & 0xFFFF] = p_buffer;
        m_versionBuffers[p_owner & 0xFFFF][p_range & 0xFFFF] = p_versionBuffer;

        m_numberOfRanges++;
    }

    /**
     * Moves a range to new owner and range after recovery.
     *
     * @param p_originalOwner
     *         the original owner (before recovery)
     * @param p_originalRange
     *         the original range (before recovery)
     * @param p_newOwner
     *         the new owner (after recovery)
     * @param p_newRange
     *         the new RangeID (after recovery)
     * @param p_buffer
     *         the secondary log buffer to link
     * @param p_versionBuffer
     *         the version buffer to link
     */
    public void moveRange(final short p_originalOwner, final short p_originalRange, final short p_newOwner,
            short p_newRange, final SecondaryLogBuffer p_buffer, final VersionBuffer p_versionBuffer) {

        try {
            remove(p_originalOwner, p_originalRange, false);
        } catch (final IOException ignore) {
            // Cannot be here if p_close is false
        }

        insertRange(p_newOwner, p_newRange, p_buffer, p_versionBuffer);
    }

    /**
     * Removes buffers and logs for given range and closes them (this also deletes the log files).
     *
     * @param p_owner
     *         the peer
     */
    public void removeAndCloseBuffersAndLogs(final short p_owner, final short p_range) throws IOException {
        remove(p_owner, p_range, true);
    }

    /**
     * Removes backup range.
     *
     * @param p_creatorID
     *         the creator ID
     * @param p_rangeID
     *         the range ID
     * @param p_close
     *         whether the logs and buffers must be closed or not
     * @throws IOException
     *         if a log could not be deleted
     */
    private void remove(final short p_creatorID, final short p_rangeID, final boolean p_close) throws IOException {
        SecondaryLogBuffer[] buffers = m_secondaryLogBuffers[p_creatorID & 0xFFFF];
        VersionBuffer[] versionBuffers = m_versionBuffers[p_creatorID & 0xFFFF];

        if (buffers == null || versionBuffers == null) {
            LOGGER.error("There is no backup range for NID=%d and RID=%d", p_creatorID, p_rangeID);
            return;
        }

        if (p_close) {
            SecondaryLogBuffer buffer = buffers[p_rangeID & 0xFFFF];
            if (buffer != null) {
                buffer.closeAndRemoveLog();
            }

            VersionBuffer versionBuffer = versionBuffers[p_rangeID & 0xFFFF];
            if (versionBuffer != null) {
                versionBuffer.closeAndRemoveLog();
            }
        }
        buffers[p_rangeID & 0xFFFF] = null;
        versionBuffers[p_rangeID & 0xFFFF] = null;

        boolean isNodeEmpty = true;
        for (int i = 0; i < buffers.length; i++) {
            if (buffers[i] != null) {
                isNodeEmpty = false;
                break;
            }
        }
        if (isNodeEmpty) {
            m_secondaryLogBuffers[p_creatorID & 0xFFFF] = null;
            m_versionBuffers[p_creatorID & 0xFFFF] = null;

            m_numberOfNodes--;
        }

        m_numberOfRanges--;
    }

    /**
     * Closes all logs and buffers from this node
     */
    public void closeLogsAndBuffers() {
        for (int i = 0; i < m_secondaryLogBuffers.length; i++) {
            SecondaryLogBuffer[] secondaryLogBuffers = m_secondaryLogBuffers[i];
            if (secondaryLogBuffers != null) {
                VersionBuffer[] versionBuffers = m_versionBuffers[i];

                for (int j = 0; j < secondaryLogBuffers.length; j++) {
                    if (secondaryLogBuffers[j] != null) {
                        try {
                            secondaryLogBuffers[j].close();
                        } catch (final IOException e) {
                            LOGGER.error("Could not close secondary log for NID=%d and RID=%d", (short) i, (short) j);
                        }
                    }
                    if (versionBuffers[j] != null) {
                        try {
                            versionBuffers[j].close();
                        } catch (final IOException e) {
                            LOGGER.error("Could not close version log for NID=%d and RID=%d", (short) i, (short) j);
                        }
                    }
                }
            }
        }
        m_numberOfRanges = 0;
    }

    /**
     * Returns all secondary log buffers for a random peer.
     * Used for reorganization, only.
     *
     * @return an array of secondary log buffers or null
     */
    public SecondaryLogBuffer[] getSecondaryLogBuffersOfRandomNode() {
        int index = 0;
        int counter = 0;
        int randomNumber = RandomUtils.getRandomValue(m_numberOfNodes);

        while (counter < randomNumber || m_secondaryLogBuffers[index] == null) {
            if (m_secondaryLogBuffers[index] != null) {
                counter++;
            }

            if (++index == Short.MAX_VALUE * 2 + 1) {
                return null;
            }
        }

        return m_secondaryLogBuffers[index];
    }

    /**
     * Gets the number of logs in this catalog.
     *
     * @return the number of logs
     */
    public int getNumberOfLogs() {
        return m_numberOfRanges;
    }

    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder("BackupRanges:\n");

        for (int i = 0; i < m_secondaryLogBuffers.length; i++) {
            SecondaryLogBuffer[] buffers = m_secondaryLogBuffers[i];
            if (buffers != null) {
                ret.append("\t Node ").append((short) i).append(" [");
                for (int j = 0; j < buffers.length; j++) {
                    ret.append(" Range ").append((short) j).append(" (");
                    if (buffers[j] == null) {
                        ret.append("null) ");
                    } else {
                        ret.append(buffers[j].getLog()).append(") ");
                    }
                }
                ret.append("]\n");
            }
        }

        return ret.toString();
    }

}

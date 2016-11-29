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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.util.HarddriveAccessMode;

/**
 * This class implements the primary log. Furthermore this class manages all
 * secondary logs
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 13.06.2014
 */
public final class PrimaryLog extends AbstractLog {

    // Constants
    private static final String PRIMLOG_SUFFIX_FILENAME = "prim.log";
    private static final byte[] PRIMLOG_HEADER = "DXRAMPrimLogv1".getBytes(Charset.forName("UTF-8"));

    // Attributes
    private LogComponent m_logComponent;
    private long m_primaryLogSize;

    private long m_writePos;
    private long m_numberOfBytes;

    // Constructors

    /**
     * Creates an instance of PrimaryLog with user specific configuration
     *
     * @param p_logComponent
     *     the log component
     * @param p_backupDirectory
     *     the backup directory
     * @param p_nodeID
     *     the NodeID
     * @param p_primaryLogSize
     *     the size of a primary log
     * @param p_flashPageSize
     *     the size of flash page
     * @param p_mode
     *     the HarddriveAccessMode
     * @throws IOException
     *     if primary log could not be created
     */
    public PrimaryLog(final LogComponent p_logComponent, final String p_backupDirectory, final short p_nodeID, final long p_primaryLogSize,
        final int p_flashPageSize, final HarddriveAccessMode p_mode) throws IOException {
        super(new File(p_backupDirectory + 'N' + p_nodeID + '_' + PRIMLOG_SUFFIX_FILENAME), p_primaryLogSize, PRIMLOG_HEADER.length, p_mode);
        m_primaryLogSize = p_primaryLogSize;

        m_writePos = 0;
        m_numberOfBytes = 0;

        m_logComponent = p_logComponent;

        if (m_primaryLogSize < p_flashPageSize) {
            throw new IllegalArgumentException("Error: Primary log too small");
        }

        if (!createLogAndWriteHeader(PRIMLOG_HEADER)) {
            throw new IOException("Error: Primary log could not be created");
        }
    }

    // Methods
    @Override
    public long getOccupiedSpace() {
        return m_numberOfBytes;
    }

    @Override
    public int appendData(final byte[] p_data, final int p_offset, final int p_length) throws IOException, InterruptedException {
        if (m_primaryLogSize - m_numberOfBytes < p_length) {
            // Not enough free space in primary log -> flush to secondary logs and reset primary log
            m_logComponent.flushDataToSecondaryLogs();
            m_numberOfBytes = 0;
        }

        m_writePos = appendToPrimaryLog(p_data, p_offset, p_length, m_writePos);
        m_numberOfBytes += p_length;

        return p_length;
    }

}

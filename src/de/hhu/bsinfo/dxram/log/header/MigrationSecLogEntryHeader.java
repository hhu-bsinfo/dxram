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

package de.hhu.bsinfo.dxram.log.header;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.log.storage.Version;

/**
 * Extends AbstractLogEntryHeader for a migration log entry header (secondary log)
 * Fields: | Type | NodeID | LocalID | Length  | Epoch | Version | Chaining | Checksum |
 * Length: |  1   |   2    | 1,2,4,6 | 0,1,2,3 |   2   | 0,1,2,4 |   0,1    |    0,4   |
 * Type field contains type, length of LocalID field, length of length field and length of version field
 * Chaining field has length 0 for chunks smaller than 1/2 of segment size (4 MB default) and 1 for larger chunks
 * Checksum field has length 0 if checksums are deactivated in DXRAM configuration, 4 otherwise
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.06.2015
 */
class MigrationSecLogEntryHeader extends AbstractSecLogEntryHeader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(MasterSlaveComputeService.class.getSimpleName());

    // Attributes
    private static short ms_maximumSize;
    private static byte ms_nidOffset;
    private static byte ms_lidOffset;

    // Constructors

    /**
     * Creates an instance of MigrationSecLogEntryHeader
     */
    MigrationSecLogEntryHeader() {
        ms_maximumSize = (short) (LOG_ENTRY_TYP_SIZE + MAX_LOG_ENTRY_CID_SIZE + MAX_LOG_ENTRY_LEN_SIZE + LOG_ENTRY_EPO_SIZE + MAX_LOG_ENTRY_VER_SIZE +
            ChecksumHandler.getCRCSize());
        ms_nidOffset = LOG_ENTRY_TYP_SIZE;
        ms_lidOffset = (byte) (ms_nidOffset + LOG_ENTRY_NID_SIZE);
    }

    // Getter
    @Override
    protected short getNIDOffset() {
        return ms_nidOffset;
    }

    @Override
    protected short getLIDOffset() {
        return ms_lidOffset;
    }

    @Override
    public short getNodeID(final byte[] p_buffer, final int p_offset) {
        final int offset = p_offset + ms_nidOffset;

        return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
    }

    @Override
    public long getCID(final byte[] p_buffer, final int p_offset) {
        return ((long) getNodeID(p_buffer, p_offset) << 48) + getLID(p_buffer, p_offset);
    }

    @Override
    public boolean isMigrated() {
        return true;
    }

    // Methods
    @Override
    public void print(final byte[] p_buffer, final int p_offset) {
        final Version version = getVersion(p_buffer, p_offset);

        System.out.println("********************Secondary Log Entry Header (Migration)********************");
        System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
        System.out.println("* Length: " + getLength(p_buffer, p_offset));
        System.out.println("* Version: " + version.getEpoch() + ", " + version.getVersion());
        if (ChecksumHandler.checksumsEnabled()) {
            System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
        }
        System.out.println("******************************************************************************");
    }
}

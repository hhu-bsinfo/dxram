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

package de.hhu.bsinfo.dxram.log.header;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.log.storage.Version;

/**
 * Extends AbstractLogEntryHeader for a migration log entry header (primary log)
 * Fields: | Type | RangeID | Owner | NodeID | LocalID | Length  | Epoch | Version | Chaining | Checksum |
 * Length: |  1   |    1    |   2   |   2    | 1,2,4,6 | 0,1,2,3 |   2   | 0,1,2,4 |   0,2    |    0,4   |
 * Type field contains type, length of LocalID field, length of length field and length of version field
 * Chaining field has length 0 for chunks smaller than 1/2 of segment size (4 MB max.) and 2 for larger chunks (chaining ID + chain size)
 * Checksum field has length 0 if checksums are deactivated in DXRAM configuration, 4 otherwise
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.06.2015
 */
public class PrimLogEntryHeader extends AbstractPrimLogEntryHeader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(PrimLogEntryHeader.class.getSimpleName());

    // Attributes
    private static byte ms_ridOffset;
    private static byte ms_ownOffset;
    private static byte ms_nidOffset;
    private static byte ms_lidOffset;

    // Constructors

    /**
     * Creates an instance of MigrationPrimLogEntryHeader
     */
    public PrimLogEntryHeader() {
        ms_ridOffset = LOG_ENTRY_TYP_SIZE;
        ms_ownOffset = (byte) (ms_ridOffset + LOG_ENTRY_RID_SIZE);
        ms_nidOffset = (byte) (ms_ownOffset + LOG_ENTRY_OWN_SIZE);
        ms_lidOffset = (byte) (ms_nidOffset + LOG_ENTRY_NID_SIZE);
    }

    // Getter
    @Override
    public byte getRangeID(final byte[] p_buffer, final int p_offset) {
        return p_buffer[p_offset + ms_ridOffset];
    }

    @Override
    public short getOwner(final byte[] p_buffer, final int p_offset) {
        final int offset = p_offset + ms_ownOffset;

        return (short) ((p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8));
    }

    // Methods
    @Override
    public byte[] createLogEntryHeader(final long p_chunkID, final int p_size, final Version p_version, final short p_rangeID, final short p_owner) {
        byte[] result;
        byte headerSize;
        byte lengthSize;
        byte localIDSize;
        byte versionSize;
        byte checksumSize = 0;
        byte type;

        localIDSize = getSizeForLocalIDField(ChunkID.getLocalID(p_chunkID));
        lengthSize = getSizeForLengthField(p_size);
        versionSize = getSizeForVersionField(p_version.getVersion());

        if (ChecksumHandler.checksumsEnabled()) {
            checksumSize = ChecksumHandler.getCRCSize();
        }
        headerSize = (byte) (ms_lidOffset + localIDSize + lengthSize + LOG_ENTRY_EPO_SIZE + versionSize + checksumSize);

        if (ChunkID.getCreatorID(p_chunkID) == p_owner) {
            type = 0;
        } else {
            type = 1;
        }

        if (p_size + headerSize <= getMaxLogEntrySize()) {
            type = generateTypeField(type, localIDSize, lengthSize, versionSize, false);
            result = new byte[headerSize];
        } else {
            // This log entry is too large to store it at once -> adjust type field and add chaining field
            type = generateTypeField(type, localIDSize, lengthSize, versionSize, true);
            result = new byte[headerSize + LOG_ENTRY_CHA_SIZE];
        }

        putType(result, type, (byte) 0);
        putRangeID(result, p_rangeID, ms_ridOffset);
        putOwner(result, p_owner, ms_ownOffset);

        putChunkID(result, p_chunkID, localIDSize, ms_nidOffset);

        if (lengthSize == 1) {
            putLength(result, (byte) p_size, getLENOffset(result, 0));
        } else if (lengthSize == 2) {
            putLength(result, (short) p_size, getLENOffset(result, 0));
        } else {
            putLength(result, p_size, getLENOffset(result, 0));
        }

        putEpoch(result, p_version.getEpoch(), getVEROffset(result, 0));
        if (versionSize == 1) {
            putVersion(result, (byte) p_version.getVersion(), (short) (getVEROffset(result, 0) + LOG_ENTRY_EPO_SIZE));
        } else if (versionSize == 2) {
            putVersion(result, (short) p_version.getVersion(), (short) (getVEROffset(result, 0) + LOG_ENTRY_EPO_SIZE));
        } else if (versionSize > 2) {
            putVersion(result, p_version.getVersion(), (short) (getVEROffset(result, 0) + LOG_ENTRY_EPO_SIZE));
        }

        return result;
    }

    @Override
    public void print(final byte[] p_buffer, final int p_offset) {
        final Version version = getVersion(p_buffer, p_offset);

        System.out.println("********************Primary Log Entry Header (Default)*********************");
        System.out.println("* NodeID: " + getNodeID(p_buffer, p_offset));
        System.out.println("* LocalID: " + getLID(p_buffer, p_offset));
        System.out.println("* Length: " + getLength(p_buffer, p_offset));
        System.out.println("* Version: " + version.getEpoch() + ", " + version.getVersion());
        if (ChecksumHandler.checksumsEnabled()) {
            System.out.println("* Checksum: " + getChecksum(p_buffer, p_offset));
        }
        System.out.println("****************************************************************************");
    }

    @Override
    protected short getNIDOffset() {
        return ms_nidOffset;
    }

    @Override
    protected short getLIDOffset() {
        return ms_lidOffset;
    }

}

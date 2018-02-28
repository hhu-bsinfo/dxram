/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.log.storage.Version;

/**
 * Extends AbstractPrimLogEntryHeader for a primary log log entry header
 * Fields: | Type | RangeID | Owner | NodeID | LocalID | Length  | Timestamp | Epoch | Version | Chaining | Checksum |
 * Length: |  1   |    1    |   2   |   2    | 1,2,4,6 | 0,1,2,3 |    0,4    |   2   | 0,1,2,4 |   0,2    |    0,4   |
 * Type field contains type, length of LocalID field, length of length field and length of version field
 * Timestamp field has length 0 if timestamps are deactivated in DXRAM configuration, 4 otherwise
 * Chaining field has length 0 for chunks smaller than 1/2 of segment size (4 MB max.) and 2 for larger chunks (chaining ID + chain size)
 * Checksum field has length 0 if checksums are deactivated in DXRAM configuration, 4 otherwise
 * Log entry headers are read and written with absolute methods (position is untouched), only!
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.06.2015
 */
public class PrimLogEntryHeader extends AbstractPrimLogEntryHeader {

    // Attributes
    private static byte ms_ridOffset;
    private static byte ms_ownOffset;
    private static byte ms_nidOffset;
    private static byte ms_lidOffset;

    private static ByteBuffer ms_result;

    // Constructors

    /**
     * Creates an instance of MigrationPrimLogEntryHeader
     */
    PrimLogEntryHeader() {
        ms_ridOffset = LOG_ENTRY_TYP_SIZE;
        ms_ownOffset = (byte) (ms_ridOffset + LOG_ENTRY_RID_SIZE);
        ms_nidOffset = (byte) (ms_ownOffset + LOG_ENTRY_OWN_SIZE);
        ms_lidOffset = (byte) (ms_nidOffset + LOG_ENTRY_NID_SIZE);

        ms_result = ByteBuffer.allocateDirect(AbstractLogEntryHeader.getMaxLogEntrySize());
        ms_result.order(ByteOrder.LITTLE_ENDIAN);
    }

    // Getter
    @Override
    public short getRangeID(final ByteBuffer p_buffer, final int p_offset) {
        final int offset = p_offset + ms_ridOffset;

        return p_buffer.getShort(offset);
    }

    @Override
    public short getOwner(final ByteBuffer p_buffer, final int p_offset) {
        final int offset = p_offset + ms_ownOffset;

        return p_buffer.getShort(offset);
    }

    // Methods
    @Override
    public ByteBuffer createLogEntryHeader(final long p_chunkID, final int p_size, final Version p_version, final short p_rangeID, final short p_owner,
            final short p_originalOwner, final int p_timestamp) {
        byte lengthSize;
        byte localIDSize;
        byte versionSize;
        byte checksumSize = 0;
        byte type;
        byte headerSize;

        localIDSize = getSizeForLocalIDField(ChunkID.getLocalID(p_chunkID));
        lengthSize = getSizeForLengthField(p_size);
        versionSize = getSizeForVersionField(p_version.getVersion());

        if (ChecksumHandler.checksumsEnabled()) {
            checksumSize = ChecksumHandler.getCRCSize();
        }
        headerSize = (byte) (ms_lidOffset + localIDSize + lengthSize + ms_timestampSize + LOG_ENTRY_EPO_SIZE + versionSize + checksumSize);

        if (ChunkID.getCreatorID(p_chunkID) == p_originalOwner) {
            type = 0;
        } else {
            type = 1;
        }

        if (p_size + headerSize <= getMaxLogEntrySize()) {
            type = generateTypeField(type, localIDSize, lengthSize, versionSize, false);
        } else {
            // This log entry is too large to store it at once -> adjust type field and add chaining field
            type = generateTypeField(type, localIDSize, lengthSize, versionSize, true);
        }

        // It is faster to fill a pooled byte buffer in Java heap and copy it to native memory than filling the native primary write buffer directly
        ms_result.clear();
        ms_result.limit(headerSize);

        putType(ms_result, type);
        putRangeID(ms_result, p_rangeID, ms_ridOffset);
        putOwner(ms_result, p_owner, ms_ownOffset);

        putChunkID(ms_result, p_chunkID, localIDSize, ms_nidOffset);

        if (lengthSize == 1) {
            putLength(ms_result, (byte) p_size, getLENOffset(type));
        } else if (lengthSize == 2) {
            putLength(ms_result, (short) p_size, getLENOffset(type));
        } else {
            putLength(ms_result, p_size, getLENOffset(type));
        }

        if (ms_timestampSize != 0) {
            putTimestamp(ms_result, p_timestamp, getTSPOffset(type));
        }

        putEpoch(ms_result, p_version.getEpoch(), getVEROffset(type));
        if (versionSize == 1) {
            putVersion(ms_result, (byte) p_version.getVersion(), getVEROffset(type) + LOG_ENTRY_EPO_SIZE);
        } else if (versionSize == 2) {
            putVersion(ms_result, (short) p_version.getVersion(), getVEROffset(type) + LOG_ENTRY_EPO_SIZE);
        } else if (versionSize > 2) {
            putVersion(ms_result, p_version.getVersion(), getVEROffset(type) + LOG_ENTRY_EPO_SIZE);
        }

        return ms_result;
    }

    @Override
    public void print(final ByteBuffer p_buffer, final int p_offset) {
        final Version version = getVersion(p_buffer, p_offset);

        System.out.println("********************Primary Log Entry Header (Default)*********************");
        System.out.println("* Owner: " + getOwner(p_buffer, p_offset));
        System.out.println("* RangeID: " + getRangeID(p_buffer, p_offset));
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

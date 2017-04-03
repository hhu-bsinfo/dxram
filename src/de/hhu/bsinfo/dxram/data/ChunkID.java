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

package de.hhu.bsinfo.dxram.data;

/**
 * Helper class for ChunkID related issues.
 *
 * @author Florian Klein, florian.klein@hhu.de, 23.07.2013
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public final class ChunkID {

    public static final long INVALID_ID = -1;
    private static final long CREATORID_BITMASK = 0xFFFF000000000000L;
    private static final long LOCALID_BITMASK = 0x0000FFFFFFFFFFFFL;

    public static final long MAX_LOCALID = Long.MAX_VALUE & LOCALID_BITMASK;

    /**
     * Static class.
     */
    private ChunkID() {

    }

    /**
     * Get the CreatorID/NodeID part of the ChunkID.
     *
     * @param p_chunkID
     *     ChunkID.
     * @return The NodeID/CreatorID part.
     */
    public static short getCreatorID(final long p_chunkID) {
        assert p_chunkID != INVALID_ID;

        return (short) ((p_chunkID & CREATORID_BITMASK) >> 48);
    }

    /**
     * Get the LocalID part of the ChunkID
     *
     * @param p_chunkID
     *     the ChunkID
     * @return the LocalID part
     */
    public static long getLocalID(final long p_chunkID) {
        assert p_chunkID != INVALID_ID;

        return p_chunkID & LOCALID_BITMASK;
    }

    /**
     * Create a full chunkID from a local and node ID.
     *
     * @param p_nid
     *     Node ID part.
     * @param p_lid
     *     Local ID part.
     * @return Full Chunk ID.
     */
    public static long getChunkID(final short p_nid, final long p_lid) {
        return (long) p_nid << 48 | p_lid & LOCALID_BITMASK;
    }

    /**
     * Convert a chunk id to a hex string
     *
     * @param p_chunkId
     *     Chunk id to convert to a hex string.
     * @return Converted chunk id, example: 0x1111000000000001
     */
    public static String toHexString(final long p_chunkId) {
        return "0x" + Long.toHexString(p_chunkId).toUpperCase();
    }

    /**
     * Method to convert a list of chunk IDs to a list of hex strings
     *
     * @param p_chunkIDs
     *     Chunk IDs
     * @return String with list of chunk IDs in hex
     */
    public static String chunkIDArrayToString(final long[] p_chunkIDs) {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < p_chunkIDs.length; i++) {
            builder.append(toHexString(p_chunkIDs[i]));

            if (i + 1 < p_chunkIDs.length) {
                builder.append(", ");
            }
        }

        return builder.toString();
    }

    /**
     * Parse a hex string containing a chunk id.
     *
     * @param p_str
     *     Hex string with chunk id, e.g. either 0x1234567890ABCDEF or 1234567890ABCDEF
     * @return Parsed chunk id
     */
    public static long parse(final String p_str) {
        String str = p_str;

        if (str.startsWith("0x")) {
            str = str.substring(2);
        }

        return Long.parseUnsignedLong(str, 16);
    }
}

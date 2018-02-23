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

package de.hhu.bsinfo.dxram.lookup.overlay.storage;

/**
 * Barrier id.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.05.2016
 */
public final class BarrierID {
    public static final int INVALID_ID = -1;

    public static final int MAX_ID = 65535;

    /**
     * Hidden constructor
     */
    private BarrierID() {
    }

    /**
     * Get the owner node id of the barrier
     *
     * @param p_barrierId
     *     Barrier id
     * @return Owner node id of the barrier
     */
    public static short getOwnerID(final int p_barrierId) {
        assert p_barrierId != INVALID_ID;
        return (short) (p_barrierId >> 16);
    }

    /**
     * Convert the barrier id to a hex string
     *
     * @param p_barrierId
     *     Barrier id
     * @return Hex string representation
     */
    public static String toHexString(final int p_barrierId) {
        return "0x" + String.format("%08x", p_barrierId).toUpperCase();
    }

    /**
     * Parse a hex string containing a barrier id.
     *
     * @param p_str
     *     Hex string with barrier id, e.g. either 0x12345678 or 12345678
     * @return Parsed barrier id
     */
    public static int parse(final String p_str) {
        String str = p_str;

        if (str.startsWith("0x")) {
            str = str.substring(2);
        }

        return Integer.parseUnsignedInt(str, 16);
    }

    /**
     * Get the (local) barrier id of the barrier id.
     *
     * @param p_barrierId
     *     Full barrier id to get the local part of.
     * @return Local id of the barrier id.
     */
    static int getBarrierID(final int p_barrierId) {
        assert p_barrierId != INVALID_ID;
        return (short) (p_barrierId & 0xFFFF);
    }

    /**
     * Create a barrier id from a separate nid and id
     *
     * @param p_nodeId
     *     Node id (owning the barrier)
     * @param p_id
     *     Id of the barrier
     * @return Barrier id
     */
    static int createBarrierId(final short p_nodeId, final int p_id) {
        return (p_nodeId & 0xFFFF) << 16 | p_id & 0xFFFF;
    }
}

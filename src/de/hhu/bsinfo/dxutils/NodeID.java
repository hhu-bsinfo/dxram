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

package de.hhu.bsinfo.dxutils;

/**
 * Helper class for NodeID related issues.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public final class NodeID {
    public static final short INVALID_ID = -1;

    public static final int MAX_ID = 65535;

    /**
     * Utils class
     */
    private NodeID() {
    }

    /**
     * Convert a node id to a proper string representation in hex.
     *
     * @param p_nodeId
     *     Node id to convert.
     * @return Hex string of node id, example: 0x1111
     */
    public static String toHexString(final short p_nodeId) {
        int tmp = p_nodeId & 0xFFFF;
        // return "0x" + Integer.toHexString(tmp).toUpperCase();
        return "0x" + String.format("%04x", tmp).toUpperCase();
    }

    /**
     * Parse a hex string containing a node id.
     *
     * @param p_str
     *     Hex string with node id, e.g. either 0x1234 or 1234
     * @return Parsed node id
     */
    public static short parse(final String p_str) {
        String str = p_str;

        if (str.startsWith("0x")) {
            str = str.substring(2);
        }

        return (short) Integer.parseUnsignedInt(str, 16);
    }

    /**
     * Method to convert a list of node IDs to a list of hex strings
     *
     * @param p_nodeIDs
     *     Node IDs
     * @return String with list of node IDs in hex
     */
    public static String nodeIDArrayToString(final short[] p_nodeIDs) {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < p_nodeIDs.length; i++) {
            builder.append(toHexString(p_nodeIDs[i]));

            if (i + 1 < p_nodeIDs.length) {
                builder.append(", ");
            }
        }

        return builder.toString();
    }
}

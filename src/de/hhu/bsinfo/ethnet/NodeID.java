package de.hhu.bsinfo.ethnet;

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
}

package de.hhu.bsinfo.ethnet;

import java.net.InetSocketAddress;

/**
 * An interface to map NodeIDs
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.11.2015
 */
public interface NodeMap {

    /**
     * Returns the NodeID
     *
     * @return the NodeID
     */
    short getOwnNodeID();

    /**
     * Returns the address
     *
     * @param p_nodeID
     *         the NodeID
     * @return the address
     */
    InetSocketAddress getAddress(short p_nodeID);
}

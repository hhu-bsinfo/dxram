package de.hhu.bsinfo.dxram.net;

import java.net.InetSocketAddress;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.ethnet.NodeMap;

/**
 * Wrapper interface to hide the boot component for ethnet
 * but give access to the list of participating machines (ip, port).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
class NodeMappings implements NodeMap {

    private AbstractBootComponent m_boot;

    /**
     * Constructor
     *
     * @param p_bootComponent
     *     Boot component instance to wrap.
     */
    NodeMappings(final AbstractBootComponent p_bootComponent) {
        m_boot = p_bootComponent;
    }

    @Override
    public short getOwnNodeID() {
        return m_boot.getNodeID();
    }

    @Override
    public InetSocketAddress getAddress(final short p_nodeID) {
        return m_boot.getNodeAddress(p_nodeID);
    }
}

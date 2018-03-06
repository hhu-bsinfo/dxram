/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.net;

import java.net.InetSocketAddress;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxnet.NodeMap;

/**
 * Wrapper interface to hide the boot component for dxnet
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

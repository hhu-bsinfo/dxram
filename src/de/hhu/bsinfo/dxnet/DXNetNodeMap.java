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

package de.hhu.bsinfo.dxnet;

import java.net.InetSocketAddress;

/**
 * Implementation of NodeMap for DXNet usage without DXRAM.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 21.09.2017
 */
public class DXNetNodeMap implements NodeMap {

    private final short m_nodeID;
    private final InetSocketAddress[] m_nodeMap = new InetSocketAddress[(int) Math.pow(2, 16)];

    /**
     * Creates an instance of DXNetNodeMap
     *
     * @param p_ownNodeID
     *         the NodeID
     */
    public DXNetNodeMap(final short p_ownNodeID) {
        m_nodeID = p_ownNodeID;
    }

    /**
     * Add a node to node map.
     *
     * @param p_nodeID
     *         the node ID.
     * @param p_address
     *         the address.
     */
    public void addNode(final short p_nodeID, final InetSocketAddress p_address) {
        m_nodeMap[p_nodeID & 0xFFFF] = p_address;
    }

    @Override
    public short getOwnNodeID() {
        return m_nodeID;
    }

    @Override
    public InetSocketAddress getAddress(short p_nodeID) {
        return m_nodeMap[p_nodeID & 0xFFFF];
    }
}

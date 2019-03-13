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
import java.util.ArrayList;
import java.util.List;

import de.hhu.bsinfo.dxnet.NodeMap;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.boot.NodeRegistry;

/**
 * Wrapper interface to hide the boot component for dxnet
 * but give access to the list of participating machines (ip, port).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
class NodeMappings implements NodeMap, NodeRegistry.Listener {
    private BootComponent m_boot;

    private List<NodeMap.Listener> m_listener;

    /**
     * Constructor
     *
     * @param p_bootComponent
     *         Boot component instance to wrap.
     */
    NodeMappings(final BootComponent p_bootComponent) {
        m_boot = p_bootComponent;
        m_listener = new ArrayList<>();

        m_boot.registerRegistryListener(this);
    }

    @Override
    public short getOwnNodeID() {
        return m_boot.getNodeId();
    }

    @Override
    public InetSocketAddress getAddress(final short p_nodeID) {
        return m_boot.getNodeAddress(p_nodeID);
    }

    @Override
    public List<Mapping> getAvailableMappings() {
        List<Mapping> mappings = new ArrayList<>();
        List<NodeRegistry.NodeDetails> details = m_boot.getOnlineNodes();

        for (NodeRegistry.NodeDetails detail : details) {
            mappings.add(new Mapping(detail.getId(), detail.getAddress()));
        }

        return mappings;
    }

    @Override
    public void registerListener(final Listener p_listener) {
        m_listener.add(p_listener);
    }

    @Override
    public void onPeerJoined(final NodeRegistry.NodeDetails p_nodeDetails) {
        for (NodeMap.Listener listener : m_listener) {
            listener.nodeMappingAdded(p_nodeDetails.getId(), p_nodeDetails.getAddress());
        }
    }

    @Override
    public void onPeerLeft(final NodeRegistry.NodeDetails p_nodeDetails) {
        for (NodeMap.Listener listener : m_listener) {
            listener.nodeMappingRemoved(p_nodeDetails.getId());
        }
    }

    @Override
    public void onSuperpeerJoined(final NodeRegistry.NodeDetails p_nodeDetails) {
        for (NodeMap.Listener listener : m_listener) {
            listener.nodeMappingAdded(p_nodeDetails.getId(), p_nodeDetails.getAddress());
        }
    }

    @Override
    public void onSuperpeerLeft(final NodeRegistry.NodeDetails p_nodeDetails) {
        for (NodeMap.Listener listener : m_listener) {
            listener.nodeMappingRemoved(p_nodeDetails.getId());
        }
    }

    @Override
    public void onNodeUpdated(final NodeRegistry.NodeDetails p_nodeDetails) {

    }
}

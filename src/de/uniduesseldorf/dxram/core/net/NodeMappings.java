package de.uniduesseldorf.dxram.core.net;

import java.net.InetSocketAddress;

import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration.NodeEntry;

import de.uniduesseldorf.menet.NodeMap;

public class NodeMappings implements NodeMap {

	private NodesConfiguration m_nodesConfiguration;
	
	public NodeMappings(final NodesConfiguration p_nodesConfiguration)
	{
		m_nodesConfiguration = p_nodesConfiguration;
	}
	
	@Override
	public InetSocketAddress getAddress(short p_nodeID) {
		NodeEntry entry = m_nodesConfiguration.getNode(p_nodeID);
				
		return new InetSocketAddress(entry.getIP(), entry.getPort());
	}

}

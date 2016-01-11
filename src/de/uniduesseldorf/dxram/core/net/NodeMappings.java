package de.uniduesseldorf.dxram.core.net;

import java.net.InetSocketAddress;

import de.uniduesseldorf.dxram.core.boot.BootComponent;

import de.uniduesseldorf.menet.NodeMap;

public class NodeMappings implements NodeMap {

	private BootComponent m_boot;
	
	public NodeMappings(final BootComponent p_bootComponent)
	{
		m_boot = p_bootComponent;
	}
	
	@Override
	public short getOwnNodeID() {
		return m_boot.getNodeID();
	}
	
	@Override
	public InetSocketAddress getAddress(short p_nodeID) {
		return m_boot.getNodeAddress(p_nodeID);
	}
}

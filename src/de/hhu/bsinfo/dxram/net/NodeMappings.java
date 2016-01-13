package de.hhu.bsinfo.dxram.net;

import java.net.InetSocketAddress;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.menet.NodeMap;

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

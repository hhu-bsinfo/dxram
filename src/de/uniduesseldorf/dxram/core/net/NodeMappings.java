package de.uniduesseldorf.dxram.core.net;

import java.net.InetSocketAddress;

import de.uniduesseldorf.dxram.core.engine.DXRAMSystemData;

import de.uniduesseldorf.menet.NodeMap;

public class NodeMappings implements NodeMap {

	private DXRAMSystemData m_dxramSystemData;
	
	public NodeMappings(final DXRAMSystemData p_dxramSystemData)
	{
		m_dxramSystemData = p_dxramSystemData;
	}
	
	@Override
	public short getOwnNodeID() {
		return m_dxramSystemData.getNodeID();
	}
	
	@Override
	public InetSocketAddress getAddress(short p_nodeID) {
		return m_dxramSystemData.getNodeAddress(p_nodeID);
	}
}

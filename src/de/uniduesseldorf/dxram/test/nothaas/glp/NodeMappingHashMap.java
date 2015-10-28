package de.uniduesseldorf.dxram.test.nothaas.glp;

import java.util.concurrent.ConcurrentHashMap;

public class NodeMappingHashMap implements NodeMapping
{
	private ConcurrentHashMap<Long, Long> m_hashMap;
	
	public NodeMappingHashMap()
	{
		m_hashMap = new ConcurrentHashMap<Long, Long>();
	}
	
	@Override
	public long getChunkIDForNodeID(long p_nodeID) 
	{
		Long value = m_hashMap.get(p_nodeID);
		if (value == null)
			return -1;
		else
			return value;
	}

	@Override
	public void setChunkIDForNodeID(long p_nodeID, long p_chunkID) {
		m_hashMap.put(p_nodeID, p_chunkID);
	}
}

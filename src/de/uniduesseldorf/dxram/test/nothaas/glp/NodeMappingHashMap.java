package de.uniduesseldorf.dxram.test.nothaas.glp;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import de.uniduesseldorf.dxram.utils.Pair;

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

	@Override
	public long getNumMappingEntries()
	{
		return m_hashMap.size();
	}

	@Override
	public Iterator<Pair<Long, Long>> getIterator() 
	{
		return new IteratorNodeMappingHashMap();
	}
	
	public class IteratorNodeMappingHashMap implements Iterator<Pair<Long, Long>>
	{
		private Set<Entry<Long, Long>> m_elements = null;
		private Iterator<Entry<Long, Long>> m_iterator = null;
		
		public IteratorNodeMappingHashMap()
		{
			m_elements = m_hashMap.entrySet();
			m_iterator = m_elements.iterator();
		}
		
		@Override
		public boolean hasNext() 
		{
			return m_iterator.hasNext();
		}

		@Override
		public Pair<Long, Long> next() 
		{
			Entry<Long, Long> entry = m_iterator.next();
			return new Pair<Long, Long>(entry.getKey(), entry.getValue());
		}
		
	}
}

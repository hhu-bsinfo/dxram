package de.hhu.bsinfo.dxgraph.conv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VertexStorageBinarySimple implements VertexStorageBinary {

	private static class NeighbourListVertex
	{
		public ConcurrentLinkedQueue<Long> m_neighbourList = new ConcurrentLinkedQueue<Long>();

		public NeighbourListVertex()
		{
		
		}
	}
	
	private Map<Long, Long> m_idMapping = new HashMap<Long, Long>();
	private ArrayList<NeighbourListVertex> m_neighbourListsVertices = new ArrayList<NeighbourListVertex>();
	
	private AtomicLong m_totalEdgeCount = new AtomicLong(0L);
	
	private Lock m_mutex = new ReentrantLock(false);
	
	public VertexStorageBinarySimple()
	{
		
	}
	
	@Override
	public long getVertexId(final long p_hashValue)
	{
		Long vertexId = m_idMapping.get(p_hashValue);
		if (vertexId == null)
		{
			m_mutex.lock();
			vertexId = m_idMapping.get(p_hashValue);
			if (vertexId == null)
			{
				vertexId = (long) (m_neighbourListsVertices.size() + 1);
				m_neighbourListsVertices.add(new NeighbourListVertex());
				m_idMapping.put(p_hashValue, vertexId);
				
			}
			m_mutex.unlock();
		}
		
		return vertexId;
	}
	
	@Override
	public void putNeighbour(long p_vertexId, long p_neighbourVertexId)
	{
		NeighbourListVertex neighbourList = m_neighbourListsVertices.get((int) p_vertexId - 1);
		
		neighbourList.m_neighbourList.add(p_neighbourVertexId);
		
		m_totalEdgeCount.incrementAndGet();
	}
	
	@Override
	public long getTotalVertexCount()
	{
		return m_neighbourListsVertices.size();
	}
	
	@Override
	public long getTotalEdgeCount() {
		return m_totalEdgeCount.get();
	}

	@Override
	public ConcurrentLinkedQueue<Long> getVertexNeighbourList(final long p_vertexId) {
		return m_neighbourListsVertices.get((int) p_vertexId).m_neighbourList;
	}
}

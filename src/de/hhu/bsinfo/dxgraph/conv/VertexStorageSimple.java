package de.hhu.bsinfo.dxgraph.conv;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.locks.SpinLock;

// not thread safe, single threaded only
public class VertexStorageSimple implements VertexStorage {

	private static class NeighbourListVertex
	{
		public Pair<Long, String> m_neighbourList = new Pair<Long, String>(0L, new String());

		// TODO use cheaper lock
		public Lock m_mutex = new SpinLock();
		
		public NeighbourListVertex()
		{
		
		}
	}
	
	private Map<Long, Long> m_idMapping = new HashMap<Long, Long>();
	private Vector<NeighbourListVertex> m_neighbourListsVertices = new Vector<NeighbourListVertex>();
	
	private AtomicLong m_totalEdgeCount = new AtomicLong(0L);
	
	private Lock m_mutex = new SpinLock();
	
	public VertexStorageSimple()
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
		
		neighbourList.m_mutex.lock();
		
		if (!neighbourList.m_neighbourList.m_second.isEmpty()) {
			neighbourList.m_neighbourList.m_second += ", ";
		}
		neighbourList.m_neighbourList.m_second += Long.toString(p_neighbourVertexId);
		neighbourList.m_neighbourList.m_first++;
		
		neighbourList.m_mutex.unlock();
		
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
	public Pair<Long, String> getVertexNeighbourList(final long p_vertexId) {
		return m_neighbourListsVertices.get((int) p_vertexId).m_neighbourList;
	}
}

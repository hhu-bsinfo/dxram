
package de.hhu.bsinfo.dxgraph.conv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Very simple/naive implementation for a storage using a hash map.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
public class VertexStorageBinarySimple implements VertexStorage {

	private Map<Long, Long> m_idMapping = new HashMap<Long, Long>();
	private ArrayList<NeighbourListVertex> m_neighbourListsVertices = new ArrayList<NeighbourListVertex>();

	private AtomicLong m_totalVertexCount = new AtomicLong(0);
	private AtomicLong m_totalEdgeCount = new AtomicLong(0L);

	private Lock m_mutex = new ReentrantLock(false);

	/**
	 * Constructor
	 */
	public VertexStorageBinarySimple() {

	}

	/**
	 * Get the full neighbor list of one vertex.
	 *
	 * @param p_vertexId Id of the vertex.
	 * @return Neighbor list.
	 */
	public ConcurrentLinkedQueue<Long> getVertexNeighbourList(final long p_vertexId) {
		return m_neighbourListsVertices.get((int) p_vertexId).m_neighbourList;
	}

	@Override
	public long getVertexId(final long p_hashValue) {
		Long vertexId = m_idMapping.get(p_hashValue);
		if (vertexId == null) {
			m_mutex.lock();
			vertexId = m_idMapping.get(p_hashValue);
			if (vertexId == null) {
				vertexId = (long) (m_neighbourListsVertices.size() + 1);
				m_neighbourListsVertices.add(new NeighbourListVertex());
				m_idMapping.put(p_hashValue, vertexId);
				m_totalVertexCount.incrementAndGet();
			}
			m_mutex.unlock();
		}

		return vertexId;
	}

	@Override
	public void putNeighbour(final long p_vertexId, final long p_neighbourVertexId) {
		NeighbourListVertex neighbourList = m_neighbourListsVertices.get((int) p_vertexId - 1);

		neighbourList.m_neighbourList.add(p_neighbourVertexId);

		m_totalEdgeCount.incrementAndGet();
	}

	@Override
	public long getNeighbours(long p_vertexId, long[] p_buffer) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public long getTotalVertexCount() {
		return m_totalVertexCount.get();
	}

	@Override
	public long getTotalEdgeCount() {
		return m_totalEdgeCount.get();
	}

	@Override
	public long getTotalMemoryDataStructures() {
		throw new RuntimeException("Not implemented");
	}

	/**
	 * Helper/Container class for a neighbor list.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
	 */
	private static class NeighbourListVertex {
		public ConcurrentLinkedQueue<Long> m_neighbourList = new ConcurrentLinkedQueue<Long>();
	}
}

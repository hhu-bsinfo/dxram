
package de.hhu.bsinfo.dxgraph.conv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.utils.Pair;

/**
 * Very simple/naive implementation of a text based vertex storage.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
class VertexStorageTextSimple implements VertexStorageText {

	private Map<Long, Long> m_idMapping = new HashMap<>();
	private ArrayList<NeighbourListVertex> m_neighbourListsVertices = new ArrayList<>();

	private AtomicLong m_totalEdgeCount = new AtomicLong(0L);

	private Lock m_mutex = new ReentrantLock(false);

	/**
	 * Constructor
	 */
	VertexStorageTextSimple() {

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

			}
			m_mutex.unlock();
		}

		return vertexId;
	}

	@Override
	public void putNeighbour(final long p_vertexId, final long p_neighbourVertexId) {
		NeighbourListVertex neighbourList = m_neighbourListsVertices.get((int) p_vertexId - 1);

		neighbourList.m_mutex.lock();

		if (!neighbourList.m_neighbourList.m_second.isEmpty()) {
			neighbourList.m_neighbourList.m_second += ",";
		}
		neighbourList.m_neighbourList.m_second += Long.toString(p_neighbourVertexId);
		neighbourList.m_neighbourList.m_first++;

		neighbourList.m_mutex.unlock();

		m_totalEdgeCount.incrementAndGet();
	}

	@Override
	public long getNeighbours(final long p_vertexId, final long[] p_buffer) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public long getTotalVertexCount() {
		return m_neighbourListsVertices.size();
	}

	@Override
	public long getTotalEdgeCount() {
		return m_totalEdgeCount.get();
	}

	@Override
	public long getTotalMemoryDataStructures() {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public Pair<Long, String> getVertexNeighbourList(final long p_vertexId) {
		return m_neighbourListsVertices.get((int) p_vertexId).m_neighbourList;
	}

	/**
	 * Private container/helper class for a neighbour list.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
	 */
	private static class NeighbourListVertex {
		public Pair<Long, String> m_neighbourList = new Pair<>(0L, "");
		public Lock m_mutex = new ReentrantLock(false);
	}
}

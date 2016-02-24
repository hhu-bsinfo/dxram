package de.hhu.bsinfo.dxgraph.conv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxgraph.data.Vertex;

// not thread safe, single threaded only
public class VertexStorageSimple implements VertexStorage {

	private Map<Long, Long> m_idMapping = new HashMap<Long, Long>();
	private ArrayList<Vertex> m_vertices = new ArrayList<Vertex>();
	
	private long m_totalEdgeCount = 0;
	
	public VertexStorageSimple()
	{
		
	}
	
	@Override
	public long getVertexId(final long p_hashValue)
	{
		Long vertexId = m_idMapping.get(p_hashValue);
		if (vertexId == null)
		{
			vertexId = (long) (m_vertices.size() + 1);
			m_vertices.add(new Vertex(vertexId));
			m_idMapping.put(p_hashValue, vertexId);
		}
		
		return vertexId;
	}
	
	@Override
	public void putNeighbour(long p_vertexId, long p_neighbourVertexId)
	{
		Vertex vertex = m_vertices.get((int) p_vertexId - 1);
		vertex.getNeighbours().add(p_neighbourVertexId);
		m_totalEdgeCount++;
	}
	
	@Override
	public long getTotalVertexCount()
	{
		return m_vertices.size();
	}
	
	@Override
	public long getTotalEdgeCount() {
		return m_totalEdgeCount;
	}

	@Override
	public Vertex getVertex(long p_vertexId) {
		return m_vertices.get((int) p_vertexId);
	}
}

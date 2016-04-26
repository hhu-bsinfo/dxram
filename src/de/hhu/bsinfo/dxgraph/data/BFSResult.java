
package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

public class BFSResult implements DataStructure {

	private long m_id = ChunkID.INVALID_ID;
	private long m_rootVertexId = ChunkID.INVALID_ID;
	private long m_totalVisitedVertices;
	private int m_totalBFSDepth;

	public BFSResult() {

	}

	public long getRootVertexID() {
		return m_rootVertexId;
	}

	public void setRootVertexID(final long p_vertexID) {
		m_rootVertexId = p_vertexID;
	}

	public long getTotalVisitedVertices() {
		return m_totalVisitedVertices;
	}

	public void setTotalVisitedVertices(final long p_totalVisited) {
		m_totalVisitedVertices = p_totalVisited;
	}

	public int getTotalBFSDepth() {
		return m_totalBFSDepth;
	}

	public void setTotalBFSDepth(final int p_totalBFSDepth) {
		m_totalBFSDepth = p_totalBFSDepth;
	}

	@Override
	public long getID() {
		return m_id;
	}

	@Override
	public void setID(final long p_id) {
		m_id = p_id;
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int sizeofObject() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		return ChunkID.toHexString(m_id) + "[m_rootVertexId " + m_rootVertexId + ", m_totalVisitedVertices "
				+ m_totalVisitedVertices + ", " + m_totalBFSDepth + "]";
	}
}


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
		m_rootVertexId = p_importer.readLong();
		m_totalVisitedVertices = p_importer.readLong();
		m_totalBFSDepth = p_importer.readInt();

		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return Long.BYTES * 2 + Integer.BYTES;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return false;
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		p_exporter.writeLong(m_rootVertexId);
		p_exporter.writeLong(m_totalVisitedVertices);
		p_exporter.writeInt(m_totalBFSDepth);

		return sizeofObject();
	}

	@Override
	public String toString() {
		return "BFSResult " + ChunkID.toHexString(m_id) + " [m_rootVertexId " + ChunkID.toHexString(m_rootVertexId)
				+ ", m_totalVisitedVertices "
				+ m_totalVisitedVertices + ", total iteration depth " + m_totalBFSDepth + "]";
	}
}

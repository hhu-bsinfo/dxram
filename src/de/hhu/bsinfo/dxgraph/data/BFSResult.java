
package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Data structure holding results of a single BFS run.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 20.05.16
 */
public class BFSResult implements DataStructure {

	private long m_id = ChunkID.INVALID_ID;
	public long m_rootVertexId = ChunkID.INVALID_ID;
	public long m_graphSizeVertices = 0;
	public long m_graphSizeEdges = 0;
	public long m_totalVisitedVertices = 0;
	public long m_totalEdgesTraversed = 0;
	public long m_maxVertsPerSecond = 0;
	public long m_maxEdgesPerSecond = 0;
	public long m_avgVertsPerSecond = 0;
	public long m_avgEdgesPerSecond = 0;
	public long m_totalTimeMs = 0;
	public int m_totalBFSDepth = 0;

	public BFSResult() {

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
		m_graphSizeVertices = p_importer.readLong();
		m_graphSizeEdges = p_importer.readLong();
		m_totalVisitedVertices = p_importer.readLong();
		m_totalEdgesTraversed = p_importer.readLong();
		m_maxVertsPerSecond = p_importer.readLong();
		m_maxEdgesPerSecond = p_importer.readLong();
		m_avgVertsPerSecond = p_importer.readLong();
		m_avgEdgesPerSecond = p_importer.readLong();
		m_totalTimeMs = p_importer.readLong();
		m_totalBFSDepth = p_importer.readInt();

		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return Long.BYTES * 10 + Integer.BYTES;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return false;
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		p_exporter.writeLong(m_rootVertexId);
		p_exporter.writeLong(m_graphSizeVertices);
		p_exporter.writeLong(m_graphSizeEdges);
		p_exporter.writeLong(m_totalVisitedVertices);
		p_exporter.writeLong(m_totalEdgesTraversed);
		p_exporter.writeLong(m_maxVertsPerSecond);
		p_exporter.writeLong(m_maxEdgesPerSecond);
		p_exporter.writeLong(m_avgVertsPerSecond);
		p_exporter.writeLong(m_avgEdgesPerSecond);
		p_exporter.writeLong(m_totalTimeMs);
		p_exporter.writeInt(m_totalBFSDepth);

		return sizeofObject();
	}

	@Override
	public String toString() {
		return "BFSResult " + ChunkID.toHexString(m_id) + ":\n"
				+ "m_rootVertexId " + ChunkID.toHexString(m_rootVertexId) + "\n"
				+ "m_graphSizeVertices " + m_graphSizeVertices + "\n"
				+ "m_graphSizeEdges " + m_graphSizeEdges + "\n"
				+ "m_totalVisitedVertices " + m_totalVisitedVertices + "\n"
				+ "m_totalEdgesTraversed " + m_totalEdgesTraversed + "\n"
				+ "m_maxVertsPerSecond " + m_maxVertsPerSecond + "\n"
				+ "m_maxEdgesPerSecond " + m_maxEdgesPerSecond + "\n"
				+ "m_avgVertsPerSecond " + m_avgVertsPerSecond + "\n"
				+ "m_avgEdgesPerSecond " + m_avgEdgesPerSecond + "\n"
				+ "m_totalTimeMs " + m_totalTimeMs + "\n"
				+ "m_totalBFSDepth " + m_totalBFSDepth;
	}
}

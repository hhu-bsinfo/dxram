
package de.hhu.bsinfo.dxgraph.data;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Data structure holding BFS results of multiple nodes.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 20.05.16
 */
public class BFSResults implements DataStructure {
	private long m_id = ChunkID.INVALID_ID;
	private BFSResult m_aggregatedResult = new BFSResult();
	private ArrayList<Pair<Integer, BFSResult>> m_bfsResults = new ArrayList<>();

	public BFSResults() {

	}

	/**
	 * Get aggregated results of all nodes.
	 *
	 * @return Aggregated results.
	 */
	public BFSResult getAggregatedResult() {
		return m_aggregatedResult;
	}

	/**
	 * Add a result of a node running BFS.
	 *
	 * @param p_computeSlaveId Compute slave id of the node.
	 * @param p_nodeId         Node id of the node.
	 * @param p_bfsResult      BFS result of the node.
	 */
	public void addResult(final short p_computeSlaveId, final short p_nodeId, final BFSResult p_bfsResult) {
		int id = (p_nodeId << 16) | p_computeSlaveId;
		m_bfsResults.add(new Pair<>(id, p_bfsResult));
	}

	/**
	 * Get all single results of every BFS node.
	 *
	 * @return List of single results identified by compute slave id and node id.
	 */
	public ArrayList<Pair<Integer, BFSResult>> getResults() {
		return m_bfsResults;
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
	public void importObject(final Importer p_importer) {
		m_aggregatedResult = new BFSResult();
		m_aggregatedResult.importObject(p_importer);

		int size = p_importer.readInt();
		for (int i = 0; i < size; i++) {
			int id = p_importer.readInt();
			BFSResult result = new BFSResult();
			result.importObject(p_importer);
			m_bfsResults.add(new Pair<>(id, result));
		}
	}

	@Override
	public int sizeofObject() {
		int size = m_aggregatedResult.sizeofObject();
		size += Integer.BYTES;
		for (Pair<Integer, BFSResult> entry : m_bfsResults) {
			size += Integer.BYTES + entry.m_second.sizeofObject();
		}
		return size;
	}

	@Override
	public void exportObject(final Exporter p_exporter) {
		m_aggregatedResult.exportObject(p_exporter);

		p_exporter.writeInt(m_bfsResults.size());
		for (Pair<Integer, BFSResult> entry : m_bfsResults) {
			p_exporter.writeInt(entry.m_first);
			entry.second().exportObject(p_exporter);
		}
	}

	@Override
	public String toString() {
		String str = "";

		str += "Aggregated result:\n" + m_aggregatedResult + "\n--------------------------";

		str += "Node count " + m_bfsResults.size();
		for (Pair<Integer, BFSResult> entry : m_bfsResults) {
			str += "\n>>>>> " + NodeID.toHexString((short) (entry.first() >> 16))
					+ " | " + (entry.first() & 0xFFFF) + ": \n";
			str += entry.m_second;
		}

		return str;
	}
}

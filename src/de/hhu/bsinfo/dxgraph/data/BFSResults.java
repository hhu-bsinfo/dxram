package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

import java.util.ArrayList;

/**
 * Created by nothaas on 5/19/16.
 */
public class BFSResults implements DataStructure {
	private long m_id = ChunkID.INVALID_ID;
	private BFSResult m_aggregatedResult = new BFSResult();
	private ArrayList<Pair<Integer, BFSResult>> m_bfsResults = new ArrayList<>();

	public BFSResults() {

	}

	public BFSResult getAggregatedResult() {
		return m_aggregatedResult;
	}

	public void addResult(final short p_computeSlaveId, final short p_nodeId, final BFSResult p_bfsResult) {
		int id = (p_nodeId << 16) | p_computeSlaveId;
		m_bfsResults.add(new Pair<>(id, p_bfsResult));
	}

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
	public int importObject(final Importer p_importer, final int p_size) {
		m_aggregatedResult = new BFSResult();
		m_aggregatedResult.importObject(p_importer, p_size);

		int size = p_importer.readInt();
		for (int i = 0; i < size; i++) {
			int id = p_importer.readInt();
			BFSResult result = new BFSResult();
			result.importObject(p_importer, p_size);
			m_bfsResults.add(new Pair<>(id, result));
		}

		return sizeofObject();
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
	public boolean hasDynamicObjectSize() {
		return true;
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		m_aggregatedResult.exportObject(p_exporter, p_size);

		p_exporter.writeInt(m_bfsResults.size());
		for (Pair<Integer, BFSResult> entry : m_bfsResults) {
			p_exporter.writeInt(entry.m_first);
			entry.second().exportObject(p_exporter, p_size);
		}

		return sizeofObject();
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

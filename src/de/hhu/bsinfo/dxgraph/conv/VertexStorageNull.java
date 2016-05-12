package de.hhu.bsinfo.dxgraph.conv;

/**
 * Created by nothaas on 5/11/16.
 */
public class VertexStorageNull implements VertexStorage {

	@Override
	public long getVertexId(long p_hashValue) {
		return p_hashValue;
	}

	@Override
	public void putNeighbour(long p_vertexId, long p_neighbourVertexId) {

	}

	@Override
	public long getNeighbours(long p_vertexId, long[] p_buffer) {
		return 0;
	}

	@Override
	public long getTotalVertexCount() {
		return 0;
	}

	@Override
	public long getTotalEdgeCount() {
		return 0;
	}

	@Override
	public long getTotalMemoryDataStructures() {
		return 0;
	}
}

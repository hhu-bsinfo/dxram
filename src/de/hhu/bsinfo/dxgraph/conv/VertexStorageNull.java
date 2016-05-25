package de.hhu.bsinfo.dxgraph.conv;

/**
 * Dummy storage implementation for testing other parts of the converter.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.05.16
 */
public class VertexStorageNull implements VertexStorage {

	@Override
	public long getVertexId(final long p_hashValue) {
		return p_hashValue;
	}

	@Override
	public void putNeighbour(final long p_vertexId, final long p_neighbourVertexId) {

	}

	@Override
	public long getNeighbours(final long p_vertexId, final long[] p_buffer) {
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

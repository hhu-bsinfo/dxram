package de.hhu.bsinfo.dxgraph.conv;

public interface VertexStorage {
	// always return a vertex for the specified ID
	// the id given does not have to be a continuous ID
	// and should NOT be the ID assigned to the vertex.
	// make sure to generate continuous IDs on your own
	// and assign these to the vertices
	// make sure these IDs are starting at 1 (not 0)
	// ensure this call is thread safe
	long getVertexId(final long p_hashValue);
	
	void putNeighbour(long p_vertexId, long p_neighbourVertexId);
	
	// which also equals the highest ID
	long getTotalVertexCount();
	
	long getTotalEdgeCount();
}

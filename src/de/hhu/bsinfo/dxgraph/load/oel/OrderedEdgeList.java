package de.hhu.bsinfo.dxgraph.load.oel;

import de.hhu.bsinfo.dxgraph.data.Vertex;

// interface for an ordered edge list
// which contains lists of neighbors for each vertex
// vertex indices should start with id 1
public interface OrderedEdgeList {
	
	// when having multiple lists, identifies order
	// (splitting the file and loading from multiple nodes)
	public int getNodeIndex();
	
	public long getTotalVertexCount();

	// read vertex data. does not re-base 
	// the ids of the neighbors or modify the read data
	public Vertex readVertex();
}

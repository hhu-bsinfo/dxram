
package de.hhu.bsinfo.dxgraph.load.oel;

import de.hhu.bsinfo.dxgraph.data.Vertex2;

// interface for an ordered edge list
// which contains lists of neighbors for each vertex
// vertex indices should start with id 1
public interface OrderedEdgeList {

	// when having multiple lists, identifies order
	// (splitting the file and loading from multiple nodes)
	// TODO remove this, have this with the index
	int getNodeIndex();

	// TODO remove this, have this with the index
	long getTotalVertexCount();

	// read vertex data. does not re-base
	// the ids of the neighbors or modify the read data
	Vertex2 readVertex();
}

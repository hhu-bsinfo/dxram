package de.hhu.bsinfo.dxgraph.conv;

import de.hhu.bsinfo.utils.Pair;

public interface VertexStorageText extends VertexStorage
{	
	// getting vertices for exporting
	Pair<Long, String> getVertexNeighbourList(final long p_vertexId);
	
}

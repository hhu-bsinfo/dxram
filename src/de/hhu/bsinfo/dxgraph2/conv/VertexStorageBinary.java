package de.hhu.bsinfo.dxgraph.conv;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

public interface VertexStorageBinary extends VertexStorage
{	
	// getting vertices for exporting
	ConcurrentLinkedQueue<Long> getVertexNeighbourList(final long p_vertexId);
	
}

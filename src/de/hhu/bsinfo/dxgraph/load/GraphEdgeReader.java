package de.hhu.bsinfo.dxgraph.load;

import java.util.Vector;

import de.hhu.bsinfo.utils.Pair;

public interface GraphEdgeReader 
{
	int readEdges(Vector<Pair<Long, Long>> p_buffer, int p_count);
	
	long getTotalNumberOfEdges();
}

package de.uniduesseldorf.dxgraph.load;

import java.util.Vector;

import de.uniduesseldorf.utils.Pair;

public interface GraphEdgeReader 
{
	int readEdges(Vector<Pair<Long, Long>> p_buffer, int p_count);
	
	long getTotalNumberOfEdges();
}

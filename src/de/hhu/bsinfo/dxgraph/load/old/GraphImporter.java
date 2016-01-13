package de.hhu.bsinfo.dxgraph.load.old;

import de.hhu.bsinfo.dxgraph.load.NodeMapping;

public interface GraphImporter 
{
	boolean addEdge(int p_instance, int p_totalInstances, long p_nodeFrom, long p_nodeTo, NodeMapping p_nodeMapping);
}

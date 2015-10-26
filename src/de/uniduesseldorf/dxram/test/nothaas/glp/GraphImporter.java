package de.uniduesseldorf.dxram.test.nothaas.glp;

public interface GraphImporter 
{
	boolean addEdge(int p_instance, int p_totalInstances, long p_nodeFrom, long p_nodeTo, NodeMapping p_nodeMapping);
}

package de.hhu.bsinfo.dxgraph.load;

public interface GraphLoaderResultDelegate 
{
	long[] getRoots();
	
	long getTotalVertexCount();
	
	long getTotalEdgeCount();
}

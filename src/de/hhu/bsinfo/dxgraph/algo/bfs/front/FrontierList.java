package de.hhu.bsinfo.dxgraph.algo.bfs.front;

public interface FrontierList {

	void pushBack(final long p_val);
	
	long size();
	
	boolean isEmpty();
	
	void reset();
	
	long popFront();
}

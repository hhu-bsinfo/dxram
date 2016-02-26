package de.hhu.bsinfo.dxgraph.load;

public interface RebaseVertexID {

	long rebase(final long p_id);
	
	void rebase(final long[] p_ids);
}

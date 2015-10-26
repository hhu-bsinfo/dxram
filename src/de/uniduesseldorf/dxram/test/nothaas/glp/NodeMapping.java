package de.uniduesseldorf.dxram.test.nothaas.glp;

public interface NodeMapping {

	long getChunkIDForNodeID(long p_nodeID);
	
	void setChunkIDForNodeID(long p_nodeID, long p_chunkID);
}

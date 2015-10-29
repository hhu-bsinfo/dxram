package de.uniduesseldorf.dxram.test.nothaas.glp;

import java.util.Iterator;

import de.uniduesseldorf.dxram.utils.Pair;

public interface NodeMapping {

	long getChunkIDForNodeID(long p_nodeID);
	
	void setChunkIDForNodeID(long p_nodeID, long p_chunkID);
	
	long getNumMappingEntries();
	
	Iterator<Pair<Long, Long>> getIterator();
}

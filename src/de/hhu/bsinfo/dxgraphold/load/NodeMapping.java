package de.hhu.bsinfo.dxgraph.load;

import java.util.Iterator;

import de.hhu.bsinfo.utils.Pair;

public interface NodeMapping {

	long getChunkIDForNodeID(long p_nodeID);
	
	void setChunkIDForNodeID(long p_nodeID, long p_chunkID);
	
	long getNumMappingEntries();
	
	Iterator<Pair<Long, Long>> getIterator();
}

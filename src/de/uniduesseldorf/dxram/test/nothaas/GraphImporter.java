package de.uniduesseldorf.dxram.test.nothaas;

import java.io.File;
import java.util.List;

import de.uniduesseldorf.dxram.utils.Pair;

public interface GraphImporter 
{
	// make sure to use the same table across multiple threads
	// and that the table is thread safe
	public void setMappingTable(EdgeListNodeIDMappingTable mappingTable);
	
	// false if opening the file or anything related failed
	// can be called multiple times
	public boolean setEdgeInputFile(File edgeFile);
	
	// when returning null, no edges left, otherwise valid edge read
	public List<Pair<Long, Long>> readEdges(int numEdges);
	
	// valid chunk id or -1 if not occupied
	public long getChunkIDForNode(long node);
	
	public void setChunkIDForNode(long node, long chunkID);
}

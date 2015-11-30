package de.uniduesseldorf.dxgraph.load;

import java.util.Iterator;

import de.uniduesseldorf.dxram.core.chunk.storage.PagingTable;
import de.uniduesseldorf.dxram.core.chunk.storage.SmallObjectHeap;
import de.uniduesseldorf.dxram.core.chunk.storage.StorageUnsafeMemory;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Pair;

public class NodeMappingPagingInMemory implements NodeMapping
{
	private SmallObjectHeap m_rawMemory = null;
	private PagingTable m_nodeMappingTable = null;
	
	public NodeMappingPagingInMemory() throws MemoryException
	{
		m_rawMemory = new SmallObjectHeap(new StorageUnsafeMemory());
		// 1GB ram and 128mb segments  
		m_rawMemory.initialize(1024 * 1024 * 1024 * 2L, 1024 * 1024 * 128);
		m_nodeMappingTable = new PagingTable();
		m_nodeMappingTable.initialize(m_rawMemory);
	}
	
	@Override
	public long getChunkIDForNodeID(long p_nodeID) 
	{
		try {
			long chunkID = m_nodeMappingTable.get(p_nodeID);
			if (chunkID == 0)
				return -1;
			else
				return chunkID;
		} catch (MemoryException e) {
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public void setChunkIDForNodeID(long p_nodeID, long p_chunkID) {
		try {
			m_nodeMappingTable.set(p_nodeID, p_chunkID);
		} catch (MemoryException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public long getNumMappingEntries()
	{
		// TODO ?
		return -1;
	}

	@Override
	public Iterator<Pair<Long, Long>> getIterator() {
		// TODO Auto-generated method stub
		return null;
	}
}

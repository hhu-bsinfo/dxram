package de.uniduesseldorf.dxram.test.nothaas.glp;

import de.uniduesseldorf.dxram.core.chunk.storage.PagingTable;
import de.uniduesseldorf.dxram.core.chunk.storage.RawMemory;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

public class NodeMappingPagingInMemory implements NodeMapping
{
	private RawMemory m_rawMemory = null;
	private PagingTable m_nodeMappingTable = null;
	
	public NodeMappingPagingInMemory() throws MemoryException
	{
		m_rawMemory = new RawMemory();
		m_rawMemory.initialize(1024 * 1024 * 1024, 1024 * 1024 * 16); // XXX should be big enough? maybe a little less does the job as well...
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

}

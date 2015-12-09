package de.uniduesseldorf.dxgraph.load.old;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;

import de.uniduesseldorf.dxgraph.load.NodeMapping;

public class GraphImporterSimple implements GraphImporter
{
	public GraphImporterSimple()
	{
		
	}
	
	@Override
	public boolean addEdge(int p_instance, int p_totalInstances, long p_nodeFrom, long p_nodeTo,
			NodeMapping p_nodeMapping) 
	{
		try 
		{
			long chunkIDFrom = Chunk.INVALID_CHUNKID;
			long chunkIDTo = Chunk.INVALID_CHUNKID;
			
			// target node
			chunkIDTo = p_nodeMapping.getChunkIDForNodeID(p_nodeTo);
			// only check if exists and put back new chunk
			if (chunkIDTo == Chunk.INVALID_CHUNKID)
			{
				Chunk chunkTo = Core.createNewChunk(Integer.BYTES);
				chunkTo.getData().putInt(0, 0); // 0 edges
				Core.put(chunkTo);
				chunkIDTo = chunkTo.getChunkID();
				p_nodeMapping.setChunkIDForNodeID(p_nodeTo, chunkIDTo);
			}
			
			// source node
			chunkIDFrom = p_nodeMapping.getChunkIDForNodeID(p_nodeFrom);
			Chunk chunkFrom = null;
			if (chunkIDFrom == Chunk.INVALID_CHUNKID)
			{
				chunkFrom = Core.createNewChunk(Integer.BYTES);
				chunkFrom.getData().putInt(0, 0); // 0 edges
				Core.put(chunkFrom);
				chunkIDFrom = chunkFrom.getChunkID();
				p_nodeMapping.setChunkIDForNodeID(p_nodeFrom, chunkIDFrom);
			}
			else
				chunkFrom = Core.get(chunkIDFrom);
			
			// add target node/edge
			{						
				// realloc bigger chunk
				Core.remove(chunkIDFrom);
				Chunk reallocedChunk = Core.createNewChunk(chunkFrom.getData().capacity() + Long.BYTES);
				reallocedChunk.getData().put(chunkFrom.getData());
				reallocedChunk.getData().putLong(chunkFrom.getData().capacity(), chunkIDTo);
				// increase outgoing edges count
				int outgoingEdges = reallocedChunk.getData().getInt(0);
				reallocedChunk.getData().putInt(0, outgoingEdges + 1);
				
				chunkFrom = reallocedChunk;
			}
			
			Core.put(chunkFrom);
		} catch (DXRAMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
	}

}

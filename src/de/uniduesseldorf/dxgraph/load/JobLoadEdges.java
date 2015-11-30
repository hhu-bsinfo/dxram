package de.uniduesseldorf.dxgraph.load;

import java.util.Vector;

import de.uniduesseldorf.dxcompute.ComputeJob;
import de.uniduesseldorf.dxgraph.SimpleVertex;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.utils.Pair;

public class JobLoadEdges extends ComputeJob
{
	private GraphEdgeReader m_edgeReader;
	private NodeMapping m_nodeMapping = null;

	public JobLoadEdges(final GraphEdgeReader p_edgeReader, final NodeMapping p_nodeMapping)
	{
		m_edgeReader = p_edgeReader;
		m_nodeMapping = p_nodeMapping;
	}
	
	@Override
	protected void execute() 
	{
		final int edgeCount = 100;
		Vector<Pair<Long, Long>> buffer = new Vector<Pair<Long, Long>>();
		int readEdges = 0;
		
		do
		{				
			readEdges = m_edgeReader.readEdges(buffer, edgeCount);
			if (readEdges <= 0)
				break;
			
			for (Pair<Long, Long> edge : buffer)
			{
				m_importer.addEdge(edge.first(), edge.second(), m_nodeMapping);
			}
			
			buffer.clear();
		}
		while (readEdges > 0);
		
		System.out.println("Worker (" + m_workerInstance + ") finished.");
	}

	@Override
	public long getJobID() {
		// TODO Auto-generated method stub
		return 0;
	}

	private boolean addEdge(long p_vertexFrom, long p_vertexTo) 
	{
		try 
		{
			long chunkIDFrom = Chunk.INVALID_CHUNKID;
			long chunkIDTo = Chunk.INVALID_CHUNKID;
			
			// target vertex
			chunkIDTo = m_nodeMapping.getChunkIDForNodeID(p_vertexTo);
			// check if target vertex exists and create empty vertex if not
			if (chunkIDTo == Chunk.INVALID_CHUNKID)
			{
				final int vertexSize = SimpleVertex.getSizeWithNeighbours(0);
				byte[] vertexData;
				
				chunkIDTo = getStorageInterface().create(vertexSize);
				vertexData = new byte[vertexSize];
				SimpleVertex.setUserData(vertexData, -1);
				SimpleVertex.setNumberOfNeighbours(vertexData, 0);
	
				getStorageInterface().put(chunkIDTo, vertexData);
				
				m_nodeMapping.setChunkIDForNodeID(p_vertexTo, chunkIDTo);
			}
			
			// source vertex
			chunkIDFrom = m_nodeMapping.getChunkIDForNodeID(p_vertexFrom);
			// only check if exists and put back new chunk with added neighbour
			if (chunkIDFrom == Chunk.INVALID_CHUNKID)
			{
				final int vertexSize = SimpleVertex.getSizeWithNeighbours(1);
				byte[] vertexData;
				
				chunkIDFrom = getStorageInterface().create(vertexSize);
				vertexData = new byte[vertexSize];
				SimpleVertex.setUserData(vertexData, -1);
				SimpleVertex.setNumberOfNeighbours(vertexData, 1);
				SimpleVertex.setNeighbour(vertexData, 0, chunkIDTo);
				
				getStorageInterface().put(chunkIDFrom, vertexData);
				
				m_nodeMapping.setChunkIDForNodeID(p_vertexFrom, chunkIDFrom);
			}
			// vertex exist, get and add neighbour
			else
			{
				getStorageInterface().get(p_handle)
			}
			
			// source node
			chunkIDFrom = m_nodeMapping.getChunkIDForNodeID(p_nodeFrom);
			Chunk chunkFrom = null;
			if (chunkIDFrom == Chunk.INVALID_CHUNKID)
			{
				chunkFrom = Core.createNewChunk(Integer.BYTES);
				chunkFrom.getData().putInt(0, 0); // 0 edges
				Core.put(chunkFrom);
				chunkIDFrom = chunkFrom.getChunkID();
				m_nodeMapping.setChunkIDForNodeID(p_nodeFrom, chunkIDFrom);
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
}

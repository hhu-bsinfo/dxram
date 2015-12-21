package de.uniduesseldorf.dxgraph.load;

import java.util.Vector;

import de.uniduesseldorf.dxcompute.ComputeJob;
import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;
import de.uniduesseldorf.dxgraph.data.SimpleVertex;
import de.uniduesseldorf.dxram.core.data.Chunk;
import de.uniduesseldorf.utils.Pair;

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
				return; // don't spawn further jobs
			
			for (Pair<Long, Long> edge : buffer)
			{
				addEdge(edge.first(), edge.second());
			}
			
			buffer.clear();
		}
		while (readEdges > 0);
		
		getJobInterface().pushJobPublicLocalQueue(new JobLoadEdges(m_edgeReader, m_nodeMapping));
	}

	@Override
	public long getJobID() {
		// TODO
		return 0;
	}

	private boolean addEdge(long p_vertexFrom, long p_vertexTo) 
	{
		long chunkIDFrom = Chunk.INVALID_CHUNKID;
		long chunkIDTo = Chunk.INVALID_CHUNKID;
		
		// target vertex
		chunkIDTo = m_nodeMapping.getChunkIDForNodeID(p_vertexTo);
		// check if target vertex exists and create empty vertex if not
		if (chunkIDTo == Chunk.INVALID_CHUNKID)
		{
			final int vertexSize = SimpleVertex.getSizeWithNeighbours(0);
			SimpleVertex vertex;
			long newVertexID;
			
			newVertexID = getStorageDelegate().create(vertexSize);
			if (newVertexID == -1)
			{
				log(LOG_LEVEL.LL_ERROR, "Creating destination vertex of size " + vertexSize + " failed.");
				return false;
			}
			vertex = new SimpleVertex(newVertexID);
			if (getStorageDelegate().put(vertex) != 1)
			{
				log(LOG_LEVEL.LL_ERROR, "Putting destination vertex " + vertex + " failed.");
				return false;
			}
			m_nodeMapping.setChunkIDForNodeID(p_vertexTo, newVertexID);
		}
		
		// source vertex
		chunkIDFrom = m_nodeMapping.getChunkIDForNodeID(p_vertexFrom);
		// only check if exists and put back new chunk with added neighbour
		if (chunkIDFrom == Chunk.INVALID_CHUNKID)
		{
			final int vertexSize = SimpleVertex.getSizeWithNeighbours(1);
			SimpleVertex vertex;
			long newVertexID;
			
			newVertexID = getStorageDelegate().create(vertexSize);
			if (newVertexID == -1)
			{
				log(LOG_LEVEL.LL_ERROR, "Creating source vertex of size " + vertexSize + " failed.");
				return false;
			}
			vertex = new SimpleVertex(newVertexID);
			vertex.getNeighbours().add(chunkIDTo);

			if (getStorageDelegate().put(vertex) != 1)
			{
				log(LOG_LEVEL.LL_ERROR, "Putting source vertex " + vertex + " failed.");
				return false;
			}
			m_nodeMapping.setChunkIDForNodeID(p_vertexFrom, newVertexID);
		}
		// vertex exist, get and add neighbour
		else
		{
			SimpleVertex vertex;
		
			vertex = new SimpleVertex(chunkIDFrom);
			if (getStorageDelegate().get(vertex) != 1)
			{
				log(LOG_LEVEL.LL_ERROR, "Getting source vertex " + chunkIDFrom + " failed.");
				return false;
			}
			
			vertex.getNeighbours().add(chunkIDTo);
			
			if (getStorageDelegate().put(vertex) != 1)
			{
				log(LOG_LEVEL.LL_ERROR, "Putting source vertex " + vertex + " failed.");
				return false;
			}
		}

		
		return true;
	}
}

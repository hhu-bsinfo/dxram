package de.uniduesseldorf.dxram.test.nothaas.ge;

import java.util.Vector;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

public class DXRAMGraph implements DXRAMGraphAPI
{
	public DXRAMGraph()
	{
		
	}
	
	public void init() throws DXRAMException
	{
		System.out.println("Starting graph peer...");
		Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
				NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
	}
	
	public void shutdown()
	{
		
	}

	@Override
	public long createNode() throws DXRAMException
	{
		return createNode(0xFFFFFFFF);
	}
	
	@Override
	public long createNode(long... p_edges) throws DXRAMException
	{
		return createNode(0xFFFFFFFF, p_edges);
	}
	
	@Override
	public long createNode(int p_userData, long... p_edges) throws DXRAMException
	{
		Chunk chunk = Core.createNewChunk(Integer.BYTES * 2);
		chunk.getData().putInt(0, p_userData); // user data
		chunk.getData().putInt(4, p_edges.length);
		for (int i = 0; i < p_edges.length; i++)
		{
			chunk.getData().putLong(8 + i * Long.BYTES, p_edges[i]);
		}
		
		Core.put(chunk);
		return chunk.getChunkID();
	}
	
	@Override
	public long addEdges(long p_sourceNodeID, long... p_destinationNodeIDs) throws DXRAMException
	{
		Chunk chunk = Core.get(p_sourceNodeID);
		
//		// realloc bigger chunk
//		Core.remove(chunk.getChunkID());
//		Chunk reallocedChunk = Core.createNewChunk(chunkFrom.getData().capacity() + Long.BYTES);
//		reallocedChunk.getData().put(chunkFrom.getData());
//		reallocedChunk.getData().putLong(chunkFrom.getData().capacity(), chunkIDTo);
//		// increase outgoing edges count
//		int outgoingEdges = reallocedChunk.getData().getInt(0);
//		reallocedChunk.getData().putInt(0, outgoingEdges + 1);
//		
//		chunkFrom = reallocedChunk;
		return -1;
	}


	@Override
	public Vector<Long> getEdges(long p_nodeID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean deleteEdge(long p_sourceNodeID, long p_destinationNodeID) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteNode(long p_nodeID) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getUserData(long p_nodeID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setUserData(long p_nodeID, int p_userData) {
		// TODO Auto-generated method stub
		
	}
}

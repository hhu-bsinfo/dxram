package de.hhu.bsinfo.dxgraph.algo.bfs;

import java.util.HashMap;

import de.hhu.bsinfo.dxgraph.algo.bfs.front.FrontierList;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.VerticesForNextFrontierMessage;
import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkService;

public class BFSThreadDist extends Thread {
	
	private int m_id = -1;
	
	private LoggerService m_loggerService = null;
	private ChunkService m_chunkService = null;
	private NetworkService m_networkService = null;
	private short m_nodeId = -1;
	
	private Vertex2[] m_vertexBatch = null;
	private int m_messageBatchSize = -1;
	private HashMap<Short, VerticesForNextFrontierMessage> m_remoteMessages = new HashMap<Short, VerticesForNextFrontierMessage>();
	
	private int m_currentIterationLevel = 0;		
	private FrontierList m_curFrontier = null;
	private FrontierList m_nextFrontier = null;
	
	private volatile int m_visitedCounterRun = 0;
	private volatile boolean m_iterationLevelDone = true;
	private volatile boolean m_exitThread = false;
	
	public BFSThreadDist(
			final int p_id, 
			final LoggerService p_loggerService, 
			final ChunkService p_chunkService,
			final NetworkService p_networkService,
			final short p_nodeId, 
			final int p_vertexBatchSize,
			final int p_vertexMessageBatchSize,
			final FrontierList p_curFrontierShared,
			final FrontierList p_nextFrontierShared)
	{
		super("BFSThread-" + p_id);
		
		m_id = p_id;
		
		m_loggerService = p_loggerService;
		m_chunkService = p_chunkService;
		m_nodeId = p_nodeId;
		
		m_vertexBatch = new Vertex2[p_vertexBatchSize];
		for (int i = 0; i < m_vertexBatch.length; i++) {
			m_vertexBatch[i] = new Vertex2(ChunkID.INVALID_ID);
		}
		m_messageBatchSize = p_vertexMessageBatchSize;
		
		m_curFrontier = p_curFrontierShared;
		m_nextFrontier = p_nextFrontierShared;
	}
	
	public void setCurrentBFSIterationLevel(final int p_iterationLevel) {
		m_currentIterationLevel = p_iterationLevel;
	}
	
	public void triggerNextIteration() {
		m_visitedCounterRun = 0;
		m_iterationLevelDone = false;
	}
	
	public boolean isIterationLevelDone() {
		return m_iterationLevelDone;
	}
	
	public int getVisitedCountLastRun() {
		return m_visitedCounterRun;
	}
	
	public void triggerFrontierSwap() {
		FrontierList tmp = m_curFrontier;
		m_curFrontier = m_nextFrontier;
		m_nextFrontier = tmp;
	}
	
	public void exitThread() {
		m_exitThread = true;
	}
	
	@Override
	public void run() 
	{
		while (true)
		{
			while (m_iterationLevelDone)
			{						
				if (m_exitThread)
					return;
				
				Thread.yield();
			}
			
			int validVertsInBatch = 0;
			for (int i = 0; i < m_vertexBatch.length; i++) {
				long tmp = m_curFrontier.popFront();
				if (tmp != -1)
				{
					m_vertexBatch[i].setID(ChunkID.getChunkID(m_nodeId, tmp));
					validVertsInBatch++;
				}
				else
				{
					if (validVertsInBatch == 0) {
						break;
					}
					m_vertexBatch[i].setID(ChunkID.INVALID_ID);
				}
			}
			
			if (validVertsInBatch == 0) {
				m_iterationLevelDone = true;
				continue;
			}
			
			int gett = m_chunkService.get(m_vertexBatch, 0, validVertsInBatch);
			if (gett != validVertsInBatch)
			{
				m_loggerService.error(getClass(), "Getting vertices in BFS Thread " + m_id + " failed: " + gett + " != " + validVertsInBatch);
				return;
			}
			
			int writeBackCount = 0;
			for (int i = 0; i < validVertsInBatch; i++) {
				// check first if visited
				Vertex2 vertex = m_vertexBatch[i];
				
				// skip vertices that were already marked invalid before
				if (vertex.getID() == ChunkID.INVALID_ID) {
					continue;
				}
				
				if (vertex.getUserData() != m_currentIterationLevel) {							
					writeBackCount++;
					// set depth level
					vertex.setUserData(m_currentIterationLevel);
					m_visitedCounterRun++;
					long[] neighbours = vertex.getNeighbours();
					
					for (long neighbour : neighbours) {	
						// sort by remote and local vertices
						short creatorId = ChunkID.getCreatorID(neighbour);
						if (creatorId != m_nodeId) {
							// go remote, fill message buffers until they are full -> send
							VerticesForNextFrontierMessage msg = m_remoteMessages.get(creatorId);
							if (msg == null) {
								msg = new VerticesForNextFrontierMessage(creatorId, m_messageBatchSize);
							}
							
							if (msg.getNumVerticesInBatch() == msg.getBatchSize()) {
								if (m_networkService.sendMessage(msg) != NetworkErrorCodes.SUCCESS) {
									m_loggerService.error(getClass(), "Sending vertex message to node " + Integer.toHexString(creatorId) + " failed");
									return; 
								}
								
								msg.setNumVerticesInBatch(0);
							}
						} else {
							m_nextFrontier.pushBack(ChunkID.getLocalID(neighbour));
						}
					}
				} else {
					// already visited, don't have to put back to storage
					vertex.setID(ChunkID.INVALID_ID);
				}
			}
			
			// write back changes 
			int put = m_chunkService.put(ChunkLockOperation.NO_LOCK_OPERATION, m_vertexBatch, 0, validVertsInBatch);
			if (put != writeBackCount)
			{
				m_loggerService.error(getClass(), "Putting vertices in BFS Thread " + m_id + " failed: " + put + " != " + writeBackCount);
				return; 
			}
		}
	}
}

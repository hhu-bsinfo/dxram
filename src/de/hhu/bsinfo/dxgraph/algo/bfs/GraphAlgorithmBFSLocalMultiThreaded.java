package de.hhu.bsinfo.dxgraph.algo.bfs;

import de.hhu.bsinfo.dxgraph.algo.bfs.front.BitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BitVectorOptimized;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BulkFifo;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.BulkFifoNaive;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.FrontierList;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.TreeSetFifo;
import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxgraph.load.GraphLoaderResultDelegate;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;

public class GraphAlgorithmBFSLocalMultiThreaded extends GraphAlgorithm implements MessageReceiver {

	private int m_vertexBatchCount = 1000;

	private FrontierList m_curFrontier = null;
	private FrontierList m_nextFrontier = null;
	
	private Vertex2[] m_vertexBatch = null;
	
	private long m_visitedCounter = 0;
	
	public GraphAlgorithmBFSLocalMultiThreaded(final int p_vertexBatchCount, final Class<? extends FrontierList> p_frontierListType, final GraphLoaderResultDelegate p_loaderResultsDelegate, final long... p_entryNodes)
	{
		super(p_loaderResultsDelegate, p_entryNodes);
		m_vertexBatchCount = p_vertexBatchCount;

		if (p_frontierListType == BitVector.class)
		{
			long totalVertexCount = getGraphLoaderResultDelegate().getTotalVertexCount();
			m_curFrontier = new BitVector(totalVertexCount);
			m_nextFrontier = new BitVector(totalVertexCount);
		}
		else if (p_frontierListType == BitVectorOptimized.class)
		{
			long totalVertexCount = getGraphLoaderResultDelegate().getTotalVertexCount();
			m_curFrontier = new BitVectorOptimized(totalVertexCount);
			m_nextFrontier = new BitVectorOptimized(totalVertexCount);
		}
		else if (p_frontierListType == BulkFifoNaive.class)
		{
			m_curFrontier = new BulkFifoNaive();
			m_nextFrontier = new BulkFifoNaive();
		}
		else if (p_frontierListType == BulkFifo.class)
		{
			m_curFrontier = new BulkFifo();
			m_nextFrontier = new BulkFifo();
		}
		else if (p_frontierListType == TreeSetFifo.class)
		{
			m_curFrontier = new TreeSetFifo();
			m_nextFrontier = new TreeSetFifo();
		}
		else
		{
			throw new RuntimeException("Cannot create instance of FrontierList type " + p_frontierListType);
		}
		
		// pool init
		m_vertexBatch = new Vertex2[m_vertexBatchCount];
		for (int i = 0; i < m_vertexBatch.length; i++) {
			m_vertexBatch[i] = new Vertex2(ChunkID.INVALID_ID);
		}
	}
	
	@Override
	protected boolean execute(long[] p_entryNodes) 
	{
		// execute a full BFS for each node
		for (long entryNode : p_entryNodes)
		{
			long iterationDepth = runBFS(entryNode);
			
		}
		
		
		

		m_loggerService.info(getClass(), "Finished BFS.");
		return true;
	}
	
	private long runBFS(long p_entryNode)
	{
		// TODO for entry node, check if local, otherwise 
		// push to remote node 
		
		// initial root set
//		for (int i = 0; i < p_parameterChunkIDs.length; i++) {
//			m_curFrontier.pushBack(ChunkID.getLocalID(p_parameterChunkIDs[i]));
//		}
		
		return 0;
	}
	
	private void runBFSJob()
	{
		int curIterationLevel = 0;
		long previousRunParentId = ChunkID.INVALID_ID;
		short nodeId = m_bootService.getNodeID();
		
		while (true)
		{			
			boolean iterationLevelDone = false;
			while (!iterationLevelDone)
			{
				int curBatchSize = 0;
				for (int i = 0; i < m_vertexBatchCount; i++) {
					long tmp = m_curFrontier.popFront();
					if (tmp != -1)
					{
						// don't walk back
						if (tmp != previousRunParentId) {
							m_vertexBatch[i].setID(ChunkID.getChunkID(nodeId, tmp));
							curBatchSize++;
						}
					}
					else
					{
						iterationLevelDone = true;
					}
				}
			
				int gett = m_chunkService.get(m_vertexBatch, 0, curBatchSize);
				if (gett != curBatchSize)
				{
					m_loggerService.error(getClass(), "Getting vertices failed.");
					return; // TODO proper error handling
				}
				
				int writeBackCount = 0;
				for (int i = 0; i < gett; i++) {
					// check first if visited
					if (m_vertexBatch[i].getUserData() == -1) {							
						writeBackCount++;
						// set depth level
						m_vertexBatch[i].setUserData(curIterationLevel);
						m_visitedCounter++;
						long[] neighbours = m_vertexBatch[i].getNeighbours();
						
						for (long neighbour : neighbours) {								
							// don't walk back
							if (neighbour != previousRunParentId) {									
								
								// TODO check if vertex is on current node as we can only process nodes
								// which are stored locally
								// -> send remote nodes to their owner and have it processed there
								
								m_nextFrontier.pushBack(ChunkID.getLocalID(neighbour));
							}
						}
					} else {
						// already visited, don't have to put back to storage
						m_vertexBatch[i].setID(ChunkID.INVALID_ID);
					}
				}
				
				// write back changes 
				int put = m_chunkService.put(ChunkLockOperation.NO_LOCK_OPERATION, m_vertexBatch, 0, gett);
				if (put != writeBackCount)
				{
					m_loggerService.error(getClass(), "Putting vertices failed.");
					return; // TODO proper error handling
				}
			}
			
			// check if we got anything to process in next
			// TODO: later have work stealing from other threads
			if (m_nextFrontier.isEmpty())
			{
				m_loggerService.info(getClass(), "Thread finished.");
				break;
			}
			
			// swap buffers at the end of the round when iteration level done
			FrontierList tmp = m_curFrontier;
			m_curFrontier = m_nextFrontier;
			m_nextFrontier = tmp;
			m_nextFrontier.reset();
			curIterationLevel++;
		}
		
		m_loggerService.info(getClass(), "Iteration depth " + curIterationLevel + ", total visited vertices " + m_visitedCounter);
	
	}

	@Override
	public void onIncomingMessage(AbstractMessage p_message) {
		// TODO Auto-generated method stub
		
	}
}

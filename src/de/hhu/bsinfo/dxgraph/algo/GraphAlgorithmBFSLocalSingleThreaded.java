package de.hhu.bsinfo.dxgraph.algo;

import de.hhu.bsinfo.dxgraph.algo.bfs.BitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.BitVectorOptimized;
import de.hhu.bsinfo.dxgraph.algo.bfs.BulkFifo;
import de.hhu.bsinfo.dxgraph.algo.bfs.BulkFifoNaive;
import de.hhu.bsinfo.dxgraph.algo.bfs.FrontierList;
import de.hhu.bsinfo.dxgraph.algo.bfs.TreeSetFifo;
import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxgraph.load.GraphLoaderResultDelegate;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public class GraphAlgorithmBFSLocalSingleThreaded extends GraphAlgorithm {

	private int m_batchCountPerJob = 1;
	private Class<? extends FrontierList> m_frontierListType = null;
	
	public GraphAlgorithmBFSLocalSingleThreaded(final int p_batchCountPerJob, final Class<? extends FrontierList> p_frontierListType, final GraphLoaderResultDelegate p_loaderResultsDelegate, final long... p_entryNodes)
	{
		super(p_loaderResultsDelegate, p_entryNodes);
		m_batchCountPerJob = p_batchCountPerJob;
		m_frontierListType = p_frontierListType;
	}
	
	@Override
	protected boolean execute(long[] p_entryNodes) 
	{
		BFSThread thread = new BFSThread(
				0, 
				m_loggerService, 
				m_chunkService, 
				m_batchCountPerJob,
				m_bootService.getNodeID(),
				getGraphLoaderResultDelegate().getTotalVertexCount(), 
				m_frontierListType,
				p_entryNodes[0]);
		
		m_loggerService.info(getClass(), "Starting BFS with 1 thread.");
		
		thread.start();
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {
		}
		
		try {
			thread.join();
		} catch (InterruptedException e) {
		}
		
		m_loggerService.info(getClass(), "Finished BFS.");
		return true;
	}

	private static class BFSThread extends Thread {
		
		private int m_id = -1;
		
		private LoggerService m_loggerService = null;
		private ChunkService m_chunkService = null;
		
		private int m_vertexBatchCount = 1;
		private short m_nodeId = -1;
		private FrontierList m_curFrontier = null;
		private FrontierList m_nextFrontier = null;
		
		private Vertex2[] m_vertexBatch = null;
		private long m_previousRunParentId = ChunkID.INVALID_ID;
		
		private long m_visitedCounter = 0;
		
		public BFSThread(
				final int p_id, 
				final LoggerService p_loggerService, 
				final ChunkService p_chunkService, 
				final int p_vertexBatchCount, 
				final short p_nodeId, 
				final long p_totalVertexCount,
				final Class<? extends FrontierList> p_class,
				final long... p_parameterChunkIDs)
		{
			super("BFSThread-" + p_id);
			
			m_id = p_id;
			
			m_loggerService = p_loggerService;
			m_chunkService = p_chunkService;
			
			m_vertexBatchCount = p_vertexBatchCount;
			m_nodeId = p_nodeId;
			
			if (p_class == BitVector.class)
			{
				m_curFrontier = new BitVector(p_totalVertexCount);
				m_nextFrontier = new BitVector(p_totalVertexCount);
			}
			else if (p_class == BitVectorOptimized.class)
			{
				m_curFrontier = new BitVectorOptimized(p_totalVertexCount);
				m_nextFrontier = new BitVectorOptimized(p_totalVertexCount);
			}
			else if (p_class == BulkFifoNaive.class)
			{
				m_curFrontier = new BulkFifoNaive();
				m_nextFrontier = new BulkFifoNaive();
			}
			else if (p_class == BulkFifo.class)
			{
				m_curFrontier = new BulkFifo();
				m_nextFrontier = new BulkFifo();
			}
			else if (p_class == TreeSetFifo.class)
			{
				m_curFrontier = new TreeSetFifo();
				m_nextFrontier = new TreeSetFifo();
			}
			else
			{
				throw new RuntimeException("Cannot create instance of FrontierList type " + p_class);
			}
			
			// pool init
			m_vertexBatch = new Vertex2[m_vertexBatchCount];
			for (int i = 0; i < m_vertexBatch.length; i++) {
				m_vertexBatch[i] = new Vertex2(ChunkID.INVALID_ID);
			}
			
			// initial root set
			for (int i = 0; i < p_parameterChunkIDs.length; i++) {
				m_curFrontier.pushBack(ChunkID.getLocalID(p_parameterChunkIDs[i]));
			}
		}
		
		@Override
		public void run() 
		{
			int curIterationLevel = 0;
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
							if (tmp != m_previousRunParentId) {
								m_vertexBatch[i].setID(ChunkID.getChunkID(m_nodeId, tmp));
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
								if (neighbour != m_previousRunParentId) {									
									
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
					m_loggerService.info(getClass(), "Thread " + m_id + " finished.");
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
	}
}

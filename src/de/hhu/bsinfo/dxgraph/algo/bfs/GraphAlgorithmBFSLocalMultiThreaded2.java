package de.hhu.bsinfo.dxgraph.algo.bfs;

import de.hhu.bsinfo.dxgraph.algo.bfs.front.ConcurrentBitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.FrontierList;
import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxgraph.load.GraphLoaderResultDelegate;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.utils.Pair;

public class GraphAlgorithmBFSLocalMultiThreaded2 extends GraphAlgorithm {

	private int m_vertexBatchCountPerThread = 100;

	private FrontierList m_curFrontier = null;
	private FrontierList m_nextFrontier = null;
	
	private BFSThread[] m_threads = null;
	
	public GraphAlgorithmBFSLocalMultiThreaded2(final int p_vertexBatchCountPerThread, final int p_threadCount, final GraphLoaderResultDelegate p_loaderResultsDelegate, final long... p_entryNodes)
	{
		super(p_loaderResultsDelegate, p_entryNodes);
		m_vertexBatchCountPerThread = p_vertexBatchCountPerThread;		
		m_threads = new BFSThread[p_threadCount];
	}
	
	@Override
	protected boolean execute(long[] p_entryNodes) 
	{
		m_loggerService.info(getClass(), "Starting " + m_threads.length + " BFS Threads...");
		
		long totalVertexCount = getGraphLoaderResultDelegate().getTotalVertexCount();
		m_curFrontier = new ConcurrentBitVector(totalVertexCount);
		m_nextFrontier = new ConcurrentBitVector(totalVertexCount);
		
		for (int i = 0; i < m_threads.length; i++) {
			m_threads[i] = new BFSThread(i, m_loggerService, m_chunkService, m_bootService.getNodeID(), m_vertexBatchCountPerThread, m_curFrontier, m_nextFrontier);
			m_threads[i].start();
		}
		
		// execute a full BFS for each node
		int bfsIteration = 0;
		for (long entryNode : p_entryNodes)
		{
			m_loggerService.info(getClass(), "Executing BFS iteration (" + bfsIteration + ") with root node " + Long.toHexString(entryNode));
			m_curFrontier.pushBack(ChunkID.getLocalID(entryNode));
			Pair<Long, Integer> results = runBFS(bfsIteration);
			m_loggerService.info(getClass(), "Results BFS iteration (" + bfsIteration + ") for root node " + Long.toHexString(entryNode) + ": Iteration depth " + results.second() + ", total visited vertices " + results.first());
		
			bfsIteration++;
		}
		
		for (int i = 0; i < m_threads.length; i++) {
			m_threads[i].exitThread();
		}
		
		m_loggerService.info(getClass(), "Joining BFS threads...");
		for (int i = 0; i < m_threads.length; i++) {
			try {
				m_threads[i].join();
			} catch (InterruptedException e) {
			}
		}

		m_loggerService.info(getClass(), "Finished BFS.");
		return true;
	}
	
	private static class BFSThread extends Thread {
		
		private int m_id = -1;
		
		private LoggerService m_loggerService = null;
		private ChunkService m_chunkService = null;
		private short m_nodeId = -1;
		
		private Vertex2[] m_vertexBatch = null;
		
		private int m_currentIterationLevel = 0;		
		private FrontierList m_curFrontier = null;
		private FrontierList m_nextFrontier = null;
		
		private volatile int m_visitedCounterRun = 0;
		private volatile boolean m_iterationLevelDone = true;
		private volatile boolean m_exitThread = false;
		
		public BFSThread(
				final int p_id, 
				final LoggerService p_loggerService, 
				final ChunkService p_chunkService,
				final short p_nodeId, 
				final int p_vertexBatchSize,
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
			
			m_curFrontier = p_curFrontierShared;
			m_nextFrontier = p_nextFrontierShared;
		}
		
		public void triggerNextIteration() {
			m_iterationLevelDone = false;
		}
		
		public boolean isIterationLevelDone() {
			return m_iterationLevelDone;
		}
		
		public int getVisitedCountLastRun() {
			return m_visitedCounterRun;
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
						m_iterationLevelDone = true;
					}
				}
				
				if (validVertsInBatch == 0) {
					continue;
				}
				
				m_visitedCounterRun = 0;
				
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
							m_nextFrontier.pushBack(ChunkID.getLocalID(neighbour));
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

	
	private Pair<Long, Integer> runBFS(final int p_bfsIteration)
	{
		long visitedCounter = 0;
		int curIterationLevel = 0;
		
		while (true)
		{			
			m_loggerService.debug(getClass(), "Iteration level " + curIterationLevel + " cur frontier size: " + m_curFrontier.size());
			
			boolean iterationLevelDone = false;
			while (!iterationLevelDone)
			{
				// kick off threads with current frontier
				for (int t = 0; t < m_threads.length; t++) {	
					m_threads[t].triggerNextIteration();
				}
				
				// join forked threads
				int i = 0;
				long tmpVisitedCounter = 0;
				while (i < m_threads.length) {
					if (!m_threads[i].isIterationLevelDone()) {
						Thread.yield();
						continue;
					}
					tmpVisitedCounter += m_threads[i].getVisitedCountLastRun();
					i++;
				}
				
				visitedCounter += tmpVisitedCounter;
			}
			
			m_loggerService.debug(getClass(), "Finished iteration of level " + curIterationLevel);
			
			// swap buffers at the end of the round when iteration level done
			FrontierList tmp = m_curFrontier;
			m_curFrontier = m_nextFrontier;
			m_nextFrontier = tmp;
			m_nextFrontier.reset();
			
			if (m_curFrontier.isEmpty())
			{
				break;
			}
			
			curIterationLevel++;
		}
		
		return new Pair<Long, Integer>(visitedCounter, curIterationLevel);
	}
}

package de.hhu.bsinfo.dxgraph.algo.bfs;

import de.hhu.bsinfo.dxgraph.algo.bfs.front.FrontierList;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.HalfConcurrentBitVector;
import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxgraph.load.GraphLoaderResultDelegate;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.utils.Pair;

public class GraphAlgorithmBFSLocalMultiThreaded extends GraphAlgorithm {

	private int m_vertexBatchCountPerThread = 100;

	private FrontierList m_curFrontier = null;
	private FrontierList m_nextFrontier = null;
	
	private Vertex2[] m_vertexBatch = null;
	private BFSThread[] m_threads = null;
	
	public GraphAlgorithmBFSLocalMultiThreaded(final int p_vertexBatchCountPerThread, final int p_threadCount, final GraphLoaderResultDelegate p_loaderResultsDelegate, final long... p_entryNodes)
	{
		super(p_loaderResultsDelegate, p_entryNodes);
		m_vertexBatchCountPerThread = p_vertexBatchCountPerThread;
		
		m_vertexBatch = new Vertex2[m_vertexBatchCountPerThread * p_threadCount];
		for (int i = 0; i < m_vertexBatch.length; i++) {
			m_vertexBatch[i] = new Vertex2(ChunkID.INVALID_ID);
		}
		
		m_threads = new BFSThread[p_threadCount];
	}
	
	@Override
	protected boolean execute(long[] p_entryNodes) 
	{
		m_loggerService.info(getClass(), "Starting " + m_threads.length + " BFS Threads...");
		
		long totalVertexCount = getGraphLoaderResultDelegate().getTotalVertexCount();
		m_curFrontier = new HalfConcurrentBitVector(totalVertexCount);
		m_nextFrontier = new HalfConcurrentBitVector(totalVertexCount);
		
		for (int i = 0; i < m_threads.length; i++) {
			m_threads[i] = new BFSThread(i, m_loggerService, m_chunkService);
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
		
		private Vertex2[] m_vertexBatch = null;
		private int m_vertexBatchStartOffset = 0;
		private int m_currentBatchSize = 0;
		private int m_currentActualBatchSize = 0;
		
		private long m_previousRunParentId = ChunkID.INVALID_ID;
		private int m_currentIterationLevel = 0;
		private FrontierList m_nextFrontier = null;
		
		private volatile int m_visitedCounterRun = 0;
		private volatile boolean m_oneRun = false;
		private volatile boolean m_exitThread = false;
		
		public BFSThread(
				final int p_id, 
				final LoggerService p_loggerService, 
				final ChunkService p_chunkService)
		{
			super("BFSThread-" + p_id);
			
			m_id = p_id;
			
			m_loggerService = p_loggerService;
			m_chunkService = p_chunkService;
		}
		
		public void setVertexBatch(final Vertex2[] p_array, final int p_startOffset, final int p_size, final int p_actualBatchSize) {
			m_vertexBatch = p_array;
			m_vertexBatchStartOffset = p_startOffset;
			m_currentBatchSize = p_size;
			m_currentActualBatchSize = p_actualBatchSize;
		}
		
		public void setPreviousRunParentId(final long p_parentId) {
			m_previousRunParentId = p_parentId;
		}
		
		public void setCurrentIterationLevel(final int p_iterationLevel) {
			m_currentIterationLevel = p_iterationLevel;
		}
		
		public void setNextFrontierShared(final FrontierList p_nextFrontier) {
			m_nextFrontier = p_nextFrontier;
		}
		
		public void executeOneRun() {
			m_oneRun = true;
		}
		
		public boolean isRunDone() {
			return !m_oneRun;
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
				while (!m_oneRun)
				{
					if (m_exitThread)
						return;
					
					Thread.yield();
				}
				m_visitedCounterRun = 0;
				
				int gett = m_chunkService.get(m_vertexBatch, m_vertexBatchStartOffset, m_currentBatchSize);
				if (gett != m_currentActualBatchSize)
				{
					m_loggerService.error(getClass(), "Getting vertices in BFS Thread " + m_id + " failed: " + gett + " != " + m_currentActualBatchSize);
					return;
				}
				
				int writeBackCount = 0;
				for (int i = 0; i < m_currentBatchSize; i++) {
					// check first if visited
					Vertex2 vertex = m_vertexBatch[i + m_vertexBatchStartOffset];
					
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
							// don't walk back
							if (neighbour != m_previousRunParentId) {									
								m_nextFrontier.pushBack(ChunkID.getLocalID(neighbour));
							}
						}
					} else {
						// already visited, don't have to put back to storage
						vertex.setID(ChunkID.INVALID_ID);
					}
				}
				
				// write back changes 
				int put = m_chunkService.put(ChunkLockOperation.NO_LOCK_OPERATION, m_vertexBatch, m_vertexBatchStartOffset, m_currentBatchSize);
				if (put != writeBackCount)
				{
					m_loggerService.error(getClass(), "Putting vertices in BFS Thread " + m_id + " failed: " + put + " != " + writeBackCount);
					return; 
				}
				
				m_oneRun = false;
			}
		}
	}

	
	private Pair<Long, Integer> runBFS(final int p_bfsIteration)
	{
		long visitedCounter = 0;
		int curIterationLevel = 0;
		long previousRunParentId = ChunkID.INVALID_ID;
		short nodeId = m_bootService.getNodeID();
		
		while (true)
		{			
			m_loggerService.debug(getClass(), "Iteration level " + curIterationLevel + " cur frontier size: " + m_curFrontier.size());
			
			boolean iterationLevelDone = false;
			while (!iterationLevelDone)
			{
				long curItVertexCount = m_curFrontier.size();
				int perThreadCount = 0;
				if (curItVertexCount - m_vertexBatchCountPerThread * m_threads.length >= 0) {
					perThreadCount = m_vertexBatchCountPerThread;
				} else {
					// if we have less vertices available to fill up the threads
					// make sure to equally distribute them
					perThreadCount = (int) (curItVertexCount / m_threads.length + 1);
				}
		
				int threadBatchStartOffset = 0;
				for (int t = 0; t < m_threads.length; t++) {				
					int validVertsInBatch = 0;
					for (int i = 0; i < perThreadCount; i++) {
						long tmp = m_curFrontier.popFront();
						if (tmp != -1)
						{
							// don't walk back
							if (tmp != previousRunParentId) {
								m_vertexBatch[i + threadBatchStartOffset].setID(ChunkID.getChunkID(nodeId, tmp));
								validVertsInBatch++;
							} else {
								m_vertexBatch[i + threadBatchStartOffset].setID(ChunkID.INVALID_ID);
							}
						}
						else
						{
							iterationLevelDone = true;
							// mark remaining vertices invalid
							m_vertexBatch[i + threadBatchStartOffset].setID(ChunkID.INVALID_ID);
						}
					}
					
					// don't waste a thread on non valid vertices
					if (validVertsInBatch > 0)
					{
						m_threads[t].setVertexBatch(m_vertexBatch, threadBatchStartOffset, perThreadCount, validVertsInBatch);
						m_threads[t].setPreviousRunParentId(previousRunParentId);
						m_threads[t].setCurrentIterationLevel(p_bfsIteration);
						m_threads[t].setNextFrontierShared(m_nextFrontier);
						m_threads[t].executeOneRun();
					}
					
					threadBatchStartOffset += perThreadCount;
					
					if (iterationLevelDone) {
						break;
					}
				}
				
				// join forked threads
				int i = 0;
				long tmpVisitedCounter = 0;
				while (i < m_threads.length) {
					if (!m_threads[i].isRunDone()) {
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

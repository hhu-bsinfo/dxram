package de.hhu.bsinfo.dxgraph.algo;

import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public class GraphAlgorithmBFS4 extends GraphAlgorithm {

	private int m_batchCountPerJob = 1;
	
	public GraphAlgorithmBFS4(final int p_batchCountPerJob, final long... p_entryNodes)
	{
		super(p_entryNodes);
		m_batchCountPerJob = p_batchCountPerJob;
	}
	
	@Override
	protected boolean execute(long[] p_entryNodes) 
	{
		BFSThread[] threads = new BFSThread[p_entryNodes.length];
		
		for (int i = 0; i < threads.length; i++) {
			// TODO hardcoded vertex count -> we need a pass through from the loading phase (vertex count, edge count?)
			threads[i] = new BFSThread(i, m_loggerService, m_chunkService, m_batchCountPerJob, 244631, m_bootService.getNodeID(), p_entryNodes[i]);
		}
		
		m_loggerService.info(getClass(), "Starting BFS with " + threads.length + " threads.");
		
		for (BFSThread thread : threads) {
			thread.start();
		}
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {
		}
		
		for (BFSThread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
			}
		}
		
		m_loggerService.info(getClass(), "Finished BFS.");
		return true;
	}
	
	private static class GraphBitVector
	{
		private long[] m_vector = null;		
		
		private int m_itPos = 0;
		private long m_itBit = 0;
		
		private long m_count = 0;
		
		public GraphBitVector(final long p_vertexCount)
		{
			m_vector = new long[(int) ((p_vertexCount / 64L) + 1L)];
		}
		
		public void set(final long p_index)
		{
			long tmp = (1L << (p_index % 64L));
			if ((m_vector[(int) (p_index / 64L)] & tmp) == 0)
			{				
				m_count++;
				m_vector[(int) (p_index / 64L)] |= tmp;
			}
		}
		
		public boolean isEmpty()
		{
			return m_count == 0;
		}
		
		public void resetIterator()
		{
			m_itPos = 0;
			m_itBit = 0;
			m_count = 0;
			for (int i = 0; i < m_vector.length; i++) {
				m_vector[i] = 0;
			}
		}
		
		public long getNextIndexSet()
		{
			while (m_count > 0)
			{
				if (m_vector[m_itPos] != 0)
				{
					while (m_itBit < 64L)
					{
						if (((m_vector[m_itPos] >> m_itBit) & 1L) != 0)
						{
							m_count--;
							return m_itPos * 64L + m_itBit++;
						}
						
						m_itBit++;
					}
					
					m_itBit = 0;
				}
				
				m_itPos++;
			}
			
			return -1;
		}
	}

	// TODO have proper base class within a
	// worker system -> simplified version of job system
	// with fixed amount of threads.
	private static class BFSThread extends Thread {
		
		private int m_id = -1;
		
		private LoggerService m_loggerService = null;
		private ChunkService m_chunkService = null;
		
		private int m_vertexBatchCount = 1;
		private short m_nodeId = -1;
		private GraphBitVector m_curFrontier = null;
		private GraphBitVector m_nextFrontier = null;
		
		private Vertex2[] m_vertexBatch = null;
		private long m_previousRunParentId = ChunkID.INVALID_ID;
		
		private long m_visitedCounter = 0;
		
		public BFSThread(final int p_id, final LoggerService p_loggerService, final ChunkService p_chunkService, 
				final int p_vertexBatchCount, final long p_totalVertexCount, final short p_nodeId, final long... p_parameterChunkIDs)
		{
			super("BFSThread-" + p_id);
			
			m_id = p_id;
			
			m_loggerService = p_loggerService;
			m_chunkService = p_chunkService;
			
			m_vertexBatchCount = p_vertexBatchCount;
			m_nodeId = p_nodeId;
			
			m_curFrontier = new GraphBitVector(p_totalVertexCount);
			m_nextFrontier = new GraphBitVector(p_totalVertexCount);
			
			// pool init
			m_vertexBatch = new Vertex2[m_vertexBatchCount];
			for (int i = 0; i < m_vertexBatch.length; i++) {
				m_vertexBatch[i] = new Vertex2(ChunkID.INVALID_ID);
			}
			
			// initial root set
			for (int i = 0; i < p_parameterChunkIDs.length; i++) {
				m_curFrontier.set(ChunkID.getLocalID(p_parameterChunkIDs[i]));
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
						long tmp = m_curFrontier.getNextIndexSet();
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
							// TODO set depth level?
							m_vertexBatch[i].setUserData(curIterationLevel);
							m_visitedCounter++;
							long[] neighbours = m_vertexBatch[i].getNeighbours();
							
							for (long neighbour : neighbours) {								
								// don't walk back
								if (neighbour != m_previousRunParentId) {									
									
									// TODO check if vertex is on current node as we can only process nodes
									// which are stored locally
									// -> send remote nodes to their owner and have it processed there
									
									m_nextFrontier.set(ChunkID.getLocalID(neighbour));
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
				GraphBitVector tmp = m_curFrontier;
				m_curFrontier = m_nextFrontier;
				m_nextFrontier = tmp;
				m_nextFrontier.resetIterator();
				curIterationLevel++;
			}
			
			m_loggerService.info(getClass(), "Iteration depth " + curIterationLevel + ", total visited vertices " + m_visitedCounter);
		}
	}
}

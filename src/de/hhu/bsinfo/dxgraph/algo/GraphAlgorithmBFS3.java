package de.hhu.bsinfo.dxgraph.algo;

import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public class GraphAlgorithmBFS3 extends GraphAlgorithm {

	private int m_batchCountPerJob = 1;
	
	
	public GraphAlgorithmBFS3(final int p_batchCountPerJob, final long... p_entryNodes)
	{
		super(p_entryNodes);
		m_batchCountPerJob = p_batchCountPerJob;
	}
	
	@Override
	protected boolean execute(long[] p_entryNodes) 
	{
		BFSThread[] threads = new BFSThread[p_entryNodes.length];
		
		for (int i = 0; i < threads.length; i++) {
			threads[i] = new BFSThread(i, m_loggerService, m_chunkService, m_batchCountPerJob, p_entryNodes[i]);
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
	
	private static class BulkFifo
	{
		private int m_bulkSize = 16 * 1024 * 1024 / Long.BYTES;
		// TODO dynamically grow the blocks also in grow section
		private long[][] m_chainedFifo = new long[10000][];
		
		private int m_posBack = 0;
		private int m_blockBack = 0;
		
		private int m_posFront = 0;
		private int m_blockFront = 0;
		
		public static class EmptyException extends Exception
		{
			private static final long serialVersionUID = -2491644239410657262L;
		}
		
		public BulkFifo()
		{
			m_chainedFifo[0] = new long[m_bulkSize];
		}
		
		public BulkFifo(final int p_bulkSize)
		{
			m_bulkSize = p_bulkSize;
			m_chainedFifo[0] = new long[m_bulkSize];
		}
		
		public int size()
		{
			if (m_blockFront == m_blockBack) {
				return m_posBack - m_posFront;
			} else {
				int size = 0;
				size += m_bulkSize - m_posFront;
				size += m_bulkSize * (m_blockBack - m_blockFront + 1);
				size += m_posBack;
				return size;
			}
		}
		
		public void reset()
		{
			m_posBack = 0;
			m_blockBack = 0;
			
			m_posFront = 0;
			m_blockFront = 0;
		}
		
		// TODO have optimized version for bulk push back using memcopy as well
		public void pushBack(final long p_val)
		{
			if (m_posBack == m_bulkSize) {
				// grow back
				m_chainedFifo[++m_blockBack] = new long[m_bulkSize];
				m_posBack = 0;
			}
			
			if (m_posBack < m_bulkSize)
				m_chainedFifo[m_blockBack][m_posBack++] = p_val;
		}
		
		public boolean isEmpty()
		{
			return m_blockBack == m_blockFront && m_posBack == m_posFront;
		}
		
		public long popFront() throws EmptyException
		{
			if (m_blockBack == m_blockFront && m_posBack == m_posFront)
				throw new EmptyException();
			
			long tmp = m_chainedFifo[m_blockFront][m_posFront++];
			// go to next block, jump if necessary
			if (m_posFront == m_bulkSize)
			{
				// TODO auto shrink doesn't work with reset...new idea?
//				// auto shrink
//				m_chainedFifo[m_blockFront] = null;
				
				m_blockFront++;
				m_posFront = 0;
			}
			
			return tmp;
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
		private BulkFifo m_curFrontier = new BulkFifo();
		private BulkFifo m_nextFrontier = new BulkFifo();
		
		private Vertex2[] m_vertexBatch = null;
		private long m_previousRunParentId = ChunkID.INVALID_ID;
		
		public BFSThread(final int p_id, final LoggerService p_loggerService, final ChunkService p_chunkService, final int p_vertexBatchCount, final long... p_parameterChunkIDs)
		{
			super("BFSThread-" + p_id);
			
			m_id = p_id;
			
			m_loggerService = p_loggerService;
			m_chunkService = p_chunkService;
			
			m_vertexBatchCount = p_vertexBatchCount;
			
			// pool init
			m_vertexBatch = new Vertex2[m_vertexBatchCount];
			for (int i = 0; i < m_vertexBatch.length; i++) {
				m_vertexBatch[i] = new Vertex2(ChunkID.INVALID_ID);
			}
			
			// initial root set
			for (int i = 0; i < p_parameterChunkIDs.length; i++) {
				m_curFrontier.pushBack(p_parameterChunkIDs[i]);
			}
		}
		
		@Override
		public void run() 
		{
			while (true)
			{				
				boolean iterationLevelDone = false;
				while (!iterationLevelDone)
				{
					int curBatchSize = 0;
					for (int i = 0; i < m_vertexBatchCount; i++) {
						try {
							long tmp = m_curFrontier.popFront();
							// don't walk back
							if (tmp != m_previousRunParentId) {
								m_vertexBatch[i].setID(tmp);
							}
						} catch (de.hhu.bsinfo.dxgraph.algo.GraphAlgorithmBFS3.BulkFifo.EmptyException e) {
							iterationLevelDone = true;
							break;
						}

						curBatchSize++;
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
							long[] neighbours = m_vertexBatch[i].getNeighbours();
							
							for (long neighbour : neighbours) {
								// don't walk back
								if (neighbour != m_previousRunParentId) {
									m_nextFrontier.pushBack(neighbour);
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
				BulkFifo tmp = m_curFrontier;
				m_curFrontier = m_nextFrontier;
				m_nextFrontier = tmp;
				m_nextFrontier.reset();
			}
		}
	}
}

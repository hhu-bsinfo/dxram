package de.hhu.bsinfo.dxgraph.algo;

import java.util.ArrayList;
import java.util.List;

import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxgraph.data.Counter;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.lock.LockService;
import de.hhu.bsinfo.dxram.lock.LockService.ErrorCode;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public class GraphAlgorithmBFSLocked extends GraphAlgorithm {

	private int m_batchCountPerJob = 1;
	
	public GraphAlgorithmBFSLocked(final int p_batchCountPerJob, final long... p_entryNodes)
	{
		super(p_entryNodes);
		m_batchCountPerJob = p_batchCountPerJob;
	}
	
	@Override
	protected boolean execute(long[] p_entryNodes) 
	{
		// create chunk for counting visited nodes
		Counter vertexCounter = new Counter();
		if (m_chunkService.create(vertexCounter) != 1)
		{
			m_loggerService.error(getClass(), "Creating chunk for counting visited vertices failed.");
			return false;
		}
		
		if (m_chunkService.put(vertexCounter) != 1)
		{
			m_loggerService.error(getClass(), "Putting chunk for counting visited vertices failed.");
			return false;
		}
		
		int counter = 0;
		while (counter < p_entryNodes.length)
		{
			long[] tmp = null;
			if (counter + m_batchCountPerJob < p_entryNodes.length)	{
				tmp = new long[m_batchCountPerJob];
				for (int i = 0; i < m_batchCountPerJob; i++) {
					tmp[i] = p_entryNodes[counter + i];
				}
				
				counter += m_batchCountPerJob;
			} else {
				int remainder = p_entryNodes.length - counter;
				tmp = new long[remainder];
				for (int i = 0; i < remainder; i++) {
					tmp[i] = p_entryNodes[counter + i];
				}
				
				counter += remainder;
			}
			
			for (long entry : tmp) {	
				m_loggerService.info(getClass(), "Starting BFS with entry node " + Long.toHexString(entry));
			}
			m_jobService.pushJob(new JobBFS(m_batchCountPerJob, vertexCounter.getID(), tmp));
		}
		
		boolean ret = m_jobService.waitForLocalJobsToFinish();
		m_loggerService.info(getClass(), "Finished BFS.");
		
		if (m_chunkService.get(vertexCounter) != 1)
		{
			m_loggerService.error(getClass(), "Get chunk for counting visited vertices failed.");
			return false;
		}
		
		m_loggerService.info(getClass(), "Total visited vertices: " + vertexCounter.getCounter());
		
		return ret;
	}

	private static class JobBFS extends Job {

		public static final short MS_TYPE_ID = 1;
		static {
			registerType(MS_TYPE_ID, JobBFS.class);
		}
		
		private int m_vertexBatchCount = 1;
		private long m_chunkIDVisitedCount = -1;
		
		public JobBFS(final int p_vertexBatchCount, final long p_chunkIDVisitedCount, final long... p_parameterChunkIDs)
		{
			super(p_parameterChunkIDs);
			m_vertexBatchCount = p_vertexBatchCount;
			m_chunkIDVisitedCount = p_chunkIDVisitedCount;
		}

		@Override
		public short getTypeID() {
			return MS_TYPE_ID;
		}

		@Override
		protected void execute(short p_nodeID, long[] p_chunkIDs) {
			ChunkService chunkService = getService(ChunkService.class);
			JobService jobService = getService(JobService.class);
			LoggerService loggerService = getService(LoggerService.class);
			LockService lockService = getService(LockService.class);
			
			Vertex[] entryVertices = new Vertex[p_chunkIDs.length];
			for (int i = 0; i < p_chunkIDs.length; i++) {
				entryVertices[i] = new Vertex(p_chunkIDs[i]);
			}
			
			ArrayList<Vertex> lockedVertices = new ArrayList<Vertex>();
			ArrayList<Vertex> failedLockedVertices = new ArrayList<Vertex>();
			
			for (int i = 0; i < entryVertices.length; i++)
			{
				if (lockService.lock(true, 500, entryVertices[i]) != ErrorCode.SUCCESS) {
					failedLockedVertices.add(entryVertices[i]);
				} else {
					lockedVertices.add(entryVertices[i]);
				}
			}
			
			entryVertices = lockedVertices.toArray(new Vertex[0]);
			if (chunkService.get(entryVertices) != entryVertices.length)
			{
				loggerService.error(getClass(), "Getting vertices failed.");
				return;
			}
	
			// -----------------------------------------------------------------------------------
			
			long visited = 0;
			for (Vertex v : entryVertices)
			{
				if (v.getUserData() == -1)
				{
					v.setUserData(0);

					visited++;
					
					// spawn further jobs for neighbours
					List<Long> neighbours = v.getNeighbours();
					int neightbourIndex = 0;
					while (neightbourIndex < neighbours.size())
					{
						int neighbourCount = 0;
						if (neightbourIndex + m_vertexBatchCount >= neighbours.size()) {
							neighbourCount = neighbours.size() - neightbourIndex;
						} else {
							neighbourCount = m_vertexBatchCount;
						}
						
						long[] batch = new long[neighbourCount];
						for (int i = 0; i < batch.length; i++) {
							batch[i] = neighbours.get(neightbourIndex++);
						}
						
						if (!jobService.pushJob(new JobBFS(m_vertexBatchCount, m_chunkIDVisitedCount, batch))) {
							loggerService.error(getClass(), "Creating job for neighbours of vertex " + v + " failed.");
						}
					}
				}
			}
			
			// put back data and unlock all
			if (chunkService.put(ChunkLockOperation.WRITE_LOCK, entryVertices) != entryVertices.length)
			{
				loggerService.error(getClass(), "Putting vertices failed.");
			}
			
			// -----------------------------------------------------------------------------------
			
			if (visited > 0)
			{
				// update visited count
				if (lockService.lock(true, 30000, m_chunkIDVisitedCount) != ErrorCode.SUCCESS)
				{
					loggerService.error(getClass(), "Locking visited counter chunk " + Long.toHexString(m_chunkIDVisitedCount) + " failed.");
					return;
				}
				
				Counter counter = new Counter(m_chunkIDVisitedCount);
				if (chunkService.get(counter) != 1)
				{
					loggerService.error(getClass(), "Getting counter failed.");
					return;
				}
				
				counter.incrementCounter(visited);
				System.out.println(counter);
				
				if (chunkService.put(ChunkLockOperation.WRITE_LOCK, counter) != 1)
				{
					loggerService.error(getClass(), "Upadting counter failed.");
				}
			}
			
			// -----------------------------------------------------------------------------------
			
			// spawn separate job for vertices that failed to lock
			if (failedLockedVertices.size() > 0)
			{
				long[] failedChunkIDs = new long[failedLockedVertices.size()];
				for (int i = 0; i < failedChunkIDs.length; i++) {
					failedChunkIDs[i] = failedLockedVertices.get(i).getID();
				}
			
				if (!jobService.pushJob(new JobBFS(m_vertexBatchCount, m_chunkIDVisitedCount, failedChunkIDs)))
				{
					loggerService.error(getClass(), "Creating job for failed vertices failed.");
				}
			}
		}
	}
}

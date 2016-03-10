package de.hhu.bsinfo.dxgraph.algo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

import de.hhu.bsinfo.dxgraph.data.Counter;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.utils.locks.SpinLock;

@Deprecated
public class GraphAlgorithmBFSSingleThreadDep extends GraphAlgorithm {

	private int m_batchCountPerJob = 1;
	
	public GraphAlgorithmBFSSingleThreadDep(final int p_batchCountPerJob, final long... p_entryNodes)
	{
		super(null, p_entryNodes);
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
		private long m_chunkIDVisitedCount = ChunkID.INVALID_ID;
		private Lock m_mutex = new SpinLock();
		
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
			
			Vertex[] entryVertices = new Vertex[p_chunkIDs.length];
			for (int i = 0; i < p_chunkIDs.length; i++) {
				if (p_chunkIDs[i] == 0) {
					loggerService.warn(getClass(), "Invalid vertix with id 0 found, filtering.");
				} else {
					entryVertices[i] = new Vertex(p_chunkIDs[i]);
				}
			}
			
			// don't let multiple workers execute this
			// this harms performance a lot but we want this
			// to be fully accurate for verification reasons
			m_mutex.lock();
			
			if (chunkService.get(entryVertices) != entryVertices.length)
			{
				loggerService.error(getClass(), "Getting vertices failed.");
				return;
			}
			
			// -----------------------------------------------------------------------------------
			
			ArrayList<Vertex> visited = new ArrayList<Vertex>();
			for (Vertex v : entryVertices)
			{
				if (v.getUserData() == -1)
				{
					v.setUserData(0);
					
					visited.add(v);

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
			
			// -----------------------------------------------------------------------------------
			
			if (visited.size() > 0)
			{				
				// put back data
				if (chunkService.put(visited.toArray(new Vertex[0])) != visited.size())
				{
					loggerService.error(getClass(), "Putting visited vertices failed.");
				}
				
				Counter counter = new Counter(m_chunkIDVisitedCount);
				if (chunkService.get(counter) != 1)
				{
					loggerService.error(getClass(), "Getting counter failed.");
					return;
				}
				
				counter.incrementCounter(visited.size());
				
				if (chunkService.put(counter) != 1)
				{
					loggerService.error(getClass(), "Upadting counter failed.");
				}
			}
			
			m_mutex.unlock();
		}
	}
}

package de.hhu.bsinfo.dxgraph.algo;

import java.util.List;

import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxgraph.load.GraphLoaderResultDelegate;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.logger.LoggerService;

@Deprecated
public class GraphAlgorithmBFSDep extends GraphAlgorithm {

	private int m_batchCountPerJob = 1;
	
	public GraphAlgorithmBFSDep(final int p_batchCountPerJob, final GraphLoaderResultDelegate p_loaderResultsDelegate, final long... p_entryNodes)
	{
		super(p_loaderResultsDelegate, p_entryNodes);
		m_batchCountPerJob = p_batchCountPerJob;
	}
	
	@Override
	protected boolean execute(long[] p_entryNodes) 
	{
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
			m_jobService.pushJob(new JobBFS(m_batchCountPerJob, tmp));
		}
		
		boolean ret = m_jobService.waitForLocalJobsToFinish();
		m_loggerService.info(getClass(), "Finished BFS.");
		return ret;
	}

	private static class JobBFS extends Job {

		public static final short MS_TYPE_ID = 1;
		static {
			registerType(MS_TYPE_ID, JobBFS.class);
		}
		
		private int m_vertexBatchCount = 1;
		
		public JobBFS(final int p_vertexBatchCount, final long... p_parameterChunkIDs)
		{
			super(p_parameterChunkIDs);
			m_vertexBatchCount = p_vertexBatchCount;
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
			
			if (chunkService.get(entryVertices) != entryVertices.length)
			{
				loggerService.error(getClass(), "Getting vertices failed.");
				return;
			}
			
			// -----------------------------------------------------------------------------------
			
			for (Vertex v : entryVertices)
			{
				if (v.getUserData() == -1)
				{
					v.setUserData(0);
					if (chunkService.put(v) != 1)
					{
						loggerService.error(getClass(), "Marking vertex " + v + " as visited failed.");
						continue;
					}
					
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
						
						if (!jobService.pushJob(new JobBFS(m_vertexBatchCount, batch))) {
							loggerService.error(getClass(), "Creating job for neighbours of vertex " + v + " failed.");
						}
					}
				}
			}
		}
	}
}

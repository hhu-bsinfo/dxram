package de.hhu.bsinfo.dxgraph.algo;

import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxgraph.load.GraphLoaderResultDelegate;
import de.hhu.bsinfo.dxram.chunk.AsyncChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.logger.LoggerService;

@Deprecated
public class GraphAlgorithmBFS2Dep extends GraphAlgorithm {

	private int m_batchCountPerJob = 1;
	
	public GraphAlgorithmBFS2Dep(final int p_batchCountPerJob, final GraphLoaderResultDelegate p_loaderResultsDelegate,  long... p_entryNodes)
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
			AsyncChunkService asyncChunkService = getService(AsyncChunkService.class);
			ChunkService chunkService = getService(ChunkService.class);
			JobService jobService = getService(JobService.class);
			LoggerService loggerService = getService(LoggerService.class);
			
			Vertex2[] entryVertices = new Vertex2[p_chunkIDs.length];
			for (int i = 0; i < p_chunkIDs.length; i++) {
				if (p_chunkIDs[i] == 0) {
					loggerService.warn(getClass(), "Invalid vertix with id 0 found, filtering.");
				} else {
					entryVertices[i] = new Vertex2(p_chunkIDs[i]);
				}
			}
			
			int gett = chunkService.get(entryVertices);
			if (gett != entryVertices.length)
			{
				loggerService.error(getClass(), "Getting vertices failed.");
				return;
			}
			
			// -----------------------------------------------------------------------------------
			
			for (int i = 0; i < entryVertices.length; i++)
			{
				if (entryVertices[i].getUserData() == -1)
				{
					entryVertices[i].setUserData(0);
					
					// spawn further jobs for neighbours
					long[] neighbours = entryVertices[i].getNeighbours();
					int neightbourIndex = 0;
					while (neightbourIndex < neighbours.length)
					{
						int neighbourCount = 0;
						if (neightbourIndex + m_vertexBatchCount >= neighbours.length) {
							neighbourCount = neighbours.length - neightbourIndex;
						} else {
							neighbourCount = m_vertexBatchCount;
						}
						
						long[] batch = new long[neighbourCount];
						for (int j = 0; j < batch.length; j++) {
							batch[j] = neighbours[neightbourIndex++];
						}
						
						if (!jobService.pushJob(new JobBFS(m_vertexBatchCount, batch))) {
							loggerService.error(getClass(), "Creating job for neighbours of vertex " + entryVertices[i] + " failed.");
						}
					}
				}
				else
				{
					entryVertices[i] = null;
				}
			}
			
			asyncChunkService.put(entryVertices);
		}
	}
}

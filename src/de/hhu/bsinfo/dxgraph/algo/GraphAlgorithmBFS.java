package de.hhu.bsinfo.dxgraph.algo;

import java.util.List;

import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public class GraphAlgorithmBFS extends GraphAlgorithm {

	private int m_batchCountPerJob = 1;
	
	public GraphAlgorithmBFS()
	{
		
	}
	
	public void setBatchCountPerJob(final int p_batchCount)
	{
		m_batchCountPerJob = p_batchCount;
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
			
			m_jobService.pushJob(new JobBFS(m_batchCountPerJob, tmp));
		}
		
		return m_jobService.waitForLocalJobsToFinish();
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
				entryVertices[i] = new Vertex(p_chunkIDs[i]);
			}
			
			if (chunkService.get(entryVertices) != entryVertices.length)
			{
				loggerService.error(getClass(), "Getting vertices failed.");
				return;
			}
		
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
					
					System.out.println(this + ", Visited: " + v);
					
					// spawn further jobs for neighbours
					List<Long> neighbours = v.getNeighbours();
					int neighboursLeft = neighbours.size();
					while (true)
					{
						if (neighboursLeft <= m_vertexBatchCount) {
							// use an array that fits
							long[] batch = new long[neighboursLeft];
							
							for (int i = 0; i < neighboursLeft; i++) {
								batch[i] = neighbours.remove(neighbours.size() - 1);
								// might yield better performance as the list does not have
								// to be shifted
							}
							
							jobService.pushJob(new JobBFS(m_vertexBatchCount, batch));
							break;
						} else {
							long[] batch = new long[m_vertexBatchCount];
							for (int i = 0; i < batch.length; i++) {
								batch[i] = neighbours.remove(neighbours.size() - 1);
								// might yield better performance as the list does not have
								// to be shifted
							}
							neighboursLeft -= batch.length;
							
							jobService.pushJob(new JobBFS(m_vertexBatchCount, batch));
						}
					}
				}
				else
				{
					System.out.println(this + ", Already visited: " + v);
				}
			}
		}
	}
}

package de.hhu.bsinfo.dxgraph.algo;

import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.job.JobService;

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
			
			pushJob(new JobBFS(tmp));
		}
		
		return waitForLocalJobsToFinish();
	}

	private static class JobBFS extends Job {

		public static final short MS_TYPE_ID = 1;
		static {
			registerType(MS_TYPE_ID, JobBFS.class);
		}
		
		public JobBFS(final long... p_parameterChunkIDs)
		{
			super(p_parameterChunkIDs);
		}

		@Override
		public short getTypeID() {
			return MS_TYPE_ID;
		}

		@Override
		protected void execute(short p_nodeID, long[] p_chunkIDs) {
			ChunkService chunkService = getService(ChunkService.class);
			JobService jobService = getService(JobService.class);
			
			Vertex[] entryVertices = new Vertex[p_chunkIDs.length];
			for (int i = 0; i < p_chunkIDs.length; i++) {
				entryVertices[i] = new Vertex(p_chunkIDs[i]);
			}
			
			if (chunkService.get(entryVertices) != entryVertices.length)
			{
				// TODO error handling
			}
		
			for (Vertex v : entryVertices)
			{
				if (v.getUserData() == -1)
				{
					
					v.setUserData(0);
					if (chunkService.put(v) != 1)
					{
						// TODO error handling
					}
					System.out.println(this + ", Visited: " + v);
					
					// spawn further jobs for neighbours
					for (Long neighbour : v.getNeighbours())
					{
						// TODO have block of neighbours for a job? avoid creating to many jobs
						// -> have evaluation with different amounts of vertices per job
						jobService.pushJob(new JobBFS(neighbour));
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

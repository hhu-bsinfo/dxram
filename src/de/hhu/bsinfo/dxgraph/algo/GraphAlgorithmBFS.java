package de.hhu.bsinfo.dxgraph.algo;

import java.util.List;

import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public class GraphAlgorithmBFS extends GraphAlgorithm {

	private int m_batchCountPerJob = 1;
	
	public GraphAlgorithmBFS(final int p_batchCountPerJob, final long... p_entryNodes)
	{
		super(p_entryNodes);
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
		
//			System.out.print(this + ", entry vertices: ");
//			for (Vertex v : entryVertices)
//			{
//				System.out.print(Long.toHexString(v.getID()) + ", ");
//			}
//			System.out.println();
			
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
						
						jobService.pushJob(new JobBFS(m_vertexBatchCount, batch));
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

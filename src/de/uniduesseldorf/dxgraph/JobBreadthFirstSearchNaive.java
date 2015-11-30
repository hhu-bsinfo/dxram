package de.uniduesseldorf.dxgraph;

import de.uniduesseldorf.dxcompute.ComputeJob;
import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;

// TODO create separate optimized version:
// bulk get neighbours by job and pass buffered data to jobs instead of id only (?)
// -> disadvantage: needs more memory for buffers for sure
public class JobBreadthFirstSearchNaive extends ComputeJob
{
	private long m_entryNode;
	private int m_previousDepth;
	
	public JobBreadthFirstSearchNaive(final long p_entryNode, final int p_previousDepth)
	{
		m_entryNode = p_entryNode;
		m_previousDepth = p_previousDepth;
	}
	
	@Override
	protected void execute() 
	{		
		final int curDepth = m_previousDepth + 1;
		
		byte[] vertexData = getStorageInterface().get(m_entryNode);
		if (vertexData == null)
		{
			getJobInterface().log(LOG_LEVEL.LL_ERROR, "No data for vertex " + m_entryNode);
			return;
		}

		int userData = SimpleVertex.getUserData(vertexData);
		// not visited yet
		if (userData == -1)
		{
			SimpleVertex.setUserData(vertexData, curDepth);
			getStorageInterface().put(m_entryNode, vertexData);
			
			// gather neighbours in bulk
			int curNeighbour = 0;
			int numNeighbours = SimpleVertex.getNumberOfNeighbours(vertexData);
			for (int i = 0; i < numNeighbours; i++)
			{
				long newEntryNode = SimpleVertex.getNeighbour(vertexData, curNeighbour);
				getJobInterface().pushJobPublicLocalQueue(new JobBreadthFirstSearchNaive(newEntryNode, curDepth));
			}
		}
	}

	@Override
	public long getJobID() {
		return m_entryNode;
	}

}

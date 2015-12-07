package de.uniduesseldorf.dxgraph.bfs;

import de.uniduesseldorf.dxcompute.ComputeJob;
import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;
import de.uniduesseldorf.dxgraph.data.SimpleVertex;

public class JobBreadthFirstSearchNaive extends ComputeJob
{
	private SimpleVertex m_entryNode;
	private int m_previousDepth;
	
	public JobBreadthFirstSearchNaive(final SimpleVertex p_entryNode, final int p_previousDepth)
	{
		m_entryNode = p_entryNode;
		m_previousDepth = p_previousDepth;
	}
	
	@Override
	protected void execute() 
	{		
		final int curDepth = m_previousDepth + 1;
		
		m_entryNode.setUserData(1);
		if (getStorageDelegate().put(m_entryNode) < 1)
			log(LOG_LEVEL.LL_ERROR, "Marking vertex " + m_entryNode + " visited failed.");
		
		// TODO lock current vertex with chunk locking interface of dxram?
		
		SimpleVertex[] neighbours = new SimpleVertex[m_entryNode.getNeighbours().size()];
		for (int i = 0; i < m_entryNode.getNeighbours().size(); i++)
			neighbours[i] = new SimpleVertex(m_entryNode.getNeighbours().get(i));
		
		int neighbourCount = getStorageDelegate().get(neighbours);
		if (neighbourCount < neighbours.length)
			log(LOG_LEVEL.LL_WARNING, "Getting neighbours of vertex " + m_entryNode 
					+ " failed (" + neighbourCount + "/" + neighbours.length + ")");
				
		for (int i = 0; i < neighbours.length; i++)
		{
			if (neighbours[i].getUserData() != 1)
				getJobInterface().pushJobPublicLocalQueue(new JobBreadthFirstSearchNaive(neighbours[i], curDepth));
		}
	}

	@Override
	public long getJobID() {
		return m_entryNode.getID();
	}

}

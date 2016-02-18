package de.hhu.bsinfo.dxgraph.gen;

import de.hhu.bsinfo.dxcompute.Task;

public abstract class GraphGenerator extends Task
{
	private int m_numNodes = 1;
	
	public GraphGenerator()
	{
		
	}
	
	// cluster nodes to be precise
	public void setNumNodes(final int p_numNodes)
	{
		m_numNodes = p_numNodes;
	}
	
	@Override
	public boolean execute() {
		m_loggerService.debug(getClass(), "Executing graph generation for " + m_numNodes + " nodes.");
		boolean ret = generate(m_numNodes);
		if (ret) {
			m_loggerService.debug(getClass(), "Executing graph generation successful.");
		} else {
			m_loggerService.error(getClass(), "Executing graph generation failed.");
		}
		
		return ret;
	}
	
	public abstract boolean generate(final int p_numNodes);
}

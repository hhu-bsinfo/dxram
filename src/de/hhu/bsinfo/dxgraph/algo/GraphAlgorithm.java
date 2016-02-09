package de.hhu.bsinfo.dxgraph.algo;

import de.hhu.bsinfo.dxgraph.GraphTask;

public abstract class GraphAlgorithm extends GraphTask {

	private long[] m_entryNodes = new long[0];
	
	public GraphAlgorithm()
	{
		
	}
	
	public void setEntryNodes(final long... p_entryNodes)
	{
		m_entryNodes = p_entryNodes;
	}
	
	@Override
	public boolean execute() {
		log("Executing algorithm with " + m_entryNodes.length + " entry nodes.");
		boolean ret = execute(m_entryNodes);
		if (ret) {
			log("Executing graph algorithm successful.");
		} else {
			logError("Executing graph algorithm failed.");
		}
		
		return ret;
	}

	protected abstract boolean execute(final long[] p_entryNodes);
}

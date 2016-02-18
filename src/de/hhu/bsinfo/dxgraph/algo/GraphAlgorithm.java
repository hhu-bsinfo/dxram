package de.hhu.bsinfo.dxgraph.algo;

import de.hhu.bsinfo.dxcompute.Task;

public abstract class GraphAlgorithm extends Task {

	private long[] m_entryNodes = new long[0];
	
	public GraphAlgorithm(final long... p_entryNodes)
	{
		m_entryNodes = p_entryNodes;
	}

	@Override
	public boolean execute() {
		m_loggerService.debug(getClass(), "Executing algorithm with " + m_entryNodes.length + " entry nodes.");
		boolean ret = execute(m_entryNodes);
		if (ret) {
			m_loggerService.debug(getClass(), "Executing graph algorithm successful.");
		} else {
			m_loggerService.error(getClass(), "Executing graph algorithm failed.");
		}
		
		return ret;
	}

	protected abstract boolean execute(final long[] p_entryNodes);
}

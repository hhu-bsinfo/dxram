package de.hhu.bsinfo.dxgraph.algo.bfs;

import de.hhu.bsinfo.dxcompute.Task;
import de.hhu.bsinfo.dxgraph.load.GraphLoaderResultDelegate;

public abstract class GraphAlgorithm extends Task {

	private GraphLoaderResultDelegate m_loaderResultDelegate;
	private long[] m_entryNodes = new long[0];
	
	public GraphAlgorithm(final GraphLoaderResultDelegate p_loaderResultsDelegate, final long... p_entryNodes)
	{
		m_loaderResultDelegate = p_loaderResultsDelegate;
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
	
	protected GraphLoaderResultDelegate getGraphLoaderResultDelegate()
	{
		return m_loaderResultDelegate;
	}

	protected abstract boolean execute(final long[] p_entryNodes);
}

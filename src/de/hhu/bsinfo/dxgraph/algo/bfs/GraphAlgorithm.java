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
		m_loggerService.debug(getClass(), "Setting up algorithm for " + m_loaderResultDelegate.getTotalVertexCount() + " vertices (on this node)");
		if (!setup(m_loaderResultDelegate.getTotalVertexCount())) {
			m_loggerService.error(getClass(), "Setting up algorithm failed.");
			return false;
		}
		
		m_loggerService.debug(getClass(), "Executing algorithm with " + m_entryNodes.length + " entry nodes.");
		// have arguments override roots list loaded from file
		boolean ret;
		if (m_entryNodes.length != 0) {
			ret = execute(m_entryNodes);
		} else {
			ret = execute(m_loaderResultDelegate.getRoots());
		}
		 
		if (ret) {
			m_loggerService.debug(getClass(), "Executing graph algorithm successful.");
		} else {
			m_loggerService.error(getClass(), "Executing graph algorithm failed.");
		}
		
		return ret;
	}

	// total vertex count here is the total vertex count for this node, not the whole graph
	protected abstract boolean setup(final long p_totalVertexCount);
	
	protected abstract boolean execute(final long[] p_entryNodes);
}

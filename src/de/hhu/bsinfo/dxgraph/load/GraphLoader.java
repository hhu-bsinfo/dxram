package de.hhu.bsinfo.dxgraph.load;

import de.hhu.bsinfo.dxgraph.GraphTask;

public abstract class GraphLoader extends GraphTask
{
	public static final String MS_DEFAULT_PATH = "DEFAULT";
	
	private String m_path = new String("");
	private int m_numNodes = 1;
	
	public GraphLoader()
	{
		
	}
	
	public void setPath(final String p_path)
	{
		m_path = p_path;
	}
	
	public void setNumNodes(final int p_numNodes)
	{
		m_numNodes = p_numNodes;
	}
	
	@Override
	public boolean execute()
	{
		m_loggerService.debug(getClass(), "Loading graph, path '" + m_path + "' to " + m_numNodes + " nodes...");
		boolean ret = load(m_path, m_numNodes);
		if (ret) {
			m_loggerService.debug(getClass(), "Loading graph, path '" + m_path + "' successful.");
		} else {
			m_loggerService.error(getClass(), "Loading graph, path '" + m_path + "' failed.");
		}
		
		return ret;
	}
	
	protected abstract boolean load(final String p_path, final int p_numNodes);
}

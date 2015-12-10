package de.uniduesseldorf.dxram.core.engine;

import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodeRole;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesWatcher;

public class DXRAMSystemData 
{
	private NodesWatcher m_nodesWatcher = null;
	
	public DXRAMSystemData(final NodesWatcher p_nodesWatcher)
	{
		m_nodesWatcher = p_nodesWatcher;
	}
	
	public short getNodeID()
	{
		return m_nodesWatcher.getNodesConfiguration().getOwnNodeID();
	}
	
	public NodeRole getNodeRole()
	{
		return m_nodesWatcher.getNodesConfiguration().getOwnNodeEntry().getRole();
	}
	
	public byte[] getZookeeperData(final String p_path)
	{
		return m_nodesWatcher.getZookeeperData(p_path);
	}
}

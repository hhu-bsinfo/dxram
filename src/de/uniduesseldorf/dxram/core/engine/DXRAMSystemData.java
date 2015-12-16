package de.uniduesseldorf.dxram.core.engine;

import java.util.List;

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
	
	public byte[] zookeeperGetData(final String p_path)
	{
		return m_nodesWatcher.zookeeperGetData(p_path);
	}
	
	public List<String> zookeeperGetChildren(final String p_path) {
		return m_nodesWatcher.zookeeperGetChildren(p_path);
	}
}

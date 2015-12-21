package de.uniduesseldorf.dxram.core.engine;

import java.util.List;

import org.apache.zookeeper.data.Stat;

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
	
	public void zookeeperCreate(final String p_path) {
		m_nodesWatcher.zookeeperCreate(p_path);
	}
	
	public void zookeeperDelete(final String p_path, final int p_version) {
		m_nodesWatcher.zookeeperDelete(p_path, p_version);
	}
	
	public Stat zookeeperGetStatus(final String p_path) {
		return m_nodesWatcher.zookeeperGetStatus(p_path);
	}
	
	public byte[] zookeeperGetData(final String p_path)
	{
		return m_nodesWatcher.zookeeperGetData(p_path);
	}
	
	public byte[] zookeeperGetData(final String p_path, Stat p_status) {
		return m_nodesWatcher.zookeeperGetData(p_path, p_status);
	}
	
	public List<String> zookeeperGetChildren(final String p_path) {
		return m_nodesWatcher.zookeeperGetChildren(p_path);
	}
	
	public boolean zookeeperExists(final String p_path) {
		return m_nodesWatcher.zookeeperPathExists(p_path);
	}
}

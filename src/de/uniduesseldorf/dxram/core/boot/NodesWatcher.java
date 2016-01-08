
package de.uniduesseldorf.dxram.core.boot;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationException;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationParser;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationRuntimeException;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration.NodeEntry;

import de.uniduesseldorf.utils.BloomFilter;
import de.uniduesseldorf.utils.CRC16;
import de.uniduesseldorf.utils.ZooKeeperHandler;
import de.uniduesseldorf.utils.ZooKeeperHandler.ZooKeeperException;

/**
 * The NodesWatcher parses the initial nodes configuration file and sets up
 * routing information with zookeeper. It also listens to events from zookeeper
 * and updates the NodesConfiguration based on these events.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 9.12.15
 */
public final class NodesWatcher implements Watcher {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(NodesWatcher.class);
	


	// Constructors
	/**
	 * Creates an instance of DefaultNodesConfigurationParser
	 */
	public NodesWatcher(final String p_ownIP, final int p_ownPort, final String p_zooKeeperPath, final String p_zookeeperConnection, final int p_zookeeperTimeout, final int p_zookeeperBitfieldSize) 
	{

	}
	
	/**
	 * Get the NodesConfiguration the watcher uses to keep track of routing information
	 * of other nodes and our own instance.
	 * @return NodesConfiguration.
	 */
	public NodesConfiguration getNodesConfiguration()
	{
		return m_nodesConfiguration;
	}
	
	public void zookeeperCreate(final String p_path) {
		try {
			m_zookeeper.create(p_path);
		} catch (ZooKeeperException | KeeperException | InterruptedException e) {
			LOGGER.error("Creating path in zookeeper failed.", e);
		}
	}
	
	public Stat zookeeperGetStatus(final String p_path) {
		Stat status = null;
		
		try {
			status = m_zookeeper.getStatus(p_path);
		} catch (ZooKeeperException e) {
			LOGGER.error("Getting status from zookeeper failed.", e);
		}
		
		return status;
	}
	
	public void zookeeperDelete(final String p_path, final int p_version) {
		try {
			m_zookeeper.delete(p_path, p_version);
		} catch (ZooKeeperException e) {
			LOGGER.error("Deleting path from zookeeper failed.", e);
		}
	}
	
	public List<String> zookeeperGetChildren(final String p_path) {
		List<String> children = null;
		
		try {
			children = m_zookeeper.getChildren(p_path);
		} catch (ZooKeeperException e) {
			LOGGER.error("Getting children of " + p_path + " from zookeeper failed.", e);
		}
		
		return children;
	}
	
	public byte[] zookeeperGetData(final String p_path)
	{
		byte[] data = null;
		
		try {
			data = m_zookeeper.getData(p_path);
		} catch (ZooKeeperException e) {
			LOGGER.error("Getting data from zookeeper failed.", e);
		}
		
		return data;
	}
	
	public byte[] zookeeperGetData(final String p_path, Stat p_status)
	{
		byte[] data = null;
		
		try {
			data = m_zookeeper.getData(p_path, p_status);
		} catch (ZooKeeperException e) {
			LOGGER.error("Getting data from zookeeper failed.", e);
		}
		
		return data;
	}
	
	public boolean zookeeperSetData(final String p_path, final byte[] p_data, final int p_version)
	{
		try {
			m_zookeeper.setData(p_path, p_data, p_version);
			return true;
		} catch (ZooKeeperException e) {
			LOGGER.error("Setting data on zookeeper failed.", e);
			return false;
		}
	}
	
	public boolean zookeeperPathExists(final String p_path) {
		boolean ret = false;
		
		try {
			ret = m_zookeeper.exists(p_path);
		} catch (ZooKeeperException e) {
			LOGGER.error("Checking if path exists in zookeeper failed.", e);
		}
		
		return ret;
	}
	
	/** 
	 * Setup the watcher by parsing the configuration (file) provided with the parser interface
	 * and setup zookeeper with initial information.
	 * @param p_parser Parse to parse initial nodes configuration data from.
	 * @throws NodesConfigurationException If any error occurs while parsing configuration data.
	 */
	public void setupNodeRouting(final NodesConfigurationParser p_parser) throws NodesConfigurationException
	{
		List<NodeEntry> nodeEntries;
		
		nodeEntries = p_parser.readConfiguration();
		parseNodes(nodeEntries);
		
		LOGGER.debug("Resulting nodes configuration for setup:\n" + m_nodesConfiguration);
	}
	
	public void close(final boolean p_cleanup)
	{
		try {
			m_zookeeper.close(p_cleanup);
		} catch (ZooKeeperException e) {
			LOGGER.error("Closing zookeeper failed.", e);
		}
	}

	// Methods

	@Override
	public void process(final WatchedEvent p_event) {
		// TODO: Check this!
		String path;
		String prefix;

		List<String> childs;
		short nodeID;
		String node;
		String[] splits;

		if (p_event.getType() == Event.EventType.None && p_event.getState() == KeeperState.Expired) {
			LOGGER.error("ERR:ZooKeeper state expired");
		} else {
			try {
				path = p_event.getPath();
				prefix = m_zookeeper.getPath() + "/";
				while (m_isStarting) {
					try {
						Thread.sleep(100);
					} catch (final InterruptedException e) {}
				}
				if (null != path) {
					if (path.equals(prefix + "nodes/new")) {
						childs = m_zookeeper.getChildren("nodes/new", this);
						for (String child : childs) {
							nodeID = Short.parseShort(child);
							node = new String(m_zookeeper.getData("nodes/new/" + nodeID));
							splits = node.split("/");

							m_nodesConfiguration.addNode(nodeID, new NodeEntry(splits[0], Integer.parseInt(splits[1]), (short) 0, (short) 0, NodeRole.PEER));
						}
					}
				}
			} catch (final ZooKeeperException e) {
				LOGGER.error("ERR:Could not access ZooKeeper", e);
			}
		}
	}

}

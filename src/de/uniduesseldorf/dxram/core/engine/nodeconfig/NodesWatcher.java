
package de.uniduesseldorf.dxram.core.engine.nodeconfig;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodeRole;
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
	
	// Attributes
	private String m_ownIP;
	private int m_ownPort;
	private NodesConfiguration m_nodesConfiguration;
	private ZooKeeperHandler m_zookeeper;
	private int m_zookeeperBitfieldSize;
	private short m_bootstrap;
	private CRC16 m_hashGenerator;
	private BloomFilter m_bloomFilter;

	private boolean m_isStarting;

	// Constructors
	/**
	 * Creates an instance of DefaultNodesConfigurationParser
	 */
	public NodesWatcher(final String p_ownIP, final int p_ownPort, final String p_zooKeeperPath, final String p_zookeeperConnection, final int p_zookeeperTimeout, final int p_zookeeperBitfieldSize) 
	{
		m_ownIP = p_ownIP;
		m_ownPort = p_ownPort;
		m_nodesConfiguration = new NodesConfiguration();
		m_zookeeper = new ZooKeeperHandler(p_zooKeeperPath, p_zookeeperConnection, p_zookeeperTimeout);
		m_isStarting = true;
		m_zookeeperBitfieldSize = p_zookeeperBitfieldSize;
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
	/**
	 * Parses the configured nodes
	 * @param p_nodes
	 *            the nodes to parse
	 * @return the parsed nodes
	 * @throws DXRAMException
	 *             if the nodes could not be parsed
	 */
	private void parseNodes(final List<NodeEntry> p_nodes) {
		String barrier;
		boolean parsed = false;

		m_hashGenerator = new CRC16();
		m_bloomFilter = new BloomFilter(m_zookeeperBitfieldSize, 65536);

		barrier = "barrier";

		try {
			if (!m_zookeeper.exists("nodes")) {
				try {
					// Set barrier object
					m_zookeeper.createBarrier(barrier);
					// Load nodes routing information
					parseNodesBootstrap(p_nodes);
					parsed = true;
					// Delete barrier object
					m_zookeeper.deleteBarrier(barrier);
				} catch (final ZooKeeperException | KeeperException | InterruptedException e) {
					// Barrier does exist
				}
			}

			if (!parsed) {
				// normal node
				m_zookeeper.waitForBarrier(barrier, this);
				 parseNodesNormal(p_nodes);
			}
		} catch (final ZooKeeperException e) {
			throw new NodesConfigurationRuntimeException("Could not access ZooKeeper", e);
		}

		m_isStarting = false;
	}

	/**
	 * Parses information from a nodes configuration object and creates routing information
	 * in zookeeper. Also assigns valid node IDs and 
	 * @param p_nodes
	 *            the nodes to parse
	 * @return the parsed nodes
	 * @note this method is called by bootstrap only
	 */
	private void parseNodesBootstrap(final List<NodeEntry> p_nodes) {
		short nodeID;
		int numberOfSuperpeers;
		int seed;

		LOGGER.trace("Entering parseNodesBootstrap");

		try {
			m_zookeeper.create("nodes");

			// Parse node information
			numberOfSuperpeers = 0;
			seed = 1;

			for (NodeEntry entry : p_nodes) {
				nodeID = m_hashGenerator.hash(seed);
				while (m_bloomFilter.contains(nodeID) || nodeID == -1) {
					nodeID = m_hashGenerator.hash(++seed);
				}
				seed++;

				m_bloomFilter.add(nodeID);

				// assign own node entry
				if (m_ownIP.equals(entry.getIP()) && m_ownPort == entry.getPort()) {
					m_nodesConfiguration.setOwnNodeID(nodeID);
					m_bootstrap = nodeID;
				}
				if (entry.getRole().equals(NodeRole.SUPERPEER)) {
					numberOfSuperpeers++;
				}

				m_nodesConfiguration.addNode((short) (nodeID & 0x0000FFFF), entry);
			}

			m_zookeeper.create("nodes/new");
			m_zookeeper.setChildrenWatch("nodes/new", this);

			m_zookeeper.create("nodes/free");
			m_zookeeper.setChildrenWatch("nodes/free", this);

			// check if own node entry was correctly assigned to a valid node ID
			if (m_nodesConfiguration.getOwnNodeEntry() == null) {
				throw new NodesConfigurationRuntimeException("Bootstrap entry for node in nodes.config missing!");
			}

			m_zookeeper.create("nodes/bootstrap", (m_bootstrap + "").getBytes());
			m_zookeeper.create("nodes/superpeers", (numberOfSuperpeers + "").getBytes());

			m_zookeeper.create("nodes/peers");
			// Register superpeer
			m_zookeeper.create("nodes/superpeers/" + m_nodesConfiguration.getOwnNodeID());
		} catch (final ZooKeeperException | KeeperException | InterruptedException e) {
			throw new NodesConfigurationRuntimeException("Parsing nodes bootstrap failed.", e);
		}

		LOGGER.trace("Exiting parseNodesBootstrap");
	}

	/**
	 * Parses nodes.config and stores routing information in net for nodes
	 * @param p_nodes
	 *            the nodes to parse
	 * @return the parsed nodes
	 * @note this method is called by every node except bootstrap
	 */
	private void parseNodesNormal(final List<NodeEntry> p_nodes) {
		short nodeID;
		int seed;
		String node;
		List<String> childs;

		String[] splits;

		LOGGER.trace("Entering parseNodesNormal");


		try {
			// Parse node information
			seed = 1;

			for (NodeEntry entry : p_nodes) {
				nodeID = m_hashGenerator.hash(seed);
				while (m_bloomFilter.contains(nodeID) || nodeID == -1) {
					nodeID = m_hashGenerator.hash(++seed);
				}
				seed++;

				m_bloomFilter.add(nodeID);

				if (m_ownIP.equals(entry.getIP()) && m_ownPort == entry.getPort()) {
					m_nodesConfiguration.setOwnNodeID(nodeID);
					m_bootstrap = nodeID;
				}

				m_nodesConfiguration.addNode((short) (nodeID & 0x0000FFFF), entry);
			}

			m_bootstrap = Short.parseShort(new String(m_zookeeper.getData("nodes/bootstrap")));

			// Apply changes
			childs = m_zookeeper.getChildren("nodes/new");
			for (String child : childs) {
				nodeID = Short.parseShort(child);
				node = new String(m_zookeeper.getData("nodes/new/" + nodeID));
				m_bloomFilter.add(nodeID);

				// Set routing information for that node
				splits = node.split("/");
				
				m_nodesConfiguration.addNode(nodeID, new NodeEntry(splits[0], Integer.parseInt(splits[1]), (short) 0, (short) 0, NodeRole.PEER));

				if (nodeID == m_nodesConfiguration.getOwnNodeID()) {
					// NodeID was already re-used
					m_nodesConfiguration.setOwnNodeID(NodesConfiguration.INVALID_NODE_ID);
				}
			}

			if (m_nodesConfiguration.getOwnNodeID() == NodesConfiguration.INVALID_NODE_ID) {
				// Add this node if it was not in start configuration
				LOGGER.warn("node not in nodes.config (" + m_ownIP + ", " + m_ownPort + ")");

				node = m_ownIP + "/" + m_ownPort + "/" + "P" + "/" + 0 + "/" + 0;

				childs = m_zookeeper.getChildren("nodes/free");
				if (!childs.isEmpty()) {
					nodeID = Short.parseShort(childs.get(0));
					m_nodesConfiguration.setOwnNodeID(nodeID);
					m_zookeeper.create("nodes/new/" + nodeID, node.getBytes());
					m_zookeeper.delete("nodes/free/" + nodeID);
				} else {
					splits = m_ownIP.split("\\.");
					seed = ((Integer.parseInt(splits[1]) << 16) + (Integer.parseInt(splits[2]) << 8) + Integer.parseInt(splits[3])) * -1;
					nodeID = m_hashGenerator.hash(seed);
					while (m_bloomFilter.contains(nodeID) || -1 == nodeID) {
						nodeID = m_hashGenerator.hash(--seed);
					}
					m_bloomFilter.add(nodeID);
					// Set own NodeID
					m_nodesConfiguration.setOwnNodeID(nodeID);
					m_zookeeper.create("nodes/new/" + nodeID, node.getBytes());
				}
				
				// Set routing information for that node
				m_nodesConfiguration.addNode(nodeID, new NodeEntry(m_ownIP, m_ownPort, (short) 0, (short) 0, NodeRole.PEER));
			} else {
				// Remove NodeID if this node failed before
				nodeID = m_nodesConfiguration.getOwnNodeID();
				if (m_zookeeper.exists("nodes/free/" + nodeID)) {
					m_zookeeper.delete("nodes/free/" + nodeID);
				}
			}
			// Set watches
			m_zookeeper.setChildrenWatch("nodes/new", this);
			m_zookeeper.setChildrenWatch("nodes/free", this);

			// Register peer/superpeer
			if (m_nodesConfiguration.getOwnNodeEntry().getRole().equals(NodeRole.SUPERPEER)) {
				m_zookeeper.create("nodes/superpeers/" + m_nodesConfiguration.getOwnNodeID());
			} else if (m_nodesConfiguration.getOwnNodeEntry().getRole().equals(NodeRole.PEER)) {
				m_zookeeper.create("nodes/peers/" + m_nodesConfiguration.getOwnNodeID());
			}
		} catch (final ZooKeeperException | KeeperException | InterruptedException e) {
			throw new NodesConfigurationRuntimeException("Parsing nodes normal failed.", e);
		}

		LOGGER.trace("Exiting parseNodesNormal");
	}

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

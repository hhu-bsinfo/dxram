
package de.uniduesseldorf.dxram.core.api.config;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.NodesConfigurationEntry;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.util.NodeID;

import de.uniduesseldorf.utils.BloomFilter;
import de.uniduesseldorf.utils.CRC16;
import de.uniduesseldorf.utils.ZooKeeperHandler;
import de.uniduesseldorf.utils.ZooKeeperHandler.ZooKeeperException;

/**
 * Parses the configured nodes
 * @author Florian Klein 28.11.2013
 */
public final class DefaultNodesConfigurationParser extends AbstractNodesConfigurationParser implements Watcher {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(DefaultNodesConfigurationParser.class);
	private static final Role DEFAULT_ROLE = Core.getConfiguration().getRoleValue(ConfigurationConstants.DXRAM_ROLE);

	// Attributes
	private short m_bootstrap;
	private CRC16 m_hashGenerator;
	private BloomFilter m_bloomFilter;

	private boolean m_isStarting;

	// Constructors
	/**
	 * Creates an instance of DefaultNodesConfigurationParser
	 */
	public DefaultNodesConfigurationParser() {
		m_isStarting = true;
	}

	// Methods
	@Override
	public long[] parseNodes(final NodesConfiguration p_configuration) throws DXRAMException {
		return parseNodes(p_configuration.getNodes());
	}

	@Override
	public long[] parseNodes(final List<NodesConfigurationEntry> p_nodes) throws DXRAMException {
		long[] ret = null;
		String barrier;
		boolean parsed = false;

		m_hashGenerator = new CRC16();
		m_bloomFilter = new BloomFilter(Core.getConfiguration().getIntValue(ConfigurationConstants.ZOOKEEPER_BITFIELD_SIZE), 65536);

		barrier = "barrier";

		try {
			if (!ZooKeeperHandler.exists("nodes")) {
				try {
					// Set barrier object
					ZooKeeperHandler.createBarrier(barrier);
					// Load nodes routing information
					ret = parseNodesBootstrap(p_nodes);
					parsed = true;
					// Delete barrier object
					ZooKeeperHandler.deleteBarrier(barrier);
				} catch (final ZooKeeperException | KeeperException | InterruptedException e) {
					// Barrier does exist
				}
			}

			if (!parsed) {
				// normal node
				ZooKeeperHandler.waitForBarrier(barrier, this);
				ret = parseNodesNormal(p_nodes);
			}
		} catch (final ZooKeeperException e) {
			throw new DXRAMException("Could not access ZooKeeper", e);
		}

		m_isStarting = false;

		return ret;
	}

	@Override
	public long parseNewNode(final NodesConfigurationEntry p_node, final long[] p_existingNodes) {
		return -1;
	}

	/**
	 * Parses nodes.config and stores routing information in net
	 * @param p_nodes
	 *            the nodes to parse
	 * @return the parsed nodes
	 * @note this method is called by bootstrap only
	 */
	private long[] parseNodesBootstrap(final List<NodesConfigurationEntry> p_nodes) {
		long[] ret;
		short nodeID;
		int numberOfSuperpeers;
		int seed;

		String ownIP;
		int ownPort;

		LOGGER.trace("Entering parseNodesBootstrap");

		ret = new long[NodeID.MAX_ID + 1];
		Arrays.fill(ret, -1);

		try {
			ZooKeeperHandler.create("nodes");

			// Parse node information
			numberOfSuperpeers = 0;
			seed = 1;

			ownIP = Core.getConfiguration().getStringValue(ConfigurationConstants.NETWORK_IP);
			ownPort = Core.getConfiguration().getIntValue(ConfigurationConstants.NETWORK_PORT);

			for (NodesConfigurationEntry entry : p_nodes) {
				nodeID = m_hashGenerator.hash(seed);
				while (m_bloomFilter.contains(nodeID) || nodeID == -1) {
					nodeID = m_hashGenerator.hash(++seed);
				}
				seed++;

				m_bloomFilter.add(nodeID);

				if (ownIP.equals(entry.getIP()) && ownPort == entry.getPort()) {
					// Set own NodeID
					NodeID.setLocalNodeID(nodeID);
					// Set role
					NodeID.setRole(entry.getRole());
					m_bootstrap = nodeID;
				}
				if (entry.getRole().equals(Role.SUPERPEER)) {
					numberOfSuperpeers++;
				}

				ret[nodeID & 0x0000FFFF] = parseNode(entry.getIP(), entry.getPort(), nodeID);
			}

			ZooKeeperHandler.create("nodes/new");
			ZooKeeperHandler.setChildrenWatch("nodes/new", this);

			ZooKeeperHandler.create("nodes/free");
			ZooKeeperHandler.setChildrenWatch("nodes/free", this);

			if (0 == NodeID.getLocalNodeID()) {
				LOGGER.error("Bootstrap is not in nodes.config! Exit now!");
				CoreComponentFactory.closeAll();
				System.exit(-1);
			}

			ZooKeeperHandler.create("nodes/bootstrap", (m_bootstrap + "").getBytes());
			ZooKeeperHandler.create("nodes/superpeers", (numberOfSuperpeers + "").getBytes());

			ZooKeeperHandler.create("nodes/peers");
			// Register superpeer
			ZooKeeperHandler.create("nodes/superpeers/" + NodeID.getLocalNodeID());
		} catch (final ZooKeeperException | KeeperException | InterruptedException e) {
			// TODO: Auf Fehler reagieren
			e.printStackTrace();
		}

		LOGGER.trace("Exiting parseNodesBootstrap");

		return ret;
	}

	/**
	 * Parses nodes.config and stores routing information in net for nodes
	 * @param p_nodes
	 *            the nodes to parse
	 * @return the parsed nodes
	 * @note this method is called by every node except bootstrap
	 */
	private long[] parseNodesNormal(final List<NodesConfigurationEntry> p_nodes) {
		long[] ret;
		short nodeID;
		int seed;
		String node;
		List<String> childs;

		String ownIP;
		int ownPort;

		String[] splits;

		LOGGER.trace("Entering parseNodesNormal");

		NodeID.setRole(DEFAULT_ROLE);

		ret = new long[NodeID.MAX_ID + 1];
		Arrays.fill(ret, -1);

		try {
			// Parse node information
			seed = 1;

			ownIP = Core.getConfiguration().getStringValue(ConfigurationConstants.NETWORK_IP);
			ownPort = Core.getConfiguration().getIntValue(ConfigurationConstants.NETWORK_PORT);

			for (NodesConfigurationEntry entry : p_nodes) {
				nodeID = m_hashGenerator.hash(seed);
				while (m_bloomFilter.contains(nodeID) || nodeID == -1) {
					nodeID = m_hashGenerator.hash(++seed);
				}
				seed++;

				m_bloomFilter.add(nodeID);

				if (ownIP.equals(entry.getIP()) && ownPort == entry.getPort()) {
					// Set own NodeID
					NodeID.setLocalNodeID(nodeID);
					// Set role
					NodeID.setRole(entry.getRole());
				}

				ret[nodeID & 0x0000FFFF] = parseNode(entry.getIP(), entry.getPort(), nodeID);
			}

			m_bootstrap = Short.parseShort(new String(ZooKeeperHandler.getData("nodes/bootstrap")));

			// Apply changes
			childs = ZooKeeperHandler.getChildren("nodes/new");
			for (String child : childs) {
				nodeID = Short.parseShort(child);
				node = new String(ZooKeeperHandler.getData("nodes/new/" + nodeID));
				m_bloomFilter.add(nodeID);

				// Set routing information for that node
				splits = node.split("/");
				ret[nodeID & 0x0000FFFF] = parseNode(splits[0], Integer.parseInt(splits[1]), nodeID);

				if (nodeID == NodeID.getLocalNodeID()) {
					// NodeID was already re-used
					NodeID.setLocalNodeID((short) 0);
				}
			}

			if (NodeID.getLocalNodeID() == 0) {
				// Add this node if it was not in start configuration
				LOGGER.warn("node not in nodes.config (" + ownIP + ", " + ownPort + ")");

				node = ownIP + "/" + ownPort + "/" + "P" + "/" + 0 + "/" + 0;

				childs = ZooKeeperHandler.getChildren("nodes/free");
				if (!childs.isEmpty()) {
					nodeID = Short.parseShort(childs.get(0));
					NodeID.setLocalNodeID(nodeID);
					ZooKeeperHandler.create("nodes/new/" + nodeID, node.getBytes());
					ZooKeeperHandler.delete("nodes/free/" + nodeID);
				} else {
					splits = ownIP.split("\\.");
					seed = ((Integer.parseInt(splits[1]) << 16) + (Integer.parseInt(splits[2]) << 8) + Integer.parseInt(splits[3])) * -1;
					nodeID = m_hashGenerator.hash(seed);
					while (m_bloomFilter.contains(nodeID) || -1 == nodeID) {
						nodeID = m_hashGenerator.hash(--seed);
					}
					m_bloomFilter.add(nodeID);
					// Set own NodeID
					NodeID.setLocalNodeID(nodeID);
					ZooKeeperHandler.create("nodes/new/" + nodeID, node.getBytes());
				}
				// Set routing information for that node
				ret[nodeID & 0x0000FFFF] = parseNode(ownIP, ownPort, nodeID);
			} else {
				// Remove NodeID if this node failed before
				nodeID = NodeID.getLocalNodeID();
				if (ZooKeeperHandler.exists("nodes/free/" + nodeID)) {
					ZooKeeperHandler.delete("nodes/free/" + nodeID);
				}
			}
			// Set watches
			ZooKeeperHandler.setChildrenWatch("nodes/new", this);
			ZooKeeperHandler.setChildrenWatch("nodes/free", this);

			// Register peer/superpeer
			if (NodeID.getRole().equals(Role.SUPERPEER)) {
				ZooKeeperHandler.create("nodes/superpeers/" + NodeID.getLocalNodeID());
			} else if (NodeID.getRole().equals(Role.PEER)) {
				ZooKeeperHandler.create("nodes/peers/" + NodeID.getLocalNodeID());
			}
		} catch (final ZooKeeperException | KeeperException | InterruptedException e) {
			// TODO: Auf Fehler reagieren
			e.printStackTrace();
		}

		LOGGER.trace("Exiting parseNodesNormal");

		return ret;
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
				prefix = ZooKeeperHandler.getPath() + "/";
				while (m_isStarting) {
					try {
						Thread.sleep(100);
					} catch (final InterruptedException e) {}
				}
				if (null != path) {
					if (path.equals(prefix + "nodes/new")) {
						childs = ZooKeeperHandler.getChildren("nodes/new", this);
						for (String child : childs) {
							nodeID = Short.parseShort(child);
							node = new String(ZooKeeperHandler.getData("nodes/new/" + nodeID));
							splits = node.split("/");

							Core.getNodesConfiguration().addNewNode(nodeID, parseNode(splits[0], Integer.parseInt(splits[1]), nodeID));
						}
					}
				}
			} catch (final ZooKeeperException e) {
				LOGGER.error("ERR:Could not access ZooKeeper", e);
			}
		}
	}

}

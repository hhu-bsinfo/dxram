package de.uniduesseldorf.dxram.core.boot;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;
import de.uniduesseldorf.dxram.core.engine.DXRAMEngine;
import de.uniduesseldorf.dxram.core.engine.DXRAMEngineConfigurationValues;

import de.uniduesseldorf.utils.BloomFilter;
import de.uniduesseldorf.utils.CRC16;
import de.uniduesseldorf.utils.Contract;
import de.uniduesseldorf.utils.ZooKeeperHandler;
import de.uniduesseldorf.utils.ZooKeeperHandler.ZooKeeperException;
import sun.security.action.GetLongAction;

public class ZookeeperBootComponent extends BootComponent implements Watcher {

	private final Logger m_logger = Logger.getLogger(ZookeeperBootComponent.class);
	
	// Attributes
	private String m_ownIP = null;
	private int m_ownPort = -1;
	private ZooKeeperHandler m_zookeeper = null;
	private int m_zookeeperBitfieldSize = -1;
	private short m_bootstrap = -1;
	private CRC16 m_hashGenerator = null;
	private BloomFilter m_bloomFilter = null;

	private ArrayList<NodeEntry> m_nodes = null;
	
	private boolean m_isStarting = false;
	
	public ZookeeperBootComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(ZookeeperBootConfigurationValues.Component.PATH);
		p_settings.setDefaultValue(ZookeeperBootConfigurationValues.Component.CONNECTION_STRING);
		p_settings.setDefaultValue(ZookeeperBootConfigurationValues.Component.TIMEOUT);
		p_settings.setDefaultValue(ZookeeperBootConfigurationValues.Component.BITFIELD_SIZE);
	}

	@Override
	protected boolean initComponent(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) {		
		m_ownIP = p_engineSettings.getValue(DXRAMEngineConfigurationValues.IP);
		m_ownPort = p_engineSettings.getValue(DXRAMEngineConfigurationValues.PORT);
		m_zookeeper = new ZooKeeperHandler(p_settings.getValue(ZookeeperBootConfigurationValues.Component.PATH), 
											p_settings.getValue(ZookeeperBootConfigurationValues.Component.CONNECTION_STRING), 
											p_settings.getValue(ZookeeperBootConfigurationValues.Component.TIMEOUT));
		m_isStarting = true;
		m_zookeeperBitfieldSize = p_settings.getValue(ZookeeperBootConfigurationValues.Component.BITFIELD_SIZE);
		
		m_nodes = readNodesFromSettings(p_settings);
		
		if (!parseNodes(m_nodes))
		{
			m_logger.error("Parsing nodes failed.");
			return false;
		}
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		// TODO last node has to set this to true ->
		// get information from superpeer overlay
		m_nodesWatcher.close(false);
		
		return true;
	}

	@Override
	public short getNodeID()
	{
		return m_nodesWatcher.getNodesConfiguration().getOwnNodeID();
	}
	
	@Override
	public NodeRole getNodeRole()
	{
		return m_nodesWatcher.getNodesConfiguration().getOwnNodeEntry().getRole();
	}
	
	@Override
	public InetSocketAddress getNodeAddress(final short p_nodeID) {
		NodeEntry entry = m_nodesWatcher.getNodesConfiguration().getNode(p_nodeID);
		InetSocketAddress address;
		// return "proper" invalid address if entry does not exist
		if (entry == null)
			address = new InetSocketAddress("255.255.255.255", 0xFFFF);
		else
			address = new InetSocketAddress(entry.getIP(), entry.getPort());
		
		return address;
	}
	
	@Override
	public int getNumberOfAvailableSuperpeers()
	{
		return Integer.parseInt(new String(m_nodesWatcher.zookeeperGetData("nodes/superpeers")));
	}
	
	@Override
	public short getNodeIDBootstrap()
	{
		return Short.parseShort(new String(m_nodesWatcher.zookeeperGetData("nodes/bootstrap")));
	}
	
	@Override
	public boolean nodeAvailable(final short p_nodeID)
	{
		return m_nodesWatcher.zookeeperPathExists("nodes/peers/" + p_nodeID);
	}
	
	@Override
	public short setBootstrapPeer(final short p_nodeID)
	{
		short ret;
		Stat status;
		String entry;

		status = m_nodesWatcher.zookeeperGetStatus("nodes/bootstrap");
		entry = new String(m_nodesWatcher.zookeeperGetData("nodes/bootstrap", status));
		ret = Short.parseShort(entry);
		if (ret == m_bootstrap) {
			if (!m_nodesWatcher.zookeeperSetData("nodes/bootstrap", ("" + p_nodeID).getBytes(), status.getVersion())) {
				m_bootstrap = Short.parseShort(new String(m_nodesWatcher.zookeeperGetData("nodes/bootstrap")));
			} else {
				m_bootstrap = p_nodeID;
			}
		} else {
			m_bootstrap = ret;
		}

		return m_bootstrap;
	}
	
	@Override
	public boolean reportNodeFailure(final short p_nodeID, final boolean p_isSuperpeer)
	{
		boolean ret = false;
		Stat status;

		if (m_nodesWatcher.zookeeperPathExists("nodes/peers/" + p_nodeID)) {
			m_nodesWatcher.zookeeperCreate("nodes/free/" + p_nodeID);

			status = m_nodesWatcher.zookeeperGetStatus("nodes/new/" + p_nodeID);
			if (null != status) {
				m_nodesWatcher.zookeeperDelete("nodes/new/" + p_nodeID, status.getVersion());
			}
			if (p_isSuperpeer) {
				status = m_nodesWatcher.zookeeperGetStatus("nodes/superpeers/" + p_nodeID);
				if (null != status) {
					m_nodesWatcher.zookeeperDelete("nodes/superpeers/" + p_nodeID, status.getVersion());
				}
			} else {
				status = m_nodesWatcher.zookeeperGetStatus("nodes/peers/" + p_nodeID);
				if (null != status) {
					m_nodesWatcher.zookeeperDelete("nodes/peers/" + p_nodeID, status.getVersion());
				}
			}

			ret = true;
		}

		return ret;
	}
	
	@Override
	public boolean promoteToSuperpeer()
	{
		// TODO set node Role to superpeer
		return true;
	}
	
	@Override
	public boolean demoteToPeer()
	{
		// TODO set node Role to peer
		//NodeID.setRole(Role.PEER);
		return true;
	}
	
	// -----------------------------------------------------------------------------------
	
	private ArrayList<NodeEntry> readNodesFromSettings(final Settings p_settings)
	{
		ArrayList<NodeEntry> nodes = new ArrayList<NodeEntry>();
		
		Map<Integer, Boolean> nodesEnabled = p_settings.GetValues("Nodes/Node/Enabled", Boolean.class);
		Map<Integer, String> nodesIP = p_settings.GetValues("Nodes/Node/IP", String.class);
		Map<Integer, Integer> nodesPort = p_settings.GetValues("Nodes/Node/Port", Integer.class);
		Map<Integer, String> nodesRole = p_settings.GetValues("Nodes/Node/Role", String.class);
		Map<Integer, Short> nodesRack = p_settings.GetValues("Nodes/Node/Rack", Short.class);
		Map<Integer, Short> nodesSwitch = p_settings.GetValues("Nodes/Node/Switch", Short.class);
		
		for (Entry<Integer, Boolean> entry : nodesEnabled.entrySet())
		{
			String ip = nodesIP.get(entry.getKey());
			if (ip == null)
			{
				m_logger.error("Settings entry for node missing ip.");
				continue;
			}
			
			Integer port = nodesPort.get(entry.getKey());
			if (port == null)
			{
				m_logger.error("Settings entry for node missing port.");
				continue;
			}
			
			String strRole = nodesRole.get(entry.getKey());
			if (strRole == null)
			{
				m_logger.error("Settings entry for node missing role.");
				continue;
			}
			
			Short rack = nodesRack.get(entry.getKey());
			if (rack == null)
			{
				m_logger.error("Settings entry for node missing rack.");
				continue;
			}
			
			Short szwitch = nodesSwitch.get(entry.getKey());
			if (szwitch == null)
			{
				m_logger.error("Settings entry for node missing switch.");
				continue;
			}

			NodeEntry node = new NodeEntry(ip, port, rack, szwitch, NodeRole.toNodeRole(strRole));
			nodes.add(node);
		}	
		
		return nodes;
	}
	
	private void writeNodesToSettings(final Settings p_settings, final ArrayList<NodeEntry> p_nodes)
	{
		// TODO
	}
	
	/**
	 * Parses the configured nodes
	 * @param p_nodes
	 *            the nodes to parse
	 * @return the parsed nodes
	 * @throws DXRAMException
	 *             if the nodes could not be parsed
	 */
	private boolean parseNodes(final ArrayList<NodeEntry> p_nodes) {
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
			m_logger.error("Could not access zookeeper while parsing nodes.", e);
			return false;
		}

		m_isStarting = false;
		return true;
	}
	
	/**
	 * Parses information from a nodes configuration object and creates routing information
	 * in zookeeper. Also assigns valid node IDs and 
	 * @param p_nodes
	 *            the nodes to parse
	 * @return the parsed nodes
	 * @note this method is called by bootstrap only
	 */
	private void parseNodesBootstrap(final ArrayList<NodeEntry> p_nodes) {
		short nodeID;
		int numberOfSuperpeers;
		int seed;

		m_logger.trace("Entering parseNodesBootstrap");

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

		m_logger.trace("Exiting parseNodesBootstrap");
	}

	/**
	 * Parses nodes.config and stores routing information in net for nodes
	 * @param p_nodes
	 *            the nodes to parse
	 * @return the parsed nodes
	 * @note this method is called by every node except bootstrap
	 */
	private void parseNodesNormal(final ArrayList<NodeEntry> p_nodes) {
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
	
	
	private static final class NodeEntry {

		// Attributes
		private String m_ip;
		private int m_port;
		private short m_rack;
		private short m_switch;
		private NodeRole m_role;

		// Constructors
		/**
		 * Creates an instance of NodesConfigurationEntry
		 * @param p_ip
		 *            the ip of the node
		 * @param p_port
		 *            the port of the node
		 * @param p_rack
		 *            the rack of the node
		 * @param p_switch
		 *            the switcharea of the node
		 * @param p_role
		 *            the role of the node
		 */
		public NodeEntry(final String p_ip, final int p_port, final short p_rack, final short p_switch, final NodeRole p_role) {
			Contract.checkNotNull(p_ip, "no IP given");
			Contract.check(p_port > 0 && p_port < 65536, "invalid port given");
			Contract.check(p_rack >= 0, "invalid rack given");
			Contract.check(p_switch >= 0, "invalid switch given");
			Contract.checkNotNull(p_role, "no role given");

			m_ip = p_ip;
			m_port = p_port;
			m_rack = p_rack;
			m_switch = p_switch;
			m_role = p_role;
		}

		// Getter
		/**
		 * Gets the ip of the node
		 * @return the ip of the node
		 */
		public String getIP() {
			return m_ip;
		}

		/**
		 * Gets the port of the node
		 * @return the port of the node
		 */
		public int getPort() {
			return m_port;
		}

		/**
		 * Gets the rack of the node
		 * @return the rack of the node
		 */
		public short getRack() {
			return m_rack;
		}

		/**
		 * Gets the switcharea of the node
		 * @return the switcharea of the noide
		 */
		public short getSwitch() {
			return m_switch;
		}

		/**
		 * Gets the role of the node
		 * @return the role of the noide
		 */
		public NodeRole getRole() {
			return m_role;
		}

		// Methods
		@Override
		public String toString() {
			return "NodesEntry [m_ip=" + m_ip + ", m_port=" + m_port + ", m_rack=" + m_rack + ", m_switch=" + m_switch + ", m_role="
					+ m_role.getAcronym() + "]";
		}

	}
}

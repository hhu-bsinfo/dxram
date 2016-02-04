package de.hhu.bsinfo.dxram.boot;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import de.hhu.bsinfo.dxram.boot.NodesConfiguration.NodeEntry;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMEngineConfigurationValues;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.util.NodeID;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.BloomFilter;
import de.hhu.bsinfo.utils.CRC16;
import de.hhu.bsinfo.utils.ZooKeeperHandler;
import de.hhu.bsinfo.utils.ZooKeeperHandler.ZooKeeperException;

/**
 * Implementation of the BootComponent interface with zookeeper.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class ZookeeperBootComponent extends BootComponent implements Watcher {

	// Attributes
	private String m_ownIP = null;
	private int m_ownPort = -1;
	private ZooKeeperHandler m_zookeeper = null;
	private int m_zookeeperBitfieldSize = -1;
	private short m_bootstrap = -1;
	private CRC16 m_hashGenerator = null;
	private BloomFilter m_bloomFilter = null;

	private NodesConfiguration m_nodesConfiguration = null;
	
	private boolean m_isStarting = false;
	
	private LoggerComponent m_logger = null;
	
	/**
	 * Constructor
	 * @param p_priorityInit Priority for initialization of this component. 
	 * 			When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown Priority for shutting down this component. 
	 * 			When choosing the order, consider component dependencies here.
	 */
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
		m_logger = getDependentComponent(LoggerComponent.class);
		
		m_ownIP = p_engineSettings.getValue(DXRAMEngineConfigurationValues.IP);
		m_ownPort = p_engineSettings.getValue(DXRAMEngineConfigurationValues.PORT);
		m_zookeeper = new ZooKeeperHandler(p_settings.getValue(ZookeeperBootConfigurationValues.Component.PATH), 
											p_settings.getValue(ZookeeperBootConfigurationValues.Component.CONNECTION_STRING), 
											p_settings.getValue(ZookeeperBootConfigurationValues.Component.TIMEOUT));
		m_isStarting = true;
		m_zookeeperBitfieldSize = p_settings.getValue(ZookeeperBootConfigurationValues.Component.BITFIELD_SIZE);
		
		m_nodesConfiguration = new NodesConfiguration();
		ArrayList<NodeEntry> m_nodes = readNodesFromSettings(p_settings);
		
		if (!parseNodes(m_nodes))
		{
			m_logger.error(this.getClass(), "Parsing nodes failed.");
			return false;
		}
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		// TODO last node has to set this to true ->
		// get information from superpeer overlay
		try {
			m_zookeeper.close(false);
		} catch (ZooKeeperException e) {
			m_logger.error(this.getClass(), "Closing zookeeper failed.", e);
		}
		
		return true;
	}

	@Override
	public short getNodeID()
	{
		return m_nodesConfiguration.getOwnNodeID();
	}
	
	@Override
	public NodeRole getNodeRole()
	{
		return m_nodesConfiguration.getOwnNodeEntry().getRole();
	}
	
	@Override
	public NodeRole getNodeRole(final short p_nodeID)
	{
		NodeEntry entry = m_nodesConfiguration.getNode(p_nodeID);
		if (entry == null)
			return null;
		
		return entry.getRole();
	}
	
	@Override
	public InetSocketAddress getNodeAddress(final short p_nodeID) {
		NodeEntry entry = m_nodesConfiguration.getNode(p_nodeID);
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
		// if bootstrap is not available (wrong startup order of superpeers and peers)
		byte[] data = zookeeperGetData("nodes/superpeers");
		if (data != null) {
			return Integer.parseInt(new String(data));
		} else {
			return 0;
		}
	}
	
	@Override
	public short getNodeIDBootstrap()
	{
		// if bootstrap is not available (wrong startup order of superpeers and peers)
		byte[] data = zookeeperGetData("nodes/bootstrap");
		if (data != null) {
			return Short.parseShort(new String(data));
		} else {
			return NodeID.INVALID_ID;
		}
	}
	
	@Override
	public boolean nodeAvailable(final short p_nodeID)
	{
		return zookeeperPathExists("nodes/peers/" + p_nodeID);
	}
	
	@Override
	public short setBootstrapPeer(final short p_nodeID)
	{
		short ret;
		Stat status;
		String entry;

		status = zookeeperGetStatus("nodes/bootstrap");
		entry = new String(zookeeperGetData("nodes/bootstrap", status));
		ret = Short.parseShort(entry);
		if (ret == m_bootstrap) {
			if (!zookeeperSetData("nodes/bootstrap", ("" + p_nodeID).getBytes(), status.getVersion())) {
				m_bootstrap = Short.parseShort(new String(zookeeperGetData("nodes/bootstrap")));
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

		if (zookeeperPathExists("nodes/peers/" + p_nodeID)) {
			zookeeperCreate("nodes/free/" + p_nodeID);

			status = zookeeperGetStatus("nodes/new/" + p_nodeID);
			if (null != status) {
				zookeeperDelete("nodes/new/" + p_nodeID, status.getVersion());
			}
			if (p_isSuperpeer) {
				status = zookeeperGetStatus("nodes/superpeers/" + p_nodeID);
				if (null != status) {
					zookeeperDelete("nodes/superpeers/" + p_nodeID, status.getVersion());
				}
			} else {
				status = zookeeperGetStatus("nodes/peers/" + p_nodeID);
				if (null != status) {
					zookeeperDelete("nodes/peers/" + p_nodeID, status.getVersion());
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
			m_logger.error(this.getClass(), "ERR:ZooKeeper state expired");
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
				m_logger.error(this.getClass(), "ERR:Could not access ZooKeeper", e);
			}
		}
	}
	
	// -----------------------------------------------------------------------------------
	
	/**
	 * Read the nodes list from the settings instance.
	 * @param p_settings Settings instance of the component.
	 * @return List of node entries read from the settings.
	 */
	private ArrayList<NodeEntry> readNodesFromSettings(final Settings p_settings)
	{
		ArrayList<NodeEntry> nodes = new ArrayList<NodeEntry>();
		
		Map<Integer, Boolean> nodesEnabled = p_settings.getValues("Nodes/Node/Enabled", Boolean.class);
		Map<Integer, String> nodesIP = p_settings.getValues("Nodes/Node/IP", String.class);
		Map<Integer, Integer> nodesPort = p_settings.getValues("Nodes/Node/Port", Integer.class);
		Map<Integer, String> nodesRole = p_settings.getValues("Nodes/Node/Role", String.class);
		Map<Integer, Short> nodesRack = p_settings.getValues("Nodes/Node/Rack", Short.class);
		Map<Integer, Short> nodesSwitch = p_settings.getValues("Nodes/Node/Switch", Short.class);
		
		for (Entry<Integer, Boolean> entry : nodesEnabled.entrySet())
		{
			String ip = nodesIP.get(entry.getKey());
			if (ip == null)
			{
				m_logger.error(this.getClass(), "Settings entry for node missing ip.");
				continue;
			}
			
			Integer port = nodesPort.get(entry.getKey());
			if (port == null)
			{
				m_logger.error(this.getClass(), "Settings entry for node missing port.");
				continue;
			}
			
			String strRole = nodesRole.get(entry.getKey());
			if (strRole == null)
			{
				m_logger.error(this.getClass(), "Settings entry for node missing role.");
				continue;
			}
			
			Short rack = nodesRack.get(entry.getKey());
			if (rack == null)
			{
				m_logger.error(this.getClass(), "Settings entry for node missing rack.");
				continue;
			}
			
			Short szwitch = nodesSwitch.get(entry.getKey());
			if (szwitch == null)
			{
				m_logger.error(this.getClass(), "Settings entry for node missing switch.");
				continue;
			}

			NodeEntry node = new NodeEntry(ip, port, rack, szwitch, NodeRole.toNodeRole(strRole));
			nodes.add(node);
		}	
		
		return nodes;
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
			if (!m_zookeeper.exists("nodes/bootstrap")) {
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
			m_logger.error(this.getClass(), "Could not access zookeeper while parsing nodes.", e);
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

		m_logger.trace(this.getClass(), "Entering parseNodesBootstrap");

		try {
			if (!m_zookeeper.exists("nodes")) {
				m_zookeeper.create("nodes");
			}
			
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
					m_logger.info(this.getClass(), "Own node assigned: " + entry);
				}
				if (entry.getRole().equals(NodeRole.SUPERPEER)) {
					numberOfSuperpeers++;
				}

				m_nodesConfiguration.addNode((short) (nodeID & 0x0000FFFF), entry);
				m_logger.info(this.getClass(), "Node added: " + entry);
			}

			if (!m_zookeeper.exists("nodes/new")) {
				m_zookeeper.create("nodes/new");
			}
			m_zookeeper.setChildrenWatch("nodes/new", this);

			if (!m_zookeeper.exists("nodes/free")) {
				m_zookeeper.create("nodes/free");
			}
			m_zookeeper.setChildrenWatch("nodes/free", this);

			// check if own node entry was correctly assigned to a valid node ID
			if (m_nodesConfiguration.getOwnNodeEntry() == null) {
				throw new BootRuntimeException("Bootstrap entry for node in nodes configuration missing!");
			}

			// set default/invalid data
			if (!m_zookeeper.exists("nodes/peers")) {
				m_zookeeper.create("nodes/peers");
			}
			if (!m_zookeeper.exists("nodes/superpeers")) {
				m_zookeeper.create("nodes/superpeers", (numberOfSuperpeers + "").getBytes());
			} else {
				m_zookeeper.setData("nodes/superpeers", (numberOfSuperpeers + "").getBytes());
			}
			
			// Register superpeer
			// register only if we are the superpeer. don't add peer as superpeer
			if (m_nodesConfiguration.getOwnNodeEntry().getRole().equals(NodeRole.SUPERPEER)) {
				m_zookeeper.create("nodes/bootstrap", (m_bootstrap + "").getBytes());
				m_zookeeper.create("nodes/superpeers/" + m_nodesConfiguration.getOwnNodeID());
			}
		} catch (final ZooKeeperException | KeeperException | InterruptedException e) {
			throw new BootRuntimeException("Parsing nodes bootstrap failed.", e);
		}

		m_logger.trace(this.getClass(), "Exiting parseNodesBootstrap");
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

		m_logger.trace(this.getClass(), "Entering parseNodesNormal");


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
					m_logger.info(this.getClass(), "Own node assigned: " + entry);
				}

				m_nodesConfiguration.addNode((short) (nodeID & 0x0000FFFF), entry);
				m_logger.info(this.getClass(), "Node added: " + entry);
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
				m_logger.warn(this.getClass(), "node not in nodes.config (" + m_ownIP + ", " + m_ownPort + ")");

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
			throw new BootRuntimeException("Parsing nodes normal failed.", e);
		}

		m_logger.trace(this.getClass(), "Exiting parseNodesNormal");
	}
	
	private void zookeeperCreate(final String p_path) {
		try {
			m_zookeeper.create(p_path);
		} catch (ZooKeeperException | KeeperException | InterruptedException e) {
			m_logger.error(this.getClass(), "Creating path in zookeeper failed.", e);
		}
	}
	
	private Stat zookeeperGetStatus(final String p_path) {
		Stat status = null;
		
		try {
			status = m_zookeeper.getStatus(p_path);
		} catch (ZooKeeperException e) {
			m_logger.error(this.getClass(), "Getting status from zookeeper failed.", e);
		}
		
		return status;
	}
	
	private void zookeeperDelete(final String p_path, final int p_version) {
		try {
			m_zookeeper.delete(p_path, p_version);
		} catch (ZooKeeperException e) {
			m_logger.error(this.getClass(), "Deleting path from zookeeper failed.", e);
		}
	}
	
	private byte[] zookeeperGetData(final String p_path)
	{
		byte[] data = null;
		
		try {
			data = m_zookeeper.getData(p_path);
		} catch (ZooKeeperException e) {
			m_logger.error(this.getClass(), "Getting data from zookeeper failed.", e);
		}
		
		return data;
	}
	
	private byte[] zookeeperGetData(final String p_path, Stat p_status)
	{
		byte[] data = null;
		
		try {
			data = m_zookeeper.getData(p_path, p_status);
		} catch (ZooKeeperException e) {
			m_logger.error(this.getClass(), "Getting data from zookeeper failed.", e);
		}
		
		return data;
	}
	
	private boolean zookeeperSetData(final String p_path, final byte[] p_data, final int p_version)
	{
		try {
			m_zookeeper.setData(p_path, p_data, p_version);
			return true;
		} catch (ZooKeeperException e) {
			m_logger.error(this.getClass(), "Setting data on zookeeper failed.", e);
			return false;
		}
	}
	
	private boolean zookeeperPathExists(final String p_path) {
		boolean ret = false;
		
		try {
			ret = m_zookeeper.exists(p_path);
		} catch (ZooKeeperException e) {
			m_logger.error(this.getClass(), "Checking if path exists in zookeeper failed.", e);
		}
		
		return ret;
	}
}

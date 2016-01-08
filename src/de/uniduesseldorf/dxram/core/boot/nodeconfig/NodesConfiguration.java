
package de.uniduesseldorf.dxram.core.engine.nodeconfig;

import java.util.HashMap;
import java.util.Map.Entry;

import de.uniduesseldorf.dxram.core.boot.NodeRole;

import de.uniduesseldorf.utils.Contract;

/**
 * Represents a nodes configuration for DXRAM. This also holds any information
 * about the current node as well as any remote nodes available in the system.
 * @author Florian Klein
 *         03.09.2013
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 9.12.15
 */
public final class NodesConfiguration {

	public static final int MAX_NODE_ID = 65535;
	public static final short INVALID_NODE_ID = -1;
	
	// Attributes
	private HashMap<Short, NodeEntry> m_nodes;
	
	private short m_ownID;

	// Constructors
	/**
	 * Creates an instance of NodesConfiguration
	 */
	public NodesConfiguration() {
		m_nodes = new HashMap<Short, NodeEntry>();
		m_ownID = INVALID_NODE_ID;
	}

	// Getters
	/**
	 * Gets the configured node
	 * @return the configured nodes
	 */
	public HashMap<Short, NodeEntry> getNodes() {
		return m_nodes;
	}
	
	/**
	 * Get the NodeEntry of the specified node ID.
	 * @param p_nodeID Node ID to get the entry of.
	 * @return NodeEntry containing information about the node or null if it does not exist.
	 */
	public NodeEntry getNode(final short p_nodeID)
	{
		return m_nodes.get(p_nodeID);
	}
	
	/**
	 * Get the node ID which is set for this node.
	 * @return Own node ID (or -1 if invalid).
	 */
	public short getOwnNodeID()
	{
		return m_ownID;
	}
	
	/**
	 * Get the NodeEntry corresponding to our node ID.
	 * @return NodeEntry or null if invalid.
	 */
	public NodeEntry getOwnNodeEntry()
	{
		return m_nodes.get(m_ownID);
	}

	// ---------------------------------------------------------------------------

	/**
	 * Adds a node
	 * @param p_entry
	 *            the configured node
	 */
	synchronized void addNode(final short p_nodeID, final NodeEntry p_entry) {
		m_nodes.put(p_nodeID, p_entry);
	}
	
	/**
	 * Remove a node from the mappings list.
	 * @param p_nodeID Node ID of the entry to remove.
	 */
	synchronized void removeNode(final short p_nodeID) {
		m_nodes.remove(p_nodeID);
	}
	
	/**
	 * Set the node ID for the current/own node.
	 * @param p_nodeID
	 */
	synchronized void setOwnNodeID(final short p_nodeID) {
		m_ownID = p_nodeID;
	}
	
	@Override
	public String toString() {
		String str = new String();
		
		str += "NodesConfiguration[ownID: " + m_ownID + "]:";
		for (Entry<Short, NodeEntry> entry : m_nodes.entrySet()) {
			str += "\n" + entry.getKey() + ": " + entry.getValue();
		}
		
		return str;
	}

	// Classes
	/**
	 * Describes a nodes configuration entry
	 * @author Florian Klein
	 *         03.09.2013
	 */
	public static final class NodeEntry {

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
			return "NodesConfigurationEntry [m_ip=" + m_ip + ", m_port=" + m_port + ", m_rack=" + m_rack + ", m_switch=" + m_switch + ", m_role="
					+ m_role.getAcronym() + "]";
		}

	}
}

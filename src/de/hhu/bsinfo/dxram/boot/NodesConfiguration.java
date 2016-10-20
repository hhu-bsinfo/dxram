
package de.hhu.bsinfo.dxram.boot;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Represents a nodes configuration for DXRAM. This also holds any information
 * about the current node as well as any remote nodes available in the system.
 *
 * @author Florian Klein
 *         03.09.2013
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 9.12.15
 */
public final class NodesConfiguration {

	public static final int MAX_NODE_ID = 65535;
	public static final short INVALID_NODE_ID = -1;

	private NodeEntry[] m_nodes = new NodeEntry[MAX_NODE_ID + 1];
	private short m_ownID = INVALID_NODE_ID;

	/**
	 * Creates an instance of NodesConfiguration
	 */
	public NodesConfiguration() {

	}

	/**
	 * Gets the configured node
	 *
	 * @return the configured nodes
	 */
	public NodeEntry[] getNodes() {
		return m_nodes;
	}

	/**
	 * Get the NodeEntry of the specified node ID.
	 *
	 * @param p_nodeID Node ID to get the entry of.
	 * @return NodeEntry containing information about the node or null if it does not exist.
	 */
	public NodeEntry getNode(final short p_nodeID) {
		return m_nodes[p_nodeID & 0xFFFF];
	}

	/**
	 * Get the node ID which is set for this node.
	 *
	 * @return Own node ID (or -1 if invalid).
	 */
	public short getOwnNodeID() {
		return m_ownID;
	}

	/**
	 * Get the NodeEntry corresponding to our node ID.
	 *
	 * @return NodeEntry or null if invalid.
	 */
	public NodeEntry getOwnNodeEntry() {
		return m_nodes[m_ownID & 0xFFFF];
	}

	// ---------------------------------------------------------------------------

	/**
	 * Adds a node
	 *
	 * @param p_nodeID Id of the node.
	 * @param p_entry  the configured node
	 */
	synchronized void addNode(final short p_nodeID, final NodeEntry p_entry) {
		m_nodes[p_nodeID & 0xFFFF] = p_entry;
	}

	/**
	 * Remove a node from the mappings list.
	 *
	 * @param p_nodeID Node ID of the entry to remove.
	 */
	synchronized void removeNode(final short p_nodeID) {
		m_nodes[p_nodeID & 0xFFFF] = null;
	}

	/**
	 * Set the node ID for the current/own node.
	 *
	 * @param p_nodeID Node id to set.
	 */
	synchronized void setOwnNodeID(final short p_nodeID) {
		m_ownID = p_nodeID;
	}

	@Override
	public String toString() {
		String str = "";

		str += "NodesConfiguration[ownID: " + m_ownID + "]:";
		for (int i = 0; i < m_nodes.length; i++) {
			if (m_nodes[i] != null) {
				str += "\n" + NodeID.toHexString((short) i) + ": " + m_nodes[i];
			}
		}

		return str;
	}

	/**
	 * Describes a nodes configuration entry
	 *
	 * @author Florian Klein
	 *         03.09.2013
	 */
	static final class NodeEntry {

		// configuration values
		@Expose
		private String m_ip = "127.0.0.1";
		@Expose
		private int m_port = 22222;
		@Expose
		private NodeRole m_role = NodeRole.PEER;
		@Expose
		private short m_rack = 0;
		@Expose
		private short m_switch = 0;

		// private state
		private byte m_readFromFile;

		/**
		 * Creates an instance of NodesConfigurationEntry
		 *
		 * @param p_ip           the ip of the node
		 * @param p_port         the port of the node
		 * @param p_rack         the rack of the node
		 * @param p_switch       the switcharea of the node
		 * @param p_role         the role of the node
		 * @param p_readFromFile whether this node's information was read from nodes file or not
		 */
		NodeEntry(final String p_ip, final int p_port, final short p_rack, final short p_switch,
				final NodeRole p_role, final boolean p_readFromFile) {
			assert p_ip != null;
			assert p_port > 0 && p_port < 65536;
			assert p_rack >= 0;
			assert p_switch >= 0;
			assert p_role != null;

			m_ip = p_ip;
			m_port = p_port;
			m_rack = p_rack;
			m_switch = p_switch;
			m_role = p_role;
			m_readFromFile = p_readFromFile ? (byte) 1 : (byte) 0;
		}

		/**
		 * Gets the ip of the node
		 *
		 * @return the ip of the node
		 */
		public String getIP() {
			return m_ip;
		}

		/**
		 * Gets the port of the node
		 *
		 * @return the port of the node
		 */
		public int getPort() {
			return m_port;
		}

		/**
		 * Gets the rack of the node
		 *
		 * @return the rack of the node
		 */
		public short getRack() {
			return m_rack;
		}

		/**
		 * Gets the switcharea of the node
		 *
		 * @return the switcharea of the node
		 */
		public short getSwitch() {
			return m_switch;
		}

		/**
		 * Gets the role of the node
		 *
		 * @return the role of the node
		 */
		public NodeRole getRole() {
			return m_role;
		}

		/**
		 * Gets the source of the node's information
		 *
		 * @return whether this node's information was read from nodes file or not
		 */
		public boolean readFromFile() {
			return m_readFromFile == 1;
		}

		@Override
		public String toString() {
			return "NodesConfigurationEntry [m_ip=" + m_ip + ", m_port=" + m_port + ", m_rack=" + m_rack + ", m_switch="
					+ m_switch + ", m_role=" + m_role.getAcronym() + (m_readFromFile == 1 ? "true" : "false") + "]";
		}

	}
}

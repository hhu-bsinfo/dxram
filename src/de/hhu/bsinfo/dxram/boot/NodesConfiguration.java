
package de.hhu.bsinfo.dxram.boot;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.unit.IPV4Unit;

/**
 * Represents a nodes configuration for DXRAM. This also holds any information
 * about the current node as well as any remote nodes available in the system.
 * @author Florian Klein
 *         03.09.2013
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 9.12.15
 */
public final class NodesConfiguration {

	private NodeEntry[] m_nodes = new NodeEntry[NodeID.MAX_ID + 1];
	private short m_ownID = NodeID.INVALID_ID;

	/**
	 * Creates an instance of NodesConfiguration
	 */
	public NodesConfiguration() {

	}

	/**
	 * Gets the configured node
	 * @return the configured nodes
	 */
	public NodeEntry[] getNodes() {
		return m_nodes;
	}

	/**
	 * Get the NodeEntry of the specified node ID.
	 * @param p_nodeID
	 *            Node ID to get the entry of.
	 * @return NodeEntry containing information about the node or null if it does not exist.
	 */
	public NodeEntry getNode(final short p_nodeID) {
		return m_nodes[p_nodeID & 0xFFFF];
	}

	/**
	 * Get the node ID which is set for this node.
	 * @return Own node ID (or -1 if invalid).
	 */
	public short getOwnNodeID() {
		return m_ownID;
	}

	/**
	 * Get the NodeEntry corresponding to our node ID.
	 * @return NodeEntry or null if invalid.
	 */
	public NodeEntry getOwnNodeEntry() {
		return m_nodes[m_ownID & 0xFFFF];
	}

	// ---------------------------------------------------------------------------

	/**
	 * Adds a node
	 * @param p_nodeID
	 *            Id of the node.
	 * @param p_entry
	 *            the configured node
	 */
	synchronized void addNode(final short p_nodeID, final NodeEntry p_entry) {
		m_nodes[p_nodeID & 0xFFFF] = p_entry;
	}

	/**
	 * Remove a node from the mappings list.
	 * @param p_nodeID
	 *            Node ID of the entry to remove.
	 */
	synchronized void removeNode(final short p_nodeID) {
		m_nodes[p_nodeID & 0xFFFF] = null;
	}

	/**
	 * Set the node ID for the current/own node.
	 * @param p_nodeID
	 *            Node id to set.
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
	 * @author Florian Klein
	 *         03.09.2013
	 */
	static final class NodeEntry {

		// configuration values
		@Expose
		private IPV4Unit m_address = new IPV4Unit("127.0.0.1", 22222);
		@Expose
		private NodeRole m_role = NodeRole.PEER;
		@Expose
		private short m_rack = 0;
		@Expose
		private short m_switch = 0;
		@Expose
		private byte m_readFromFile = 1;

		/**
		 * Creates an instance of NodesConfigurationEntry
		 */
		NodeEntry() {}

		/**
		 * Creates an instance of NodesConfigurationEntry
		 * @param p_address
		 *            addres of the node
		 * @param p_rack
		 *            the rack of the node
		 * @param p_switch
		 *            the switcharea of the node
		 * @param p_role
		 *            the role of the node
		 * @param p_readFromFile
		 *            whether this node's information was read from nodes file or not
		 */
		NodeEntry(final IPV4Unit p_address, final short p_rack, final short p_switch,
				final NodeRole p_role, final boolean p_readFromFile) {
			assert p_rack >= 0;
			assert p_switch >= 0;
			assert p_role != null;

			m_address = p_address;
			m_rack = p_rack;
			m_switch = p_switch;
			m_role = p_role;
			m_readFromFile = p_readFromFile ? (byte) 1 : (byte) 0;
		}

		/**
		 * Gets the address of the node
		 * @return the address of the node
		 */
		public IPV4Unit getAddress() {
			return m_address;
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
		 * @return the switcharea of the node
		 */
		public short getSwitch() {
			return m_switch;
		}

		/**
		 * Gets the role of the node
		 * @return the role of the node
		 */
		public NodeRole getRole() {
			return m_role;
		}

		/**
		 * Gets the source of the node's information
		 * @return whether this node's information was read from nodes file or not
		 */
		public boolean readFromFile() {
			return m_readFromFile == 1;
		}

		@Override
		public String toString() {
			return "NodesConfigurationEntry [m_address=" + m_address + ", m_rack=" + m_rack + ", m_switch="
					+ m_switch + ", m_role=" + m_role.getAcronym() + ", m_readFromFile="
					+ (m_readFromFile == 1 ? "true" : "false") + "]";
		}

	}
}


package de.uniduesseldorf.dxram.core.api.config;

import java.util.ArrayList;
import java.util.List;

import de.uniduesseldorf.utils.Contract;

/**
 * Represents a nodes configuration for DXRAM
 * @author Florian Klein
 *         03.09.2013
 */
public final class NodesConfiguration {

	// Attributes
	private List<NodesConfigurationEntry> m_nodes;

	// Constructors
	/**
	 * Creates an instance of NodesConfiguration
	 */
	NodesConfiguration() {
		m_nodes = new ArrayList<>();
	}

	// Getters
	/**
	 * Gets the configured node
	 * @return the configured nodes
	 */
	List<NodesConfigurationEntry> getNodes() {
		return m_nodes;
	}

	// Methods
	/**
	 * Adds a node
	 * @param p_entry
	 *            the configured node
	 */
	public synchronized void addNode(final NodesConfigurationEntry p_entry) {
		m_nodes.add(p_entry);
	}

	// Classes
	/**
	 * Describes a nodes configuration entry
	 * @author Florian Klein
	 *         03.09.2013
	 */
	public static final class NodesConfigurationEntry {

		// Attributes
		private String m_ip;
		private int m_port;
		private short m_rack;
		private short m_switch;
		private Role m_role;

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
		public NodesConfigurationEntry(final String p_ip, final int p_port, final short p_rack, final short p_switch, final Role p_role) {
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
		public Role getRole() {
			return m_role;
		}

		// Methods
		@Override
		public String toString() {
			return "NodesConfigurationEntry [m_ip=" + m_ip + ", m_port=" + m_port + ", m_rack=" + m_rack + ", m_switch=" + m_switch + ", m_role="
					+ m_role.getAcronym() + "]";
		}

	}

	/**
	 * Represents the node roles
	 * @author Florian Klein
	 *         28.11.2013
	 */
	public enum Role {

		// Constants
		PEER('P'), SUPERPEER('S'), MONITOR('M');

		// Attributes
		private char m_acronym;

		// Constructors
		/**
		 * Creates an instance of Role
		 * @param p_acronym
		 *            the role's acronym
		 */
		Role(final char p_acronym) {
			m_acronym = p_acronym;
		}

		// Getters
		/**
		 * Gets the acronym of the role
		 * @return the acronym
		 */
		public char getAcronym() {
			return m_acronym;
		}

		// Methods
		/**
		 * Gets the role for the given acronym
		 * @param p_acronym
		 *            the acronym
		 * @return the corresponding role
		 */
		public static Role getRoleByAcronym(final char p_acronym) {
			Role ret = null;

			for (Role role : values()) {
				if (role.m_acronym == p_acronym) {
					ret = role;

					break;
				}
			}

			return ret;
		}

	}

}

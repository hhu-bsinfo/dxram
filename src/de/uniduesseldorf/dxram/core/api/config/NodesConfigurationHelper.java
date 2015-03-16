
package de.uniduesseldorf.dxram.core.api.config;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.NodesConfigurationEntry;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Simplifies the access to the DXRAM nodes configuration
 * @author Florian Klein 03.09.2013
 */
public final class NodesConfigurationHelper {

	// Attributes

	private AbstractNodesConfigurationParser m_parser;

	private long[] m_nodesArray;

	// Constructors
	/**
	 * Creates an instance of NodesConfigurationHelper
	 * @param p_configuration
	 *            the nodes configuration
	 * @throws DXRAMException
	 *             if the nodes could not be parsed
	 */
	public NodesConfigurationHelper(final NodesConfiguration p_configuration) throws DXRAMException {
		m_parser = new DefaultNodesConfigurationParser();

		m_nodesArray = m_parser.parseNodes(p_configuration);
	}

	// Methods
	/**
	 * Gets the address for the node of the given nodeID
	 * @param p_nodeID
	 *            the nodeID
	 * @return the address for the node of the given nodeID
	 */
	public InetSocketAddress getAddress(final short p_nodeID) {
		InetSocketAddress ret;

		ret = new InetSocketAddress(getHost(p_nodeID), getPort(p_nodeID));

		return ret;
	}

	/**
	 * Gets the host for the node of the given nodeID
	 * @param p_nodeID
	 *            the nodeID
	 * @return the host for the node of the given nodeID
	 */
	public String getHost(final short p_nodeID) {
		String ret;
		long value;

		value = m_nodesArray[p_nodeID & 0x0000FFFF] & 0x0000FFFFFFFFFFFFL;
		ret =
				((value & 0xFF0000000000L) >> 40) + "." + ((value & 0x00FF00000000L) >> 32) + "."
						+ ((value & 0x0000FF000000L) >> 24) + "." + ((value & 0x000000FF0000L) >> 16);

		return ret;
	}

	/**
	 * Gets the port for the node of the given nodeID
	 * @param p_nodeID
	 *            the nodeID
	 * @return the port for the node of the given nodeID
	 */
	public int getPort(final short p_nodeID) {
		int ret;

		ret = (int)(m_nodesArray[p_nodeID & 0x0000FFFF] & 0xFFFF);

		return ret;
	}

	/**
	 * Parses and adds a new node
	 * @param p_node
	 *            the new node
	 * @throws DXRAMException
	 *             if the nodes could not be parsed
	 */
	public void addNewNode(final NodesConfigurationEntry p_node) throws DXRAMException {
		int nodeID;
		long value;

		value = m_parser.parseNewNode(p_node, m_nodesArray);
		nodeID = (int)(value & 0xFFFF000000000000L >> 48);

		if (nodeID >= 0) {
			m_nodesArray[nodeID & 0x0000FFFF] = value;
		}
	}

	/**
	 * Adds a new node
	 * @param p_nodeID
	 *            the nodeID
	 * @param p_value
	 *            the parsed value
	 */
	public void addNewNode(final short p_nodeID, final long p_value) {
		m_nodesArray[p_nodeID & 0x0000FFFF] = p_value;
	}

	/**
	 * Gets a copy of the array with the parsed nodes
	 * @return a copy of the array with the parsed nodes
	 */
	public long[] getParsedNodes() {
		return Arrays.copyOf(m_nodesArray, m_nodesArray.length);
	}

	/**
	 * Gets a list of all nodeIDs
	 * @return a list of all nodeIDs
	 */
	public List<Short> getAlleNodeIDs() {
		List<Short> nodes;

		nodes = new ArrayList<Short>();
		for (int i = 0;i <= NodeID.MAX_ID;i++) {
			if (-1 != m_nodesArray[i]) {
				nodes.add((short)i);
			}
		}

		return nodes;
	}

}

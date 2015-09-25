
package de.uniduesseldorf.dxram.core.api.config;

import java.util.List;

import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.NodesConfigurationEntry;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Parses the nodes configuration
 * @author Florian Klein 03.09.2013
 */
abstract class AbstractNodesConfigurationParser {

	// Methods
	/**
	 * Parses the nodes configuration
	 * @param p_configuration
	 *            the nodes configuration
	 * @return the parsed nodes
	 * @throws DXRAMException
	 *             if the nodes could not be parsed
	 */
	abstract long[] parseNodes(NodesConfiguration p_configuration) throws DXRAMException;

	/**
	 * Parses the configured nodes
	 * @param p_nodes
	 *            the nodes to parse
	 * @return the parsed nodes
	 * @throws DXRAMException
	 *             if the nodes could not be parsed
	 */
	abstract long[] parseNodes(List<NodesConfigurationEntry> p_nodes) throws DXRAMException;

	/**
	 * Parses a node
	 * @param p_node
	 *            the node to parse
	 * @param p_existingNodes
	 *            the existing parsed nodes
	 * @return the parsed node
	 * @throws DXRAMException
	 *             if the node could not be parsed
	 */
	abstract long parseNewNode(NodesConfigurationEntry p_node, long[] p_existingNodes) throws DXRAMException;

	/**
	 * Parses a node
	 * @param p_ip
	 *            the ip
	 * @param p_port
	 *            the port
	 * @param p_nodeID
	 *            the NodeID
	 * @return the parsed node
	 */
	protected final long parseNode(final String p_ip, final int p_port, final long p_nodeID) {
		long ret;
		String[] ipParts;

		ipParts = p_ip.split("\\.");

		ret = p_nodeID << 48 & 0xFFFF000000000000L;
		ret += Long.parseLong(ipParts[0]) << 40 & 0x0000FF0000000000L;
		ret += Long.parseLong(ipParts[1]) << 32 & 0x000000FF00000000L;
		ret += Long.parseLong(ipParts[2]) << 24 & 0x00000000FF000000L;
		ret += Long.parseLong(ipParts[3]) << 16 & 0x0000000000FF0000L;
		ret += p_port & 0x000000000000FFFFL;

		return ret;
	}

}

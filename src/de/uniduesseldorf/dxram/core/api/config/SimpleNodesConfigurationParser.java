
package de.uniduesseldorf.dxram.core.api.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.NodesConfigurationEntry;
import de.uniduesseldorf.dxram.core.util.NodeID;

/**
 * Parses the configured nodes
 * @author Florian Klein
 *         03.09.2013
 */
public final class SimpleNodesConfigurationParser extends AbstractNodesConfigurationParser {

	// Constructors
	/**
	 * Creates an instance of SimpleNodesConfigurationParser
	 */
	public SimpleNodesConfigurationParser() {}

	// Methods
	@Override
	public long[] parseNodes(final NodesConfiguration p_configuration) {
		return parseNodes(p_configuration.getNodes());
	}

	@Override
	public long[] parseNodes(final List<NodesConfigurationEntry> p_nodes) {
		long[] ret;
		int nodeID;
		short currentSwitch;
		short currentRack;

		ret = new long[NodeID.MAX_ID + 1];
		Arrays.fill(ret, -1);

		Collections.sort(p_nodes, new DefaultNodesConfigurationComparator());

		nodeID = -1;
		currentSwitch = -1;
		currentRack = -1;
		for (NodesConfigurationEntry entry : p_nodes) {
			if (entry.getSwitch() != currentSwitch || entry.getRack() != currentRack) {
				currentSwitch = entry.getSwitch();
				currentRack = entry.getRack();
				nodeID = (currentSwitch << 12 & 0x0000F000) + (currentRack << 6 & 0x00000FC0);
			} else {
				nodeID++;
			}

			ret[nodeID & 0x0000FFFF] = parseNode(entry.getIP(), entry.getPort(), nodeID);
		}

		return ret;
	}

	@Override
	public long parseNewNode(final NodesConfigurationEntry p_node, final long[] p_existingNodes) {
		long ret;
		int nodeID;

		nodeID = (p_node.getSwitch() << 12 & 0x0000F000) + (p_node.getRack() << 6 & 0x00000FC0);
		while (p_existingNodes[nodeID & 0x0000FFFF] != 0) {
			nodeID++;
		}

		ret = parseNode(p_node.getIP(), p_node.getPort(), nodeID);

		return ret;
	}

	/**
	 * Comparator for the NodesConfigurationEntries
	 * @author Florian Klein
	 *         03.09.2013
	 */
	private final class DefaultNodesConfigurationComparator implements Comparator<NodesConfigurationEntry> {

		// Constructors
		/**
		 * Creates an instance of DefaultNodesConfigurationComparator
		 */
		public DefaultNodesConfigurationComparator() {}

		// Methods
		@Override
		public int compare(final NodesConfigurationEntry p_entry1, final NodesConfigurationEntry p_entry2) {
			int ret;

			ret = p_entry1.getSwitch() - p_entry2.getSwitch();
			if (ret == 0) {
				ret = p_entry1.getRack() - p_entry2.getRack();
				if (ret == 0) {
					ret = p_entry1.getIP().compareTo(p_entry2.getIP());
					if (ret == 0) {
						ret = p_entry1.getPort() - p_entry2.getPort();
					}
				}
			}

			return ret;
		}

	}

}

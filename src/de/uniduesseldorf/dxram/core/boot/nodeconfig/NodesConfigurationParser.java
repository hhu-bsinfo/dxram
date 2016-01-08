package de.uniduesseldorf.dxram.core.engine.nodeconfig;

import java.util.List;

import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration.NodeEntry;

/**
 * Interface for parsing the initial nodes configuration file containing routing
 * information for other nodes in the system.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 9.12.15
 *
 */
public interface NodesConfigurationParser {
	/**
	 * Read all entries from the configuration (file).
	 * @return List of NodeEntries from the configuration.
	 * @throws NodesConfigurationException If any error occurs while parsing.
	 */
	List<NodeEntry> readConfiguration() throws NodesConfigurationException;
	
	/**
	 * Write the specified entries back to the configuration (file).
	 * @param p_nodeEntries List of entries to write back.
	 * @throws NodesConfigurationException If any error occurs while writing back.
	 */
	void writeConfiguration(final List<NodeEntry> p_nodeEntries) throws NodesConfigurationException;
}

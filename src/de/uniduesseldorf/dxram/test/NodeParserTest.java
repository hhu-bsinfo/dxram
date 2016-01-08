
package de.uniduesseldorf.dxram.test;

import de.uniduesseldorf.dxram.core.boot.NodesWatcher;
import de.uniduesseldorf.dxram.core.dxram.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.SimpleNodesConfigurationParser;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration.NodeEntry;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration.Role;

import de.uniduesseldorf.utils.config.ConfigurationHandler;

/**
 * Test case for the node configuration parser
 * @author Florian Klein
 *         03.09.2013
 */
public final class NodeParserTest {

	// Constructors
	/**
	 * Creates an instance of NodeParserTest
	 */
	private NodeParserTest() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		if (p_arguments.length == 1) {
			parse(Integer.parseInt(p_arguments[0]));
		} else {
			System.out.println("Define test mode");
		}
	}

	/**
	 * Program entry point
	 * @param p_testMode
	 *            The test mode (0 = SimpleNodesConfigurationParser, other = DefaultNCP.)
	 */
	public static void parse(final int p_testMode) {
		NodesConfiguration configuration;
		long[] parsedNodes = null;
		int superpeers = 0;
		int peers = 0;

		configuration = NodesConfigurationHandler.getEmptyConfiguration();

		addNodes(configuration, 65535, (byte) 64, (byte) 64);

		if (0 == p_testMode) {
			parsedNodes = new SimpleNodesConfigurationParser().parseNodes(configuration);

			for (long l : parsedNodes) {
				if (l != 0) {
					if ((l & 0x000F000000000000L) == 0) {
						superpeers++;
						System.out.print("S:\t");
					} else {
						peers++;
						System.out.print("P:\t");
					}
					System.out.println(Long.toHexString(l));
				}
			}

			System.out.println();
			System.out.println("Superpeers:\t" + superpeers);
			System.out.println("Peers:\t" + peers);
			System.out.println("Ratio:\t" + superpeers * 100.0 / (peers + superpeers) + "%");
		} else {
			try {
				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
				parsedNodes = new NodesWatcher().parseNodes(configuration);
			} catch (final DXRAMException e) {
				e.printStackTrace();
			}

			for (long l : parsedNodes) {
				if (l != 0) {
					System.out.println(Long.toHexString(l));
				}
			}
		}
	}

	/**
	 * Generates nodes for the configuration
	 * @param p_configuration
	 *            the nodes configuration
	 * @param p_count
	 *            the number of nodes
	 * @param p_nodesPerRack
	 *            the number of nodes per rack
	 * @param p_racksPerSwitch
	 *            the number of racks per switch
	 */
	private static void addNodes(final NodesConfiguration p_configuration, final int p_count, final byte p_nodesPerRack, final byte p_racksPerSwitch) {
		String ip;
		int port;
		short s;
		short r;
		short n;

		for (int i = 0; i < p_count; i++) {
			s = (short) (i / (p_nodesPerRack * p_racksPerSwitch));
			r = (short) (i % p_racksPerSwitch / p_nodesPerRack);
			n = (short) (i % p_nodesPerRack + 1);

			ip = "10." + (s + 1) + "." + (r + 1) + "." + (n + 1);
			port = (int) (Math.random() * 65534) + 1;

			p_configuration.addNode(new NodeEntry(ip, port, r, s, Role.PEER));
		}
	}
}

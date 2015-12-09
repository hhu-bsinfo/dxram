
package de.uniduesseldorf.dxram.run;

import de.uniduesseldorf.dxram.commands.Shell;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationHandler;

import de.uniduesseldorf.utils.config.ConfigurationHandler;

/**
 * Monitoring peer
 * @author Kevin Beineke 13.08.2015
 */
public final class Monitor {

	// Constructors
	/**
	 * Creates an instance of Monitor
	 */
	private Monitor() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {

		// Initialize DXRAM
		try {
			Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
					NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
		} catch (final DXRAMException e1) {
			e1.printStackTrace();
		}

		System.out.println("Monitor started");

		System.out.println("Creating a shell ...");
		Shell.loop();
	}

}

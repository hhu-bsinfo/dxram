
package de.uniduesseldorf.dxram.run;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationHandler;

import de.uniduesseldorf.utils.config.ConfigurationHandler;

/**
 * Superpeer
 * @author Kevin Beineke 21.08.2015
 */
public final class Superpeer {

	// Constructors
	/**
	 * Creates an instance of Superpeer
	 */
	private Superpeer() {}

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

		System.out.println("Superpeer started");

		while (true) {
			try {
				// Wait a moment
				Thread.sleep(3000);
			} catch (final InterruptedException e) {}
		}
	}

}

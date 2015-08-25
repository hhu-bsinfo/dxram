package de.uniduesseldorf.dxram.run;


import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.commands.CommandHandler;;


/**
 * Peer
 * @author Kevin Beineke 21.8.2015
 */
public final class Peer {

	// Constructors
	/**
	 * Creates an instance of Peer
	 */
	private Peer() {}


	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		// Initialize DXRAM
		try {
			Core.initialize(ConfigurationHandler
					.getConfigurationFromFile("config/dxram.config"),
					NodesConfigurationHandler
							.getConfigurationFromFile("config/nodes.dxram"));
		} catch (final DXRAMException e1) {
			e1.printStackTrace();
		}
		Core.registerCmdListenerr(new CommandHandler());

		System.out.println("Peer started");

		while (true) {
			// Wait a moment
			try {
				Thread.sleep(3000);
			} catch (final InterruptedException e) {}
		}
	}

}


package de.hhu.bsinfo.dxram.test;

import de.uniduesseldorf.dxram.core.dxram.Core;
import de.uniduesseldorf.dxram.core.dxram.nodeconfig.NodeID;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.engine.DXRAMException;
import de.hhu.bsinfo.dxram.engine.nodeconfig.NodesConfigurationHandler;
import de.hhu.bsinfo.utils.ArrayTools;
import de.hhu.bsinfo.utils.config.ConfigurationHandler;

/*
 * Start-up:
 * 1) Start at least one superpeer: With parameter "superpeer", must also be a superpeer in nodes.config
 * 2) Start server: With parameters "server x" whereas x is the number of messages that should be stored on server
 * 3) Set serverID to NodeID of server
 * 4) Start clients: No parameters
 * Notes:
 * a) The server's NodeID only changes if the number of first initialized superpeers differs. For one superpeer:
 * serverID = -15999
 * b) Server and clients can be peers and superpeers, but should be peers only for a reasonable allocation of roles in
 * DXRAM
 */
/**
 * Test case for the distributed Chunk handling
 * @author Florian Klein
 *         22.07.2013
 */
public final class MailboxTest {

	// Constructors
	/**
	 * Creates an instance of MailboxTest
	 */
	private MailboxTest() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		if (p_arguments.length == 1 && p_arguments[0].equals("superpeer")) {
			new Superpeer().start();
		} else if (p_arguments.length == 2 && p_arguments[0].equals("server")) {
			new Server(Integer.parseInt(p_arguments[1])).start();
		} else if (p_arguments.length == 1 && p_arguments[0].equals("backup")) {
			new Backup().start();
		} else {
			new Client().start();
		}
	}

	// Classes






	/**
	 * Represents a backup peer
	 * @author Kevin Beineke
	 *         20.04.2014
	 */
	private static class Backup {

		// Constructors
		/**
		 * Creates an instance of Backup
		 */
		Backup() {}

		// Methods
		/**
		 * Starts the Backup peer
		 */
		public void start() {
			// Wait a moment
			try {
				Thread.sleep(1000);
			} catch (final InterruptedException e) {}

			// Initialize EPM
			try {
				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
			} catch (final DXRAMException e1) {
				e1.printStackTrace();
			}

			System.out.println("Backup peer started");

			// Get the Mailbox-Content
			while (true) {
				// Wait a moment
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {}
			}
		}

	}

}

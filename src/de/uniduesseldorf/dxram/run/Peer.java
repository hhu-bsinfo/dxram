
package de.uniduesseldorf.dxram.run;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

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
			Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
					NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
		} catch (final DXRAMException e1) {
			e1.printStackTrace();
		}

		System.out.println("Peer started");

		try {
			Thread.sleep(10000);
		} catch (final InterruptedException e) {}


/*		// create some data
		System.out.println("Generating test data");
		try {
			for (int i=0; i<1; i++) {
				final String str = Integer.toString(i);
				final Chunk c = Core.createNewChunk(str.length());
				if (c == null) {
					System.out.println("error: createNewChunk failed while generating test data");
					return ;
				}
				final ByteBuffer b = c.getData();
				b.put(str.getBytes());

				Core.put(c);
			}
		} catch (final DXRAMException de) {
			System.out.println("DXRAMException while generating test data");
		}
		System.out.println("Done.");
*/
		/*
		 * Test:
		 * try {
		 * Core.recoverFromLog();
		 * } catch (final DXRAMException e1) {
		 * // TODO Auto-generated catch block
		 * e1.printStackTrace();
		 * }
		 */

		while (true) {
			// Wait a moment
			try {
				Thread.sleep(3000);
			} catch (final InterruptedException e) {}
		}
	}

}

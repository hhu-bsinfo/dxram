
package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.DXRAM;

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
		
		DXRAM dxram = new DXRAM();
		if (!dxram.initialize("config/dxram.conf", null, null, "Peer", true)) {
			System.out.println("Failed starting peer.");
			System.exit(-1);
		}

		System.out.println("Peer started");

		while (true) {
			// Wait a moment
			try {
				Thread.sleep(3000);
			} catch (final InterruptedException e) {}
		}
	}
}

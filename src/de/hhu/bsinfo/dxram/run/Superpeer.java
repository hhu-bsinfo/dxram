
package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.DXRAM;

/**
 * Run a DXRAM Superpeer instance.
 * @author Kevin Beineke 21.08.2015
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
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
		DXRAM dxram = new DXRAM();
		if (!dxram.initialize("config/dxram.conf", null, null, "Superpeer", true)) {
			System.out.println("Failed starting superpeer.");
			System.exit(-1);
		}

		System.out.println("Superpeer started");

		while (true) {
			// Wait a moment
			try {
				Thread.sleep(3000);
			} catch (final InterruptedException e) {}
		}
	}
}

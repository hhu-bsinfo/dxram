
package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.commands.Shell;

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
		DXRAM dxram = new DXRAM();
		dxram.initialize("config", null, null, "Monitor");

		System.out.println("Monitor started");

		while (true) {
			// Wait a moment
			try {
				Thread.sleep(3000);
			} catch (final InterruptedException e) {}
		}

		System.out.println("Monitor started");

		System.out.println("Creating a shell ...");
		Shell.loop();
	}

}
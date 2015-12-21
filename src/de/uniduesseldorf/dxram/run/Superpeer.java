
package de.uniduesseldorf.dxram.run;

import de.uniduesseldorf.dxram.core.dxram.DXRAM;

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
		DXRAM dxram = new DXRAM();
		Runtime.getRuntime().addShutdownHook(new ShutdownThread(dxram));
		dxram.initialize("config", null, null, "Superpeer");

		System.out.println("Superpeer started");

		while (true) {
			// Wait a moment
			try {
				Thread.sleep(3000);
			} catch (final InterruptedException e) {}
		}
	}

	/**
	 * Shuts down DXRAM in case of the system exits
	 * @author Florian Klein 03.09.2013
	 */
	private static final class ShutdownThread extends Thread {

		private DXRAM m_dxram = null;
		
		// Constructors
		/**
		 * Creates an instance of ShutdownThread
		 */
		private ShutdownThread(final DXRAM p_dxram) {
			super(ShutdownThread.class.getSimpleName());
			m_dxram = p_dxram;
		}

		// Methods
		@Override
		public void run() {
			m_dxram.shutdown();
		}

	}
}

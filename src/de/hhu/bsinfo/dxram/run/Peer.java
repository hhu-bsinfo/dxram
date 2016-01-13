
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
		if (!dxram.initialize("config", null, null, "Peer")) {
			System.out.println("Failed starting peer.");
			System.exit(-1);
		}
		Runtime.getRuntime().addShutdownHook(new ShutdownThread(dxram));

		System.out.println("Peer started");

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

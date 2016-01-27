
package de.uniduesseldorf.dxram.test;

import java.util.Arrays;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Second test case for Cluster 2016.
 * Tests the performance of the log (without reorganization), chunk and network interfaces:
 * - One master creates new chunks until it runs out of memory. Every CHUNKS_PER_PUT chunks, the chunks are logged by calling put().
 * - Every Chunk is replicated on three backup peers.
 * - Network bandwidth is logged externally.
 * @author Kevin Beineke
 *         19.01.2016
 */
public final class ClusterLogTest2 {

	// Constants
	protected static final int CHUNK_SIZE = 100;
	protected static final int CHUNKS_PER_PUT = 1000;

	// Constructors
	/**
	 * Creates an instance of ClusterLogTest2
	 */
	private ClusterLogTest2() {}

	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		new Master().start();
	}

	/**
	 * The Master constantly creates new chunks and sends them to backup peers.
	 * @author Kevin Beineke
	 *         19.01.2016
	 */
	private static class Master {

		// Constructors
		/**
		 * Creates an instance of Server
		 */
		Master() {}

		// Methods
		/**
		 * Starts the server
		 */
		public void start() {
			long counter = 0;
			long start;
			int[] sizes;
			Chunk[] chunks;

			// Initialize DXRAM
			try {
				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
			} catch (final DXRAMException e1) {
				e1.printStackTrace();
			}

			sizes = new int[CHUNKS_PER_PUT];
			Arrays.fill(sizes, CHUNK_SIZE);

			start = System.currentTimeMillis();
			while (counter < 3221225472L) {
				try {
					// Create array of Chunks
					chunks = Core.createNewChunks(sizes);

					// Store them in-memory and replicate them on backups' SSD
					Core.put(chunks);

					counter += CHUNKS_PER_PUT * CHUNK_SIZE;
					System.out.println("Created " + CHUNKS_PER_PUT + " chunks and replicated them.");
				} catch (final DXRAMException e) {
					e.printStackTrace();
					break;
				}
			}
			System.out.println("Time to create 3GB payload: " + (System.currentTimeMillis() - start));
		}
	}

}

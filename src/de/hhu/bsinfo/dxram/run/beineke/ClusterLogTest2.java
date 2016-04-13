
package de.hhu.bsinfo.dxram.run.beineke;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;

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
	protected static final int CHUNKS_PER_PUT = 100;

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
	 * The Master constantly creates new chunks and sends them automatically to backup peers if backup is enabled.
	 * @author Kevin Beineke
	 *         19.01.2016
	 */
	private static class Master {

		// Constructors
		/**
		 * Creates an instance of Master
		 */
		Master() {}

		// Methods
		/**
		 * Starts the Master
		 */
		public void start() {
			long counter = 0;
			long start;
			Chunk[] chunks;

			// Initialize DXRAM
			final DXRAM dxram = new DXRAM();
			dxram.initialize("config/dxram.conf");
			final ChunkService chunkService = dxram.getService(ChunkService.class);

			// Create array of Chunks
			chunks = new Chunk[CHUNKS_PER_PUT];
			for (int i = 0; i < CHUNKS_PER_PUT; i++) {
				chunks[i] = new Chunk(CHUNK_SIZE);
				chunks[i].getData().put("Test!".getBytes());
			}

			start = System.currentTimeMillis();
			while (counter < 3221225472L) {
				// Create new chunks in MemoryManagement
				chunkService.create(chunks);

				// Store them in-memory and replicate them on backups' SSD
				chunkService.put(chunks);

				counter += CHUNKS_PER_PUT * CHUNK_SIZE;
				// System.out.println("Created " + CHUNKS_PER_PUT + " chunks and replicated them.");
			}
			System.out.println("Time to create 3GB payload: " + (System.currentTimeMillis() - start) + " ms");
		}
	}

}

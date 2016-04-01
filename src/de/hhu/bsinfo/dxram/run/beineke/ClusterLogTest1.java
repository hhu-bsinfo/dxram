
package de.uniduesseldorf.dxram.test;

import java.util.Arrays;
import java.util.Random;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * First test case for Cluster 2016.
 * Tests the performance of the chunk and network interfaces:
 * - Three clients create CHUNKS_PER_CLIENT chunks of size CHUNK_SIZE and wait for requests.
 * - One master puts a range of chunks (CHUNKS_PER_PUT, random offset, random client) periodically.
 * - Network bandwidth is logged externally.
 * @author Kevin Beineke
 *         19.01.2016
 */
public final class ClusterLogTest1 {

	// Constants
	protected static final int CHUNKS_PER_CLIENT = 10000;
	protected static final int CHUNK_SIZE = 100;
	protected static final int CHUNKS_PER_PUT = 1000;

	// Constructors
	/**
	 * Creates an instance of ClusterLogTest1
	 */
	private ClusterLogTest1() {}

	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		if (p_arguments.length == 0) {
			System.out.println("Missing program argument: Role (master, client)");
		} else if (p_arguments[0].equals("master")) {
			new Master().start();
		} else if (p_arguments[0].equals("client")) {
			new Client().start();
		}
	}

	/**
	 * The Master randomly writes Chunks of the Clients.
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
			short nodeID;
			int offset;
			short[] nodeIDs;
			Chunk[] chunks;
			final Random rand = new Random();

			// Initialize DXRAM
			try {
				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
			} catch (final DXRAMException e1) {
				e1.printStackTrace();
			}

			// Create array of NodeIDs
			nodeIDs = new short[3];
			nodeIDs[0] = -15999;
			nodeIDs[1] = 320;
			nodeIDs[2] = -15615;

			// Create array of Chunks
			chunks = new Chunk[CHUNKS_PER_PUT];
			for (int i = 0; i < CHUNKS_PER_PUT; i++) {
				chunks[i] = new Chunk(0, new byte[CHUNK_SIZE]);
			}

			// Send chunks to clients. Client and offset is chosen randomly.
			while (true) {
				nodeID = nodeIDs[rand.nextInt(3)];
				offset = rand.nextInt(CHUNKS_PER_CLIENT - CHUNKS_PER_PUT) + 1;
				for (int i = 0; i < CHUNKS_PER_PUT; i++) {
					chunks[i].setChunkID(((long) nodeID << 48) + offset + i);
				}

				try {
					Core.put(chunks);
				} catch (final DXRAMException e) {
					e.printStackTrace();
				}
				System.out.println("Wrote " + CHUNKS_PER_PUT + " on " + nodeID + " starting at " + offset + ".");
			}
		}
	}

	/**
	 * The Client creates a fixed number of Chunks with fixed size.
	 * @author Kevin Beineke
	 *         19.01.2016
	 */
	private static class Client {

		// Constructors
		/**
		 * Creates an instance of Client
		 */
		Client() {}

		// Methods
		/**
		 * Starts the client
		 */
		public void start() {
			int[] sizes;
			Chunk[] chunks;

			// Initialize DXRAM
			try {
				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
			} catch (final DXRAMException e1) {
				e1.printStackTrace();
			}

			try {
				// Create chunks
				sizes = new int[CHUNKS_PER_CLIENT];
				Arrays.fill(sizes, CHUNK_SIZE);

				chunks = Core.createNewChunks(sizes);
				for (int i = 0; i < CHUNKS_PER_CLIENT; i++) {
					chunks[i].getData().put("Test!".getBytes());
				}

				// Put
				Core.put(chunks);
			} catch (final DXRAMException e1) {
				e1.printStackTrace();
			}
			System.out.println("Created " + CHUNKS_PER_CLIENT + " chunks with a size of " + CHUNK_SIZE + " bytes.");

			// Wait
			while (true) {
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {}
			}
		}
	}

}

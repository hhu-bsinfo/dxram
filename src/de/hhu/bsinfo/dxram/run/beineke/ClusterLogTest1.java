
package de.hhu.bsinfo.dxram.run.beineke;

import java.nio.ByteBuffer;
import java.util.Random;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;

/**
 * First test case for Cluster 2016.
 * Tests the performance of the chunk and network interfaces:
 * - Three masters create CHUNKS_PER_CLIENT chunks of size CHUNK_SIZE and wait for requests.
 * - One client puts a range of chunks (CHUNKS_PER_PUT, random offset, random client) periodically.
 * - Network bandwidth is logged externally.
 * @author Kevin Beineke
 *         19.01.2016
 */
public final class ClusterLogTest1 {

	// Constants
	protected static final int CHUNKS_PER_MASTER = 10000;
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
	 * The Master creates a fixed number of Chunks with fixed size.
	 * @author Kevin Beineke
	 *         19.01.2016
	 */
	private static class Master {

		// Constructors
		/**
		 * Creates an instance of Client
		 */
		Master() {}

		// Methods
		/**
		 * Starts the client
		 */
		public void start() {
			Chunk[] chunks;

			// Initialize DXRAM
			final DXRAM dxram = new DXRAM();
			dxram.initialize("config/dxram.conf");
			final ChunkService chunkService = dxram.getService(ChunkService.class);

			// Create chunks
			chunks = new Chunk[CHUNKS_PER_MASTER];
			for (int i = 0; i < CHUNKS_PER_MASTER; i++) {
				chunks[i] = new Chunk(CHUNK_SIZE);
				chunks[i].getData().put("Test!".getBytes());
			}
			chunkService.create(chunks);
			chunkService.put(chunks);

			// Put
			System.out.println("Created " + CHUNKS_PER_MASTER + " chunks with a size of " + CHUNK_SIZE + " bytes.");

			// Wait
			while (true) {
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {}
			}
		}
	}

	/**
	 * The Client randomly writes Chunks from Master.
	 * @author Kevin Beineke
	 *         19.01.2016
	 */
	private static class Client {

		// Constructors
		/**
		 * Creates an instance of Server
		 */
		Client() {}

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
			final DXRAM dxram = new DXRAM();
			dxram.initialize("config/dxram.conf");
			final ChunkService chunkService = dxram.getService(ChunkService.class);

			// Create array of NodeIDs
			nodeIDs = new short[3];
			nodeIDs[0] = -15999;
			nodeIDs[1] = 320;
			nodeIDs[2] = -15615;

			// Create array of Chunks
			chunks = new Chunk[CHUNKS_PER_PUT];
			for (int i = 0; i < CHUNKS_PER_PUT; i++) {
				chunks[i] = new Chunk(0, ByteBuffer.allocate(CHUNK_SIZE));
			}

			// Send chunks to clients. Client and offset is chosen randomly.
			while (true) {
				nodeID = nodeIDs[rand.nextInt(3)];
				offset = rand.nextInt(CHUNKS_PER_MASTER - CHUNKS_PER_PUT) + 1;
				for (int i = 0; i < CHUNKS_PER_PUT; i++) {
					chunks[i].setID(((long) nodeID << 48) + offset + i);
				}

				chunkService.put(chunks);
				System.out.println("Wrote " + CHUNKS_PER_PUT + " on " + nodeID + " starting at " + offset + ".");
			}
		}
	}

}

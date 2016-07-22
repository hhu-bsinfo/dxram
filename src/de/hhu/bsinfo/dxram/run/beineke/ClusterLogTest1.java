
package de.hhu.bsinfo.dxram.run.beineke;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;

/**
 * First test case for Cluster 2016.
 * Tests the performance of the chunk and network interfaces:
 * - X masters create CHUNKS_PER_CLIENT chunks of size CHUNK_SIZE and wait for requests.
 * - One client puts a range of chunks (CHUNKS_PER_PUT, random offset, random client) periodically.
 * - Network bandwidth is logged externally.
 * @author Kevin Beineke
 *         19.01.2016
 */
public final class ClusterLogTest1 {

	// Constants
	private static final int CHUNKS_PER_MASTER = 10000;
	private static final int CHUNK_SIZE = 100;
	private static final int CHUNKS_PER_PUT = 1000;

	private static final int CLIENT_THREADS = 10;

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
		if (p_arguments.length < 2) {
			System.out.println("Missing program argument: Role (master, client) and/or ZooKeeperString");
		} else if (p_arguments[0].equals("master")) {
			new Master().start(p_arguments[1]);
		} else if (p_arguments[0].equals("client")) {
			// Initialize DXRAM
			final DXRAM dxram = new DXRAM();
			dxram.initialize("config/dxram.conf");
			final ChunkService chunkService = dxram.getService(ChunkService.class);

			// Connect to ZooKeeper
			ZooKeeper zookeeper = null;
			try {
				zookeeper = new ZooKeeper(p_arguments[1], 10000, null);
			} catch (IOException e1) {
				System.out.println("Cannot connect to ZooKeeper! Aborting.");
				System.exit(-1);
			}

			short[] masters = null;
			try {
				// Read and store all masters
				List<String> children = zookeeper.getChildren("/dxram/nodes/masters", null);
				masters = new short[children.size()];
				for (int i = 0; i < children.size(); i++) {
					masters[i] = Short.parseShort(children.get(i));
				}
			} catch (final KeeperException | InterruptedException e) {
				e.printStackTrace();
			}

			Client currentThread = null;
			for (int i = 0; i < CLIENT_THREADS; i++) {
				currentThread = new Client(chunkService, masters);
				currentThread.start();
			}
			try {
				currentThread.join();
			} catch (final InterruptedException e) {
				/* ignore, shutting down anyway */
			}
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
		 * Creates an instance of Master
		 */
		private Master() {}

		// Methods
		/**
		 * Starts the master
		 * @param p_zookeeperString
		 *            the ZooKeeper connection string (IP:port)
		 */
		public void start(final String p_zookeeperString) {
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

			// Put
			chunkService.put(chunks);
			System.out.println("Created " + CHUNKS_PER_MASTER + " chunks with a size of " + CHUNK_SIZE + " bytes.");

			// Connect to ZooKeeper
			ZooKeeper zookeeper = null;
			try {
				zookeeper = new ZooKeeper(p_zookeeperString, 10000, null);
			} catch (final IOException e1) {
				System.out.println("Cannot connect to ZooKeeper! Aborting.");
				System.exit(-1);
			}

			try {
				if (zookeeper.exists("/dxram/nodes/masters", null) == null) {
					zookeeper.create("/dxram/nodes/masters", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				// Add master
				zookeeper.create("/dxram/nodes/masters/" + dxram.getService(BootService.class).getNodeID(),
						new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (final KeeperException | InterruptedException e) {
				System.out.println("Cannot write to ZooKeeper! Aborting.");
				System.exit(-1);
			}

			// Wait
			while (true) {
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {
					/* ignore */
				}
			}
		}
	}

	/**
	 * The Client randomly writes Chunks from Master.
	 * @author Kevin Beineke
	 *         19.01.2016
	 */
	private static class Client extends Thread {

		// Attributes
		private ChunkService m_chunkService;
		private short[] m_masters;

		// Constructors
		/**
		 * Creates an instance of Client
		 * @param p_chunkService
		 *            the initialized ChunkService
		 * @param p_masters
		 *            all masters' NodeIDs
		 */
		private Client(final ChunkService p_chunkService, final short[] p_masters) {
			m_chunkService = p_chunkService;
			m_masters = p_masters;
		}

		// Methods
		/**
		 * Starts the client
		 */
		@Override
		public void run() {
			short nodeID;
			int offset;
			Chunk[] chunks;
			final Random rand = new Random();

			// Create array of Chunks
			chunks = new Chunk[CHUNKS_PER_PUT];
			for (int i = 0; i < CHUNKS_PER_PUT; i++) {
				chunks[i] = new Chunk(0, ByteBuffer.allocate(CHUNK_SIZE));
			}

			// Send chunks to clients. Client and offset is chosen randomly.
			while (true) {
				nodeID = m_masters[rand.nextInt(m_masters.length)];
				offset = rand.nextInt(CHUNKS_PER_MASTER - CHUNKS_PER_PUT) + 1;
				for (int i = 0; i < CHUNKS_PER_PUT; i++) {
					chunks[i].setID(((long) nodeID << 48) + offset + i);
				}

				m_chunkService.put(chunks);
				// System.out.println("Wrote " + CHUNKS_PER_PUT + " on " + nodeID + " starting at " + offset + ".");
			}
		}
	}

}

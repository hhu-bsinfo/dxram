
package de.uniduesseldorf.dxram.test;

import java.util.Arrays;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Third test case for Cluster 2016. Imitates a test found in http://dx.doi.org/10.1145/2806887.
 * Tests the performance of the log service with reorganization, chunk and network interfaces:
 * Phase 1:
 * - One master creates new chunks until specified utilization is reached. Every CHUNKS_PER_PUT chunks, the chunks are
 * logged by calling put().
 * - Every Chunk is replicated on six backup peers.
 * Phase 2:
 * - Chunks are overwritten to put load on the reorganization. 75 chunks (uniform or zipfian) are updated with every
 * access.
 * - Network bandwidth and cpu load is logged externally.
 * @author Kevin Beineke
 *         22.01.2016
 */
public final class ClusterLogTest3 {

	// Constants
	protected static final long BYTES_TO_LOAD = 5196002400L;
	protected static final long BYTES_TO_UPDATE = 6000128000L;
	protected static final int CHUNK_SIZE = 100;
	protected static final int CHUNKS_PER_PUT = 100;
	protected static final int CHUNKS_PER_UPDATE = 500;
	protected static final int THREADS = 8;

	// Attributes
	// Workload: 0 -> uniform, 1 -> zipfian
	private static byte m_workload = 0;

	// Constructors
	/**
	 * Creates an instance of ClusterLogTest3
	 */
	private ClusterLogTest3() {}

	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		if (p_arguments.length == 0) {
			System.out.println("Missing program argument: Role (master, benchmark)");
		} else if (p_arguments[0].equals("master")) {
			new Master().start();
		} else if (p_arguments[0].equals("benchmark")) {
			// Initialize DXRAM
			try {
				Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
						NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
			} catch (final DXRAMException e1) {
				e1.printStackTrace();
			}

			Benchmark currentThread = null;
			for (int i = 0; i < THREADS; i++) {
				currentThread = new Benchmark();
				currentThread.start();
			}
			try {
				currentThread.join();
			} catch (final InterruptedException e) {}
		}
	}

	/**
	 * @author Kevin Beineke
	 *         22.01.2016
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

			/**
			 * Phase 1: Creating chunks
			 */
			sizes = new int[CHUNKS_PER_PUT];
			Arrays.fill(sizes, CHUNK_SIZE);

			start = System.currentTimeMillis();
			while (counter < BYTES_TO_LOAD) {
				try {
					// Create array of Chunks
					chunks = Core.createNewChunks(sizes);

					// Store them in-memory and replicate them on backups' SSD
					Core.put(chunks);

					counter += CHUNK_SIZE * CHUNKS_PER_PUT;
				} catch (final DXRAMException e) {
					e.printStackTrace();
					break;
				}
				if (counter % (100 * 1000 * 1000) == 0) {
					System.out.println("Created 100.000.000 bytes and replicated them. All: " + counter);
				}
			}
			System.out.println("Time to create " + BYTES_TO_LOAD + " bytes of payload: " + (System.currentTimeMillis() - start));
		}
	}

	/**
	 * @author Kevin Beineke
	 *         22.01.2016
	 */
	private static class Benchmark extends Thread {

		// Constructors
		/**
		 * Creates an instance of Client
		 */
		Benchmark() {}

		// Methods
		/**
		 * Starts the client
		 */
		@Override
		public void run() {
			final short[] nodeIDs = new short[2];
			nodeIDs[0] = -15807;
			nodeIDs[1] = -14847;
			long start;
			long counter = 0;
			Chunk[] chunks;

			Random rand;
			FastZipfGenerator zipf;

			// Create array of Chunks
			chunks = new Chunk[CHUNKS_PER_UPDATE];
			for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
				chunks[i] = new Chunk(0, new byte[CHUNK_SIZE]);
			}

			start = System.currentTimeMillis();
			rand = new Random();
			// Send updates to master
			if (m_workload == 0) {

				while (counter < BYTES_TO_UPDATE / THREADS) {
					/*-long offset = nextLong(rand, BYTES_TO_LOAD / CHUNK_SIZE - CHUNKS_PER_PUT) + 1;
					for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
						chunks[i].setChunkID(((long) nodeIDs[rand.nextInt(2)] << 48) + offset + i);
					}*/
					for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
						chunks[i].setChunkID(((long) nodeIDs[rand.nextInt(2)] << 48) + nextLong(rand, BYTES_TO_LOAD / CHUNK_SIZE - CHUNKS_PER_PUT) + 1);
					}

					try {
						Core.put(chunks);
					} catch (final DXRAMException e) {
						e.printStackTrace();
					}

					counter += CHUNK_SIZE * CHUNKS_PER_UPDATE;
					if (counter % (10 * 1000 * 1000) == 0) {
						System.out.println("Updated 10.000.000 bytes with random distribution. All: " + counter);
					}
				}
			} else {
				System.out.println("Initializing ZipfGenerator. This might take a little.");
				zipf = new FastZipfGenerator((int) (BYTES_TO_LOAD / CHUNK_SIZE - CHUNKS_PER_PUT + 1), 0.5);

				while (counter < BYTES_TO_UPDATE / THREADS) {
					for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
						chunks[i].setChunkID(((long) nodeIDs[rand.nextInt(2)] << 48) + zipf.next());
					}

					try {
						Core.put(chunks);
					} catch (final DXRAMException e) {
						e.printStackTrace();
					}

					counter += CHUNK_SIZE * CHUNKS_PER_UPDATE;
					if (counter % (10 * 1000 * 1000) == 0) {
						System.out.println("Updated 10.000.000 bytes with zipfian distribution. All: " + counter);
					}
				}
			}
			System.out.println("Time to update " + BYTES_TO_UPDATE + " bytes of payload: " + (System.currentTimeMillis() - start));
		}

		long nextLong(final Random rng, final long n) {
			// error checking and 2^x checking removed for simplicity.
			long bits, val;
			do {
				bits = rng.nextLong() << 1 >>> 1;
				val = bits % n;
			} while (bits - val + n - 1 < 0L);
			return val;
		}
	}

	static class FastZipfGenerator {
		private Random random = new Random(0);
		private NavigableMap<Double, Integer> map;

		FastZipfGenerator(final int size, final double skew) {
			map = computeMap(size, skew);
		}

		private NavigableMap<Double, Integer> computeMap(final int size, final double skew) {
			NavigableMap<Double, Integer> map =
					new TreeMap<Double, Integer>();

			double div = 0;
			for (int i = 1; i <= size; i++) {
				div += 1 / Math.pow(i, skew);
			}

			double sum = 0;
			for (int i = 1; i <= size; i++) {
				double p = 1.0d / Math.pow(i, skew) / div;
				sum += p;
				map.put(sum, i - 1);
			}
			return map;
		}

		public int next() {
			double value = random.nextDouble();
			return map.ceilingEntry(value).getValue() + 1;
		}

	}

}

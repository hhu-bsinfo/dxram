
package de.hhu.bsinfo.dxram.run.beineke;

import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;

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
	private static byte m_workload;

	// Constructors
	/**
	 * Creates an instance of ClusterLogTest3
	 * @param p_workload
	 *            the worload (0 -> uniform, 1 -> zipfian)
	 */
	private ClusterLogTest3(final byte p_workload) {
		m_workload = p_workload;
	}

	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		// Workload: 0 -> uniform, 1 -> zipfian
		new ClusterLogTest3((byte) 0);

		if (p_arguments.length == 0) {
			System.out.println("Missing program argument: Role (master, benchmark)");
		} else if (p_arguments[0].equals("master")) {
			new Master().start();
		} else if (p_arguments[0].equals("benchmark")) {
			// Initialize DXRAM
			final DXRAM dxram = new DXRAM();
			dxram.initialize("config/dxram.conf");
			final ChunkService chunkService = dxram.getService(ChunkService.class);

			Benchmark currentThread = null;
			for (int i = 0; i < THREADS; i++) {
				currentThread = new Benchmark(chunkService);
				currentThread.start();
			}
			try {
				currentThread.join();
			} catch (final InterruptedException e) {}
		}
	}

	/**
	 * The Master creates a fixed amount of data.
	 * @author Kevin Beineke
	 *         22.01.2016
	 */
	private static class Master {

		// Constructors
		/**
		 * Creates an instance of Master
		 */
		Master() {}

		// Methods
		/**
		 * Starts the server
		 */
		public void start() {
			long counter = 0;
			long start;
			Chunk[] chunks;

			// Initialize DXRAM
			final DXRAM dxram = new DXRAM();
			dxram.initialize("config/dxram.conf");
			final ChunkService chunkService = dxram.getService(ChunkService.class);

			/**
			 * Phase 1: Creating chunks
			 */
			// Create array of Chunks
			chunks = new Chunk[CHUNKS_PER_PUT];
			for (int i = 0; i < CHUNKS_PER_PUT; i++) {
				chunks[i] = new Chunk(CHUNK_SIZE);
				chunks[i].getData().put("Test!".getBytes());
			}

			start = System.currentTimeMillis();
			while (counter < BYTES_TO_LOAD) {
				// Create new chunks in MemoryManagement
				chunkService.create(chunks);

				// Store them in-memory and replicate them on backups' SSD
				chunkService.put(chunks);

				counter += CHUNK_SIZE * CHUNKS_PER_PUT;
				if (counter % (100 * 1000 * 1000) == 0) {
					System.out.println("Created 100.000.000 bytes and replicated them. All: " + counter);
				}
			}
			System.out.println("Time to create " + BYTES_TO_LOAD + " bytes of payload: " + (System.currentTimeMillis() - start));
		}
	}

	/**
	 * The Benchmark changes (puts) the data of all Masters.
	 * @author Kevin Beineke
	 *         22.01.2016
	 */
	private static class Benchmark extends Thread {

		// Attributes
		private ChunkService m_chunkService;

		// Constructors
		/**
		 * Creates an instance of Client
		 * @param p_chunkService
		 *            the initialized ChunkService
		 */
		Benchmark(final ChunkService p_chunkService) {
			m_chunkService = p_chunkService;
		}

		// Methods
		/**
		 * Starts the client
		 */
		@Override
		public void run() {
			final short[] nodeIDs = new short[3];
			nodeIDs[0] = 960;
			nodeIDs[1] = 640;
			nodeIDs[2] = -15807;

			long start;
			long counter = 0;
			Chunk[] chunks;

			Random rand;
			FastZipfGenerator zipf;

			// Create array of Chunks
			chunks = new Chunk[CHUNKS_PER_UPDATE];
			for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
				chunks[i] = new Chunk(CHUNK_SIZE);
				chunks[i].getData().put("Update!".getBytes());
			}

			start = System.currentTimeMillis();
			rand = new Random();
			// Send updates to master
			if (m_workload == 0) {

				while (counter < BYTES_TO_UPDATE / THREADS) {
					/*-long offset = nextLong(rand, BYTES_TO_LOAD / CHUNK_SIZE - CHUNKS_PER_PUT) + 1;
					for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
						chunks[i].setChunkID(((long) nodeIDs[rand.nextInt(nodeIDs.length)] << 48) + offset + i);
					}*/
					for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
						chunks[i].setID(((long) nodeIDs[rand.nextInt(nodeIDs.length)] << 48)
								+ nextLong(rand, BYTES_TO_LOAD / CHUNK_SIZE - CHUNKS_PER_PUT) + 1);
					}

					m_chunkService.put(chunks);

					counter += CHUNK_SIZE * CHUNKS_PER_UPDATE;
					if (counter % (10 * 1000 * 1000) == 0) {
						System.out.println(Thread.currentThread().getName()
								+ ": Updated 10.000.000 bytes with random distribution(left: " + (BYTES_TO_UPDATE / THREADS - counter) + ").");
					}
				}
			} else {
				System.out.println("Initializing ZipfGenerator. This might take a little.");
				zipf = new FastZipfGenerator((int) (BYTES_TO_LOAD / CHUNK_SIZE - CHUNKS_PER_PUT + 1), 0.5);

				while (counter < BYTES_TO_UPDATE / THREADS) {
					for (int i = 0; i < CHUNKS_PER_UPDATE; i++) {
						chunks[i].setID(((long) nodeIDs[rand.nextInt(nodeIDs.length)] << 48) + zipf.next());
					}

					m_chunkService.put(chunks);

					counter += CHUNK_SIZE * CHUNKS_PER_UPDATE;
					if (counter % (10 * 1000 * 1000) == 0) {
						System.out.println(Thread.currentThread().getName()
								+ ": Updated 10.000.000 bytes with zipfian distribution(left: " + (BYTES_TO_UPDATE / THREADS - counter) + ").");
					}
				}
			}
			System.out.println("Time to update " + BYTES_TO_UPDATE + " bytes of payload: " + (System.currentTimeMillis() - start));
		}

		/**
		 * Returns a random long
		 * @param p_rng
		 *            the random number generator
		 * @param p_max
		 *            the maximum value
		 * @return a random long
		 */
		long nextLong(final Random p_rng, final long p_max) {
			long bits;
			long val;

			do {
				bits = p_rng.nextLong() << 1 >>> 1;
				val = bits % p_max;
			} while (bits - val + p_max - 1 < 0L);
			return val;
		}
	}

	/**
	 * A random number generator with zipfian distribution
	 * Based on http://diveintodata.org/tag/zipf/
	 */
	static class FastZipfGenerator {
		private Random m_random = new Random(0);
		private NavigableMap<Double, Integer> m_map;

		/**
		 * Creates an instance of FastZipfGenerator
		 * @param p_size
		 *            the number of iterations during generation
		 * @param p_skew
		 *            the skew
		 */
		FastZipfGenerator(final int p_size, final double p_skew) {
			m_map = computeMap(p_size, p_skew);
		}

		/**
		 * Computes a map with zipfian distribution
		 * @param p_size
		 *            the number of iterations during generation
		 * @param p_skew
		 *            the skew
		 * @return the map
		 */
		private NavigableMap<Double, Integer> computeMap(final int p_size, final double p_skew) {
			final NavigableMap<Double, Integer> map = new TreeMap<Double, Integer>();

			double div = 0;
			for (int i = 1; i <= p_size; i++) {
				div += 1 / Math.pow(i, p_skew);
			}

			double sum = 0;
			for (int i = 1; i <= p_size; i++) {
				sum += 1.0d / Math.pow(i, p_skew) / div;
				map.put(sum, i - 1);
			}
			return map;
		}

		/**
		 * Returns a random integer with zipfian distribution
		 * @return a random integer
		 */
		public int next() {
			return m_map.ceilingEntry(m_random.nextDouble()).getValue() + 1;
		}

	}

}

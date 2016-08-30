
package de.hhu.bsinfo.soh;

import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tests concurrent allocation and freeing of memory
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.15
 */
public class SmallObjectHeapTest {
	private static final Lock LOCK = new ReentrantLock(false);

	private SmallObjectHeap m_memory;
	private int m_numThreads = -1;
	private int m_numOperations = -1;
	private float m_mallocFreeRatio;
	private int m_blockSizeMin = -1;
	private int m_blockSizeMax = -1;
	private boolean m_debugPrint;

	/**
	 * Constructor
	 *
	 * @param p_memorySize      Total raw memory size in bytes.
	 * @param p_segmentSize     Total size of a single segment
	 * @param p_numThreads      Number of threads to run this
	 * @param p_numOperations   Number of operations to execute.
	 * @param p_mallocFreeRatio Malloc/free ratio.
	 * @param p_blockSizeMin    Minimum memory block size to alloc.
	 * @param p_blockSizeMax    Maximum memory block size to alloc.
	 * @param p_debugPrint      Enable debug prints
	 */
	public SmallObjectHeapTest(final long p_memorySize, final long p_segmentSize,
			final int p_numThreads, final int p_numOperations, final float p_mallocFreeRatio,
			final int p_blockSizeMin, final int p_blockSizeMax, final boolean p_debugPrint) {
		assert p_memorySize > 0;
		assert p_segmentSize > 0;
		assert p_segmentSize <= p_memorySize;
		assert p_numThreads > 0;
		assert p_numOperations > 0;
		assert p_mallocFreeRatio >= 0.5;
		assert p_blockSizeMin > 0;
		assert p_blockSizeMax > 0;
		assert p_blockSizeMax > p_blockSizeMin;

		// m_memory = new SmallObjectHeap(new StorageUnsafeMemory());
		// m_memory = new SmallObjectHeap(new StorageRandomAccessFile(file));
		m_memory = new SmallObjectHeap(new StorageUnsafeMemory());
		// m_memory = new SmallObjectHeap(new StorageHybridUnsafeJNINativeMemory());
		// m_memory = new SmallObjectHeap(new StorageJavaHeap());

		if (m_memory.initialize(p_memorySize, p_segmentSize) == -1) {
			System.out.println("Initializing memory failed.");
		}

		m_numThreads = p_numThreads;
		m_numOperations = p_numOperations;
		m_mallocFreeRatio = p_mallocFreeRatio;
		m_blockSizeMin = p_blockSizeMin;
		m_blockSizeMax = p_blockSizeMax;
		m_debugPrint = p_debugPrint;
	}

	/**
	 * Main section
	 */
	public void run() {
		ExecutorService executor = Executors.newFixedThreadPool(m_numThreads);

		System.out.println(m_memory);

		System.out.println("Starting " + m_numThreads + " threads.");
		Vector<Future<?>> submitedTasks = new Vector<Future<?>>();
		for (int i = 0; i < m_numThreads; i++) {
			MemoryThread memThread = new MemoryThread(m_memory, m_numOperations,
					m_mallocFreeRatio, m_blockSizeMin, m_blockSizeMax, m_debugPrint);
			submitedTasks.add(executor.submit(memThread));
		}

		System.out.println("Waiting for workers to finish...");

		for (Future<?> future : submitedTasks) {
			try {
				future.get();
			} catch (final ExecutionException | InterruptedException e) {
				e.printStackTrace();
			}
		}

		//				File file = new File("rawMemory.dump");
		//				if (file.exists()) {
		//					file.delete();
		//					try {
		//						file.createNewFile();
		//					} catch (IOException e) {
		//						e.printStackTrace();
		//					}
		//				}
		//				m_memory.dump(file, 0, 12 * 1024 * 1024);

		System.out.println("All workers finished.");

		System.out.println("Final memory status:\n" + m_memory);

		HeapWalker.Results results = HeapWalker.walk(m_memory);
		System.out.println(results);
		HeapIntegrityChecker.check(results);

		executor.shutdown();
	}

	/**
	 * Java main entry point.
	 *
	 * @param p_args Command line arguments
	 */
	public static void main(final String[] p_args) {
		if (p_args.length < 8) {
			System.out.println(
					"Usage: RawMemoryTest <memorySize> <segmentSize> <numThreads> <numOperations> "
							+ "<mallocFreeRatio> <blockSizeMin> <blockSizeMax> <debugPrint>");
			return;
		}

		long memorySize = Long.parseLong(p_args[0]);
		long segmentSize = Long.parseLong(p_args[1]);
		int numThreads = Integer.parseInt(p_args[2]);
		int numOperations = Integer.parseInt(p_args[3]);
		float mallocFreeRatio = Float.parseFloat(p_args[4]);
		int blockSizeMin = Integer.parseInt(p_args[5]);
		int blockSizeMax = Integer.parseInt(p_args[6]);
		boolean debugPrint = Boolean.parseBoolean(p_args[7]);

		System.out.println("Initializing RawMemory test...");
		SmallObjectHeapTest test = new SmallObjectHeapTest(memorySize, segmentSize, numThreads, numOperations,
				mallocFreeRatio, blockSizeMin, blockSizeMax, debugPrint);
		System.out.println("Running test...");
		test.run();
		System.out.println("Test done.");
	}

	/**
	 * Thread execution memory allocations.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.15
	 */
	public class MemoryThread implements Runnable {
		private SmallObjectHeap m_memory;

		private float m_mallocFreeRatio;
		private int m_numMallocOperations = -1;
		private int m_numFreeOperations = -1;
		private int m_blockSizeMin = -1;
		private int m_blockSizeMax = -1;
		private boolean m_debugPrint;

		private Vector<Long> m_blocksAlloced = new Vector<Long>();

		/**
		 * Constructor
		 *
		 * @param p_rawMemory       Raw memory instance to use.
		 * @param p_numOperations   Number of operations to execute.
		 * @param p_mallocFreeRatio Malloc/free ratio.
		 * @param p_blockSizeMin    Minimum memory block size to alloc.
		 * @param p_blockSizeMax    Maximum memory block size to alloc.
		 * @param p_debugPrint      Enable debug prints
		 */
		public MemoryThread(final SmallObjectHeap p_rawMemory, final int p_numOperations, final float p_mallocFreeRatio,
				final int p_blockSizeMin,
				final int p_blockSizeMax, final boolean p_debugPrint) {
			assert m_blockSizeMin > 0;
			assert m_blockSizeMax > 0;
			assert m_mallocFreeRatio > 0;

			m_memory = p_rawMemory;
			m_blockSizeMin = p_blockSizeMin;
			m_blockSizeMax = p_blockSizeMax;
			m_mallocFreeRatio = p_mallocFreeRatio;
			m_debugPrint = p_debugPrint;

			if (m_blockSizeMax < m_blockSizeMin) {
				m_blockSizeMax = m_blockSizeMin;
			}

			if (p_mallocFreeRatio < 0.5) {
				m_numMallocOperations = p_numOperations / 2;
				m_numFreeOperations = m_numMallocOperations;
			} else {
				m_numMallocOperations = (int) (p_numOperations * p_mallocFreeRatio);
				m_numFreeOperations = p_numOperations - m_numMallocOperations;
			}
		}

		@Override
		public void run() {
			System.out.println("(" + Thread.currentThread().getId() + ") " + this);

			while (m_numMallocOperations + m_numFreeOperations > 0) {
				// random pick operations, depending on ratio
				if ((Math.random() <= m_mallocFreeRatio || m_numFreeOperations <= 0) && m_numMallocOperations > 0) {
					// execute alloc
					int size = 0;
					while (size <= 0) {
						size = (int) (Math.random() * (m_blockSizeMax - m_blockSizeMin)) + m_blockSizeMin;
					}

					long ptr = -1;

					LOCK.lock();
					ptr = m_memory.malloc(size);
					LOCK.unlock();
					if (m_debugPrint) {
						System.out.println(">>> Allocated " + size + ":\n" + m_memory);
					}
					m_blocksAlloced.add(ptr);
					m_memory.set(ptr, size, (byte) 0xBB);

					// test writing/reading across two blocks
					if (size > SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK) {
						{
							short v = 0x1122;
							m_memory.writeShort(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1, v);
							short v2 = m_memory.readShort(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1);
							if (v != v2) {
								System.out.println("!!! Chained blocks short writing/reading failed");
								System.exit(-1);
							}
						}
						{
							int v = 0xAABBCCDD;
							m_memory.writeInt(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1, v);
							int v2 = m_memory.readInt(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1);
							if (v != v2) {
								System.out.println("!!! Chained blocks int (1) writing/reading failed");
								System.exit(-1);
							}
						}
						{
							int v = 0xAABBCCDD;
							m_memory.writeInt(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 2, v);
							int v2 = m_memory.readInt(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 2);
							if (v != v2) {
								System.out.println("!!! Chained blocks int (2) writing/reading failed");
								System.exit(-1);
							}
						}
						{
							int v = 0xAABBCCDD;
							m_memory.writeInt(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 3, v);
							int v2 = m_memory.readInt(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 3);
							if (v != v2) {
								System.out.println("!!! Chained blocks int (3) writing/reading failed");
								System.exit(-1);
							}
						}
						{
							long v = 0x1122334455667788L;
							m_memory.writeLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1, v);
							long v2 = m_memory.readLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1);
							if (v != v2) {
								System.out.println("!!! Chained blocks long (1) writing/reading failed");
								System.exit(-1);
							}
						}
						{
							long v = 0x1122334455667788L;
							m_memory.writeLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 2, v);
							long v2 = m_memory.readLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 2);
							if (v != v2) {
								System.out.println("!!! Chained blocks long (2) writing/reading failed");
								System.exit(-1);
							}
						}
						{
							long v = 0x1122334455667788L;
							m_memory.writeLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 3, v);
							long v2 = m_memory.readLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 3);
							if (v != v2) {
								System.out.println("!!! Chained blocks long (3) writing/reading failed");
								System.exit(-1);
							}
						}
						{
							long v = 0x1122334455667788L;
							m_memory.writeLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 4, v);
							long v2 = m_memory.readLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 4);
							if (v != v2) {
								System.out.println("!!! Chained blocks long (4) writing/reading failed");
								System.exit(-1);
							}
						}
						{
							long v = 0x1122334455667788L;
							m_memory.writeLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 5, v);
							long v2 = m_memory.readLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 5);
							if (v != v2) {
								System.out.println("!!! Chained blocks long (5) writing/reading failed");
								System.exit(-1);
							}
						}
						{
							long v = 0x1122334455667788L;
							m_memory.writeLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 6, v);
							long v2 = m_memory.readLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 6);
							if (v != v2) {
								System.out.println("!!! Chained blocks long (6) writing/reading failed");
								System.exit(-1);
							}
						}
						{
							long v = 0x1122334455667788L;
							m_memory.writeLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 7, v);
							long v2 = m_memory.readLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 7);
							if (v != v2) {
								System.out.println("!!! Chained blocks long (7) writing/reading failed");
								System.exit(-1);
							}
						}
						{
							long v = 0x1122334455667788L;
							m_memory.writeLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 8, v);
							long v2 = m_memory.readLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 8);
							if (v != v2) {
								System.out.println("!!! Chained blocks long (8) writing/reading failed");
								System.exit(-1);
							}
						}
						{
							byte[] test = new byte[] {0x11, 0x22, 0x33, 0x44, 0x55};
							m_memory.writeBytes(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1, test, 0,
									test.length);
							byte[] test2 = new byte[test.length];
							m_memory.readBytes(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1, test2, 0,
									test2.length);
							for (int i = 0; i < test.length; i++) {
								if (test[i] != test2[i]) {
									System.out.println("!!! Chained blocks byte array writing/reading failed");
									System.exit(-1);
								}
							}
						}
						{
							short[] test = new short[] {0x11, 0x22, 0x33, 0x44, 0x55};
							m_memory.writeShorts(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1, test, 0,
									test.length);
							short[] test2 = new short[test.length];
							m_memory.readShorts(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1, test2, 0,
									test2.length);
							for (int i = 0; i < test.length; i++) {
								if (test[i] != test2[i]) {
									System.out.println("!!! Chained blocks short array writing/reading failed");
									System.exit(-1);
								}
							}
						}
						{
							int[] test = new int[] {0x11, 0x22, 0x33, 0x44, 0x55};
							m_memory.writeInts(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1, test, 0,
									test.length);
							int[] test2 = new int[test.length];
							m_memory.readInts(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1, test2, 0,
									test2.length);
							for (int i = 0; i < test.length; i++) {
								if (test[i] != test2[i]) {
									System.out.println("!!! Chained blocks int array writing/reading failed");
									System.exit(-1);
								}
							}
						}
						{
							long[] test = new long[] {0x11, 0x22, 0x33, 0x44, 0x55};
							m_memory.writeLongs(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1, test, 0,
									test.length);
							long[] test2 = new long[test.length];
							m_memory.readLongs(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK - 1, test2, 0,
									test2.length);
							for (int i = 0; i < test.length; i++) {
								if (test[i] != test2[i]) {
									System.out.println("!!! Chained blocks long array writing/reading failed");
									System.exit(-1);
								}
							}
						}
						{
							short v = 0x1122;
							m_memory.writeShort(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1, v);
							short v2 = m_memory.readShort(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1);
							if (v != v2) {
								System.out.println(
										"!!! Chained blocks short writing/reading to second full block failed");
								System.exit(-1);
							}
						}
						{
							int v = 0x11223344;
							m_memory.writeInt(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1, v);
							int v2 = m_memory.readInt(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1);
							if (v != v2) {
								System.out
										.println("!!! Chained blocks int writing/reading to second full block failed");
								System.exit(-1);
							}
						}
						{
							long v = 0x1122334455667788L;
							m_memory.writeLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1, v);
							long v2 = m_memory.readLong(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1);
							if (v != v2) {
								System.out
										.println("!!! Chained blocks long writing/reading to second full block failed");
								System.exit(-1);
							}
						}
						{
							byte[] test = new byte[] {0x11, 0x22, 0x33, 0x44, 0x55};
							m_memory.writeBytes(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1, test, 0,
									test.length);
							byte[] test2 = new byte[test.length];
							m_memory.readBytes(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1, test2, 0,
									test2.length);
							for (int i = 0; i < test.length; i++) {
								if (test[i] != test2[i]) {
									System.out.println(
											"!!! Chained blocks byte array writing/reading to second block failed");
									System.exit(-1);
								}
							}
						}
						{
							short[] test = new short[] {0x11, 0x22, 0x33, 0x44, 0x55};
							m_memory.writeShorts(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1, test, 0,
									test.length);
							short[] test2 = new short[test.length];
							m_memory.readShorts(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1, test2, 0,
									test2.length);
							for (int i = 0; i < test.length; i++) {
								if (test[i] != test2[i]) {
									System.out.println(
											"!!! Chained blocks short array writing/reading to second block failed");
									System.exit(-1);
								}
							}
						}
						{
							int[] test = new int[] {0x11, 0x22, 0x33, 0x44, 0x55};
							m_memory.writeInts(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1, test, 0,
									test.length);
							int[] test2 = new int[test.length];
							m_memory.readInts(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1, test2, 0,
									test2.length);
							for (int i = 0; i < test.length; i++) {
								if (test[i] != test2[i]) {
									System.out.println(
											"!!! Chained blocks int array writing/reading to second block failed");
									System.exit(-1);
								}
							}
						}
						{
							long[] test = new long[] {0x11, 0x22, 0x33, 0x44, 0x55};
							m_memory.writeLongs(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1, test, 0,
									test.length);
							long[] test2 = new long[test.length];
							m_memory.readLongs(ptr, SmallObjectHeapSegment.MAX_SIZE_MEMORY_BLOCK + 1, test2, 0,
									test2.length);
							for (int i = 0; i < test.length; i++) {
								if (test[i] != test2[i]) {
									System.out.println(
											"!!! Chained blocks long array writing/reading to second block failed");
									System.exit(-1);
								}
							}
						}
					}

					m_numMallocOperations--;
				} else if (m_numFreeOperations > 0) {
					// execute free if blocks allocated
					if (!m_blocksAlloced.isEmpty()) {
						Long memoryPtr = m_blocksAlloced.firstElement();
						m_blocksAlloced.remove(0);

						LOCK.lock();
						m_memory.free(memoryPtr);
						LOCK.unlock();
						if (m_debugPrint) {
							System.out.println(">>> Freed " + memoryPtr + ":\n" + m_memory);
						}

						m_numFreeOperations--;
					}
				}
			}
		}

		/**
		 * Print debug output.
		 */
		public void printDebug() {
			System.out.println(this);
		}

		@Override
		public String toString() {
			return "MemoryThread: mallocOperations " + m_numMallocOperations
					+ ", freeOperations: " + m_numFreeOperations
					+ ",  blockSizeMin: " + m_blockSizeMin
					+ ", blockSizeMax: " + m_blockSizeMax;
		}
	}
}

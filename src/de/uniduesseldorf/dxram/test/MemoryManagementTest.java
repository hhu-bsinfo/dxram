
package de.uniduesseldorf.dxram.test;

import java.text.NumberFormat;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import de.uniduesseldorf.dxram.core.chunk.storage.MemoryManager;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Tools;
import de.uniduesseldorf.dxram.utils.locks.JNILock;

/**
 * Test cases for the evaluation of the memory management
 * @author klein 26.03.2015
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.15
 */
public final class MemoryManagementTest {

	// Constants
	private static final String ARGUMENT_HELP = "-h";
	private static final String ARGUMENT_CHUNK_COUNT = "-c";
	private static final String ARGUMENT_THREAD_COUNT = "-t";
	private static final String ARGUMENT_MULTI = "-m";
	private static final String ARGUMENT_MAX_CHUNK_SIZE = "-s";
	private static final String ARGUMENT_RANDOM_CHUNK_SIZES = "-r";

	private static final int DEFAULT_CHUNK_COUNT = 100;
	private static final int DEFAULT_THREAD_COUNT = 1;
	private static final int DEFAULT_MULTI = 0;
	private static final int DEFAULT_MAX_CHUNK_SIZE = 64;
	private static final int DEFAULT_RANDOM_CHUNK_SIZES = 0;
	
	private static MemoryManager m_memoryManager;

	// Constructors
	/**
	 * Creates an instance of MemoryManagementTest
	 */
	private MemoryManagementTest() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		int chunkCount;
		int threadCount;
		int multiCount;
		int maxChunkSize;
		boolean randomChunkSizes;
		
		ArgumentHelper helper;
		TestResult result;
		
		//JNILock.load("/Users/rubbinnexx/Workspace/Uni/DXRAM/workspace/dxram/jni/libJNILock.dylib");
		JNILock.load("/home/stefan/Workspace/workspace_dxram/dxram/jni/libJNILock.so");

		helper = new ArgumentHelper(p_arguments);
		if (helper.containsArgument(ARGUMENT_HELP)) {
			System.out.println("Usage: LocalClientCachingTest [arguments]");
			System.out.println("Arguments:");
			System.out.println(ARGUMENT_CHUNK_COUNT + " number\t\tChunk Count");
			System.out.println(ARGUMENT_THREAD_COUNT + " number\t\tThread Count");

			System.exit(-1);
		} else {
			chunkCount = helper.getArgument(ARGUMENT_CHUNK_COUNT, DEFAULT_CHUNK_COUNT);
			threadCount = helper.getArgument(ARGUMENT_THREAD_COUNT, DEFAULT_THREAD_COUNT);
			multiCount = helper.getArgument(ARGUMENT_MULTI, DEFAULT_MULTI);
			maxChunkSize = helper.getArgument(ARGUMENT_MAX_CHUNK_SIZE, DEFAULT_MAX_CHUNK_SIZE);
			randomChunkSizes = helper.getArgument(ARGUMENT_RANDOM_CHUNK_SIZES, DEFAULT_RANDOM_CHUNK_SIZES) == 1;

			System.out.println();
			System.out.format("CHUNK_COUNT: %,d%n", chunkCount);
			System.out.format("THREAD_COUNT: %,d%n", threadCount);
			System.out.format("MULTI_COUNT: %,d%n", multiCount);
			System.out.format("MAX_CHUNKS_SIZE: %,d%n", maxChunkSize);
			System.out.format("RANDOM_CHUNK_SIZES: %,d%n", randomChunkSizes ? 1 : 0);

			try {
				init(chunkCount, threadCount);

				if (multiCount > 0)
				{
					// execute multi operations with given number of operations
					// executed as a multi operation
					result = evaluateMultiPuts(chunkCount, threadCount, maxChunkSize, randomChunkSizes, multiCount);
					printTestResult("MultiPuts", chunkCount, result);

					result = evaluateMultiGets(chunkCount, threadCount, maxChunkSize, multiCount);
					printTestResult("MultiGets", chunkCount, result);
					
					result = evaluateMultiGetsWithLength(chunkCount, threadCount, multiCount);
					printTestResult("MultiGetsWithLength", chunkCount, result);
				}
				else
				{
					result = evaluateSinglePuts(chunkCount, threadCount, maxChunkSize, randomChunkSizes);
					printTestResult("SinglePuts", chunkCount, result);

					result = evaluateSingleGets(chunkCount, threadCount, maxChunkSize);
					printTestResult("SingleGets", chunkCount, result);
					
					result = evaluateSingleGetsWithLengthGet(chunkCount, threadCount);
					printTestResult("SingleGetsWithLength", chunkCount, result);
				}

				deinit();
			} catch (final Throwable e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Initializes the memory management
	 * @param p_chunkCount
	 *            the number of chunks
	 * @param p_threadCount
	 *            the number of threads
	 * @throws MemoryException
	 *             if the memory management could not be initialized
	 */
	private static void init(final int p_chunkCount, final int p_threadCount) throws MemoryException {

		m_memoryManager = new MemoryManager(0);
		m_memoryManager.initialize(1073741824L * 8, 1073741824L * 8, false);
	}

	/**
	 * Deinitializes the memory management
	 * @throws MemoryException
	 *             if the memory management could not be deinitialized
	 */
	private static void deinit() throws MemoryException {
		m_memoryManager.disengage();
		m_memoryManager = null;
	}

	/**
	 * Evaluate the put operation
	 * @param p_chunkCount
	 *            the number of chunks
	 * @param p_threadCount
	 *            the number of threads
	 * @return the test result
	 * @throws InterruptedException
	 *             if the test is interrupted
	 * @throws ExecutionException
	 *             if the test could not be executed
	 */
	private static TestResult evaluateSinglePuts(final int p_chunkCount, final int p_threadCount, final int p_maxChunkSize, final boolean p_randomSizes) throws InterruptedException, ExecutionException {
		ExecutorService executorService;
		Future<?>[] futures;
		long time;

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_threadCount];
		time = System.nanoTime();
		for (int i = 0; i < p_threadCount; i++) {
			final int thread = i;
			futures[i] = executorService.submit(new Runnable() {
		
				@Override
				public void run() {
					final int threadID = thread;
					final int count = p_chunkCount / p_threadCount;
					// long chunkID;
					byte[] data;
					Random random;
					int bufferMaxSize = 0;

					random = new Random();
					bufferMaxSize = p_maxChunkSize;
					// have some random data
					data = new byte[bufferMaxSize];
					random.nextBytes(data);
					
					for (int i = 0; i < count; i++) {
						try {
							long chunkID;
							int size;
							
							if (p_randomSizes)
								size = random.nextInt(bufferMaxSize) + 1;
							else
								size = bufferMaxSize;
							
							m_memoryManager.lockManage();
							chunkID = m_memoryManager.create(size);
							if (chunkID == -1)
							{
								System.out.println("Failed creating chunk #" + i + " in thread " + threadID + " with size " + size);
								break;
							}
							
							if (m_memoryManager.put(chunkID, data, 0, size) != size)
							{
								System.out.println("Failed putting data for chunk #" + chunkID + " in thread " + threadID + " with size " + size);
								break;
							}
						} catch (final DXRAMException e) {
							e.printStackTrace();
						}
						finally
						{
							m_memoryManager.unlockManage();
						}
					}
				}

			});
		}

		for (final Future<?> future : futures) {
			future.get();
		}
		time = System.nanoTime() - time;
		executorService.shutdown();

		return new TestResult(time);
	}
	
	/**
	 * Evaluate the put operation
	 * @param p_chunkCount
	 *            the number of chunks
	 * @param p_threadCount
	 *            the number of threads
	 * @return the test result
	 * @throws InterruptedException
	 *             if the test is interrupted
	 * @throws ExecutionException
	 *             if the test could not be executed
	 */
	private static TestResult evaluateMultiPuts(final int p_chunkCount, final int p_threadCount, final int p_maxChunkSize, final boolean p_randomSizes, final int p_sizeChunkSet) throws InterruptedException, ExecutionException {
		ExecutorService executorService;
		Future<?>[] futures;
		long time;

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_threadCount];
		time = System.nanoTime();
		for (int i = 0; i < p_threadCount; i++) {
			final int thread = i;
			futures[i] = executorService.submit(new Runnable() {

				@Override
				public void run() {
					final int threadID = thread;
					final int count = p_chunkCount / p_threadCount;
					final int chunkSetCount = count / p_sizeChunkSet;
					// long chunkID;
					byte[] data;
					Random random;
					int bufferMaxSize = 0;

					random = new Random();
					bufferMaxSize = p_maxChunkSize;
					// have some random data
					data = new byte[bufferMaxSize];
					random.nextBytes(data);
					
					for (int j = 0; j < chunkSetCount; j++)
					{
						m_memoryManager.lockManage();
						for (int i = 0; i < p_sizeChunkSet; i++) {
							try {
								long chunkID;
								int size;
								
								if (p_randomSizes)
									size = random.nextInt(bufferMaxSize) + 1;
								else
									size = bufferMaxSize;
								
								
								chunkID = m_memoryManager.create(size);
								if (chunkID == -1)
								{
									System.out.println("Failed creating chunk #" + i + " in thread " + threadID + " with size 16.");
									break;
								}
								
								if (m_memoryManager.put(chunkID, data, 0, size) != size)
								{
									System.out.println("Failed putting data for chunk #" + i + " in thread " + threadID + " with size " + size);
									break;
								}
								
							} catch (final DXRAMException e) {
								e.printStackTrace();
							}
						}
						m_memoryManager.unlockManage();
					}
				}

			});
		}

		for (final Future<?> future : futures) {
			future.get();
		}
		time = System.nanoTime() - time;
		executorService.shutdown();

		return new TestResult(time);
	}

	/**
	 * Evaluate the get operation
	 * @param p_chunkCount
	 *            the number of chunks
	 * @param p_threadCount
	 *            the number of threads
	 * @return the test result
	 * @throws InterruptedException
	 *             if the test is interrupted
	 * @throws ExecutionException
	 *             if the test could not be executed
	 */
	private static TestResult evaluateSingleGets(final int p_chunkCount, final int p_threadCount, final int p_maxChunkSize) throws InterruptedException, ExecutionException {
		ExecutorService executorService;
		Future<?>[] futures;
		long time;

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_threadCount];
		time = System.nanoTime();
		for (int i = 0; i < p_threadCount; i++) {
			final int thread = i;
			futures[i] = executorService.submit(new Runnable() {

				@Override
				public void run() {
					final int threadID = thread;
					final int count = p_chunkCount / p_threadCount;
					byte[] buffer = new byte[p_maxChunkSize];

					for (int i = 1; i <= count; i++) {
						try {
							long chunkID;
							
							chunkID = (threadID * count) + i;
							
							m_memoryManager.lockAccess();
							if (m_memoryManager.get(chunkID, buffer, 0, buffer.length) == -1)
							{
								System.out.println("Failed getting data for chunk #" + chunkID + " in thread " + threadID + " with size " + buffer.length);
								break;
							}
						} catch (final DXRAMException e) {
							e.printStackTrace();
						}
						finally {
							m_memoryManager.unlockAccess();
						}
					}
				}

			});
		}

		for (final Future<?> future : futures) {
			future.get();
		}
		time = System.nanoTime() - time;
		executorService.shutdown();

		return new TestResult(time);
	}
	
	/**
	 * Evaluate the get operation
	 * @param p_chunkCount
	 *            the number of chunks
	 * @param p_threadCount
	 *            the number of threads
	 * @return the test result
	 * @throws InterruptedException
	 *             if the test is interrupted
	 * @throws ExecutionException
	 *             if the test could not be executed
	 */
	private static TestResult evaluateSingleGetsWithLengthGet(final int p_chunkCount, final int p_threadCount) throws InterruptedException, ExecutionException {
		ExecutorService executorService;
		Future<?>[] futures;
		long time;

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_threadCount];
		time = System.nanoTime();
		for (int i = 0; i < p_threadCount; i++) {
			final int thread = i;
			futures[i] = executorService.submit(new Runnable() {

				@Override
				public void run() {
					final int threadID = thread;
					final int count = p_chunkCount / p_threadCount;
					
					for (int i = 1; i <= count; i++) {
						try {
							int size;
							long chunkID;
							chunkID = (threadID * count) + i;
							
							m_memoryManager.lockAccess();
							size = m_memoryManager.getSize(chunkID);
							if (size == -1)
							{
								System.out.println("Failed getting size for chunk #" + chunkID + " in thread " + threadID);
								break;
							}
							byte[] buffer = new byte[size];
							if (m_memoryManager.get(chunkID, buffer, 0, buffer.length) != size)
							{
								System.out.println("Failed getting data for chunk #" + chunkID + " in thread " + threadID + " with size " + size);
								break;
							}
						} catch (final DXRAMException e) {
							e.printStackTrace();
						} finally {
							m_memoryManager.unlockAccess();
						}
					}
				}

			});
		}

		for (final Future<?> future : futures) {
			future.get();
		}
		time = System.nanoTime() - time;
		executorService.shutdown();

		return new TestResult(time);
	}
	
	/**
	 * Evaluate the get operation
	 * @param p_chunkCount
	 *            the number of chunks
	 * @param p_threadCount
	 *            the number of threads
	 * @return the test result
	 * @throws InterruptedException
	 *             if the test is interrupted
	 * @throws ExecutionException
	 *             if the test could not be executed
	 */
	private static TestResult evaluateMultiGets(final int p_chunkCount, final int p_threadCount, final int p_maxChunkSize, final int p_sizeChunkSet) throws InterruptedException, ExecutionException {
		ExecutorService executorService;
		Future<?>[] futures;
		long time;

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_threadCount];
		time = System.nanoTime();
		for (int i = 0; i < p_threadCount; i++) {
			final int thread = i;
			futures[i] = executorService.submit(new Runnable() {

				@Override
				public void run() {
					final int threadID = thread;
					final int count = p_chunkCount / p_threadCount;
					final int chunkSetCount = count / p_sizeChunkSet;
					byte[] buffer = new byte[p_maxChunkSize];
					long chunkID = -1;

					for (int j = 0; j < chunkSetCount; j++)
					{
						m_memoryManager.lockAccess();
						for (int i = 1; i <= p_sizeChunkSet; i++) {
							try {
								chunkID = (threadID * count) + (j * p_sizeChunkSet) + i;
								if (m_memoryManager.get(chunkID, buffer, 0, buffer.length) == -1)
								{
									System.out.println("Failed getting data for chunk #" + chunkID + " in thread " + threadID + " with size " + buffer.length);
									break;
								}
							} catch (final DXRAMException e) {
								e.printStackTrace();
							}
						}
						m_memoryManager.unlockAccess();
					}
				}

			});
		}

		for (final Future<?> future : futures) {
			future.get();
		}
		time = System.nanoTime() - time;
		executorService.shutdown();

		return new TestResult(time);
	}
	
	/**
	 * Evaluate the get operation
	 * @param p_chunkCount
	 *            the number of chunks
	 * @param p_threadCount
	 *            the number of threads
	 * @return the test result
	 * @throws InterruptedException
	 *             if the test is interrupted
	 * @throws ExecutionException
	 *             if the test could not be executed
	 */
	private static TestResult evaluateMultiGetsWithLength(final int p_chunkCount, final int p_threadCount, final int p_sizeChunkSet) throws InterruptedException, ExecutionException {
		ExecutorService executorService;
		Future<?>[] futures;
		long time;

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_threadCount];
		time = System.nanoTime();
		for (int i = 0; i < p_threadCount; i++) {
			final int thread = i;
			futures[i] = executorService.submit(new Runnable() {

				@Override
				public void run() {
					final int threadID = thread;
					final int count = p_chunkCount / p_threadCount;
					final int chunkSetCount = count / p_sizeChunkSet;

					for (int j = 0; j < chunkSetCount; j++)
					{
						m_memoryManager.lockAccess();
						for (int i = 1; i <= p_sizeChunkSet; i++) {
							try {
								long chunkID;
								int size;
								
								chunkID = (threadID * count) + (j * p_sizeChunkSet) + i;
								
								size = m_memoryManager.getSize(chunkID);
								if (size == -1)
								{
									System.out.println("Failed getting size of chunk #" + chunkID + " in thread " + threadID);
									break;
								}
								byte[] buffer = new byte[size];
								
								if (m_memoryManager.get(chunkID, buffer, 0, buffer.length) != size)
								{
									System.out.println("Failed getting data for chunk #" + chunkID + " in thread " + threadID + " with size " + size);
									break;
								}
							} catch (final DXRAMException e) {
								e.printStackTrace();
							}
						}
						m_memoryManager.unlockAccess();
					}
				}

			});
		}

		for (final Future<?> future : futures) {
			future.get();
		}
		time = System.nanoTime() - time;
		executorService.shutdown();

		return new TestResult(time);
	}

	/**
	 * Prints a test result
	 * @param p_test
	 *            the test name
	 * @param p_chunkCount
	 *            the number of chunks
	 * @param p_result
	 *            the test result
	 */
	private static void printTestResult(final String p_test, final int p_chunkCount, final TestResult p_result) {
		StringBuffer output;
		NumberFormat format;

		format = NumberFormat.getNumberInstance();
		output = new StringBuffer();

		output.append("\n" + p_test + ":");
		output.append("\nOverall Time:\t" + Tools.readableNanoTime(p_result.getTime()));
		output.append("\nTime / Op (ns):\t" + p_result.getTime());
		output.append("\nTime / Op (ns):\t" + format.format(p_result.getTime() / (p_chunkCount * 1.0)));
		output.append("\nOps / Second:\t" + format.format(p_chunkCount * 1000000000.0 / p_result.getTime()));

		System.out.println(output);
	}

	// Classes
	/**
	 * Represents a test result
	 * @author klein 26.03.2015
	 */
	private static final class TestResult {

		// Attributes
		private long m_time;

		/**
		 * Creates an instance of MemoryManagementTest
		 * @param p_time
		 *            the test time
		 */
		TestResult(final long p_time) {
			m_time = p_time;
		}

		/**
		 * Returns the test time
		 * @return the test time
		 */
		public long getTime() {
			return m_time;
		}

	}

}

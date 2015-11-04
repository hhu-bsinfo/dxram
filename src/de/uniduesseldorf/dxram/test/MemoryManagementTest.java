
package de.uniduesseldorf.dxram.test;

import java.text.NumberFormat;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.mem.RawMemory;
import de.uniduesseldorf.dxram.core.chunk.storage.CIDTable;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Tools;

/**
 * Test cases for the evaluation of the memory management
 * @author klein 26.03.2015
 */
public final class MemoryManagementTest {

	// Constants
	private static final String ARGUMENT_HELP = "-h";
	private static final String ARGUMENT_CHUNK_COUNT = "-c";
	private static final String ARGUMENT_THREAD_COUNT = "-t";

	private static final int DEFAULT_CHUNK_COUNT = 1;
	private static final int DEFAULT_THREAD_COUNT = 1;

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
		ArgumentHelper helper;
		TestResult result;

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

			System.out.println();
			System.out.format("CHUNK_COUNT: %,d%n", chunkCount);
			System.out.format("THREAD_COUNT: %,d%n", threadCount);

			try {
				init(chunkCount, threadCount);

				result = evaluatePuts(chunkCount, threadCount);
				printTestResult("Puts", chunkCount, result);

				result = evaluateGets(chunkCount, threadCount);
				printTestResult("Gets", chunkCount, result);

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

		// Initialize DXRAM
		try {
			Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
					NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
		} catch (final DXRAMException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * Deinitializes the memory management
	 * @throws MemoryException
	 *             if the memory management could not be deinitialized
	 */
	private static void deinit() throws MemoryException {
//		CIDTable.printDebugInfos();
//		RawMemory.printDebugInfos();

		Core.close();
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
	private static TestResult evaluatePuts(final int p_chunkCount, final int p_threadCount) throws InterruptedException, ExecutionException {
		ExecutorService executorService;
		Future<?>[] futures;
		long time;

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_threadCount];
		time = System.nanoTime();
		for (int i = 0; i < p_threadCount; i++) {
			futures[i] = executorService.submit(new Runnable() {

				@Override
				public void run() {
					final int count = p_chunkCount / p_threadCount;
					Chunk chunk;
					// long chunkID;
					byte[] data;
					Random random;

					random = new Random();
					for (int i = 1; i <= count; i++) {
						try {
							// chunkID = MemoryManager.getNextLocalID().getLocalID();
							data = new byte[random.nextInt(49) + 16];
							// chunk = new Chunk(chunkID, data, 0);
							//
							// MemoryManager.put(chunk);
							chunk = Core.createNewChunk(data.length);
							chunk.getData().put(data);
							Core.put(chunk);
						} catch (final DXRAMException e) {
							e.printStackTrace();
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
	private static TestResult evaluateGets(final int p_chunkCount, final int p_threadCount) throws InterruptedException, ExecutionException {
		ExecutorService executorService;
		Future<?>[] futures;
		long time;

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_threadCount];
		time = System.nanoTime();
		for (int i = 0; i < p_threadCount; i++) {
			futures[i] = executorService.submit(new Runnable() {

				@Override
				public void run() {
					final int count = p_chunkCount / p_threadCount;

					for (int i = 1; i <= count; i++) {
						try {
							Core.get(i);
						} catch (final DXRAMException e) {
							e.printStackTrace();
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
		output.append("\nTime / Op:\t" + format.format(p_result.getTime() / (p_chunkCount * 1.0)) + " nanoseconds");
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
		public TestResult(final long p_time) {
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

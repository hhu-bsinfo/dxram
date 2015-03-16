package de.uniduesseldorf.dxram.test;

import java.text.NumberFormat;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.storage.CIDTable;
import de.uniduesseldorf.dxram.core.chunk.storage.MemoryManager;
import de.uniduesseldorf.dxram.core.chunk.storage.RawMemory;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.utils.Tools;

public class MemoryManagementTest {

	// Constants
	private static final String ARGUMENT_HELP = "-h";
	private static final String ARGUMENT_CHUNK_COUNT = "-c";
	private static final String ARGUMENT_THREAD_COUNT = "-t";

	private static final int DEFAULT_CHUNK_COUNT = 1000000;
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
	public static void main(String[] p_arguments) {
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

	private static void init(final int p_chunkCount, final int p_threadCount) throws MemoryException {
		long size;

		size = p_threadCount - 1;
		size *= 1 << 30;
		size += p_chunkCount * 50L;

		MemoryManager.initialize(size);
	}

	private static void deinit() throws MemoryException {
		CIDTable.printDebugInfos();
		RawMemory.printDebugInfos();

		MemoryManager.disengage();
	}

	private static TestResult evaluatePuts(final int p_chunkCount, final int p_threadCount)
			throws InterruptedException, ExecutionException {
		ExecutorService executorService;
		Future<?>[] futures;
		long time;

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_threadCount];
		time = System.nanoTime();
		for (int i = 0;i < p_threadCount;i++) {
			futures[i] = executorService.submit(new Runnable() {

				@Override
				public void run() {
					final int COUNT = p_chunkCount / p_threadCount;
					Chunk chunk;
					long chunkID;
					byte[] data;
					Random random;

					random = new Random();
					for (int i = 1;i <= COUNT;i++) {
						try {
							chunkID = MemoryManager.getNextLocalID();
							data = new byte[random.nextInt(49) + 16];
							chunk = new Chunk(chunkID, data);

							MemoryManager.put(chunk);
						} catch (MemoryException e) {
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

	private static TestResult evaluateGets(final int p_chunkCount, final int p_threadCount)
			throws InterruptedException, ExecutionException {
		ExecutorService executorService;
		Future<?>[] futures;
		long time;

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_threadCount];
		time = System.nanoTime();
		for (int i = 0;i < p_threadCount;i++) {
			futures[i] = executorService.submit(new Runnable() {

				@Override
				public void run() {
					final int COUNT = p_chunkCount / p_threadCount;

					for (int i = 1;i <= COUNT;i++) {
						try {
							MemoryManager.get(i);
						} catch (MemoryException e) {
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

	private static void printTestResult(final String p_test, final int p_chunkCount, final TestResult p_result) {
		StringBuffer output;
		NumberFormat format;

		format = NumberFormat.getNumberInstance();
		output = new StringBuffer();

		output.append("\n" + p_test + ":");
		output.append("\nOverall Time:\t" + Tools.readableNanoTime(p_result.getTime()));
		output.append("\nTime / Op:\t" + format.format(p_result.getTime() / (p_chunkCount * 1.0)) + " nanoseconds");
		output.append("\nOps / Second:\t" + format.format((p_chunkCount * 1000000000.0) / p_result.getTime()));

		System.out.println(output);
	}

	// Classes
	private static final class TestResult {

		// Attributes
		private long time;

		/**
		 * Creates an instance of MemoryManagementTest
		 * @param p_time
		 *            the test time
		 */
		public TestResult(long p_time) {
			time = p_time;
		}

		/**
		 * Returns the test time
		 * @return the test time
		 */
		public long getTime() {
			return time;
		}

	}

}

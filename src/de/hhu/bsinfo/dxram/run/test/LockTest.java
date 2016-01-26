
package de.hhu.bsinfo.dxram.run.test;

import java.text.NumberFormat;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.utils.Tools;
import de.hhu.bsinfo.utils.locks.JNISpinLock;
import de.hhu.bsinfo.utils.locks.NoLock;
import de.hhu.bsinfo.utils.locks.SpinLock;

/**
 * Test cases for evaluating lock implementations
 * @author klein 26.03.2015
 */
public final class LockTest {

	// Constants
	private static final String ARGUMENT_HELP = "-h";
	private static final String ARGUMENT_ROUNDS = "-r";
	private static final String ARGUMENT_THREAD_COUNT = "-t";

	private static final int DEFAULT_ROUNDS = 1000000000;
	private static final int DEFAULT_THREAD_COUNT = 1;

	// Constructors
	/**
	 * Creates an instance of MemoryManagementTest
	 */
	private LockTest() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		int rounds;
		int threadCount;
		ArgumentHelper helper;
		TestResult result;

		helper = new ArgumentHelper(p_arguments);
		if (helper.containsArgument(ARGUMENT_HELP)) {
			System.out.println("Usage: LocalClientCachingTest [arguments]");
			System.out.println("Arguments:");
			System.out.println(ARGUMENT_ROUNDS + " number\t\tRounds");
			System.out.println(ARGUMENT_THREAD_COUNT + " number\t\tThread Count");

			System.exit(-1);
		} else {
			rounds = helper.getArgument(ARGUMENT_ROUNDS, DEFAULT_ROUNDS);
			threadCount = helper.getArgument(ARGUMENT_THREAD_COUNT, DEFAULT_THREAD_COUNT);

			System.out.println();
			System.out.format("ROUNDS: %,d%n", rounds);
			System.out.format("THREAD_COUNT: %,d%n", threadCount);

			try {
				result = evaluateNoLock(rounds, threadCount);
				printTestResult("NoLock", rounds, result);

				result = evaluateJNILock(rounds, threadCount);
				printTestResult("JNILock", rounds, result);

				result = evaluateSpinLock(rounds, threadCount);
				printTestResult("SpinLock", rounds, result);

				result = evaluateJavaLock(rounds, threadCount);
				printTestResult("JavaLock", rounds, result);
			} catch (final Throwable e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Evaluates the NoLock implementation
	 * @param p_rounds
	 *            the number of rounds
	 * @param p_threadCount
	 *            the number of threads
	 * @return the result
	 * @throws InterruptedException
	 *             if the test is interrupted
	 * @throws ExecutionException
	 *             if the test could not be executed
	 */
	private static TestResult evaluateNoLock(final int p_rounds, final int p_threadCount) throws InterruptedException, ExecutionException {
		long time;

		time = evaluateLock(new NoLock(), p_rounds, p_threadCount);

		return new TestResult(time);
	}

	/**
	 * Evaluates the JNILock implementation
	 * @param p_rounds
	 *            the number of rounds
	 * @param p_threadCount
	 *            the number of threads
	 * @return the result
	 * @throws InterruptedException
	 *             if the test is interrupted
	 * @throws ExecutionException
	 *             if the test could not be executed
	 */
	private static TestResult evaluateJNILock(final int p_rounds, final int p_threadCount) throws InterruptedException, ExecutionException {
		long time;

		time = evaluateLock(new JNISpinLock(), p_rounds, p_threadCount);

		return new TestResult(time);
	}

	/**
	 * Evaluates the SpinLock implementation
	 * @param p_rounds
	 *            the number of rounds
	 * @param p_threadCount
	 *            the number of threads
	 * @return the result
	 * @throws InterruptedException
	 *             if the test is interrupted
	 * @throws ExecutionException
	 *             if the test could not be executed
	 */
	private static TestResult evaluateSpinLock(final int p_rounds, final int p_threadCount) throws InterruptedException, ExecutionException {
		long time;

		time = evaluateLock(new SpinLock(), p_rounds, p_threadCount);

		return new TestResult(time);
	}

	/**
	 * Evaluates the ReentrantLock implementation
	 * @param p_rounds
	 *            the number of rounds
	 * @param p_threadCount
	 *            the number of threads
	 * @return the result
	 * @throws InterruptedException
	 *             if the test is interrupted
	 * @throws ExecutionException
	 *             if the test could not be executed
	 */
	private static TestResult evaluateJavaLock(final int p_rounds, final int p_threadCount) throws InterruptedException, ExecutionException {
		long time;

		time = evaluateLock(new ReentrantLock(false), p_rounds, p_threadCount);

		return new TestResult(time);
	}

	/**
	 * Evaluates a lock implementation
	 * @param p_lock
	 *            the lock implementation
	 * @param p_rounds
	 *            the number of rounds
	 * @param p_threadCount
	 *            the number of threads
	 * @return the result
	 * @throws InterruptedException
	 *             if the test is interrupted
	 * @throws ExecutionException
	 *             if the test could not be executed
	 */
	private static long evaluateLock(final Lock p_lock, final int p_rounds, final int p_threadCount) throws InterruptedException, ExecutionException {
		ExecutorService executorService;
		Future<?>[] futures;
		long ret;

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_threadCount];
		ret = System.nanoTime();
		for (int i = 0; i < p_threadCount; i++) {
			futures[i] = executorService.submit(new Runnable() {

				@Override
				public void run() {
					final int count = p_rounds / p_threadCount;

					for (int i = 1; i <= count; i++) {
						p_lock.lock();
						p_lock.unlock();
					}
				}

			});
		}

		for (final Future<?> future : futures) {
			future.get();
		}
		ret = System.nanoTime() - ret;
		executorService.shutdown();

		return ret;
	}

	/**
	 * Prints a test result
	 * @param p_test
	 *            the test name
	 * @param p_rounds
	 *            the number of rounds
	 * @param p_result
	 *            the test result
	 */
	private static void printTestResult(final String p_test, final int p_rounds, final TestResult p_result) {
		StringBuffer output;
		NumberFormat format;

		format = NumberFormat.getNumberInstance();
		output = new StringBuffer();

		output.append("\n" + p_test + ":");
		output.append("\nOverall Time:\t" + Tools.readableNanoTime(p_result.getTime()));
		output.append("\nTime / Op:\t" + format.format(p_result.getTime() / (p_rounds * 1.0)) + " nanoseconds");
		output.append("\nOps / Second:\t" + format.format(p_rounds / (p_result.getTime() / 1000000000.0)));

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

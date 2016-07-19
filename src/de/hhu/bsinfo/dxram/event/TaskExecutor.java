
package de.hhu.bsinfo.dxram.event;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.dxram.logger.LoggerComponent;

/**
 * Access to a singleton of an ExecutorService
 * for usage inside of the network package
 * NOTE:
 * The currently running Task has to stay in the task map.
 * Otherwise the Queue will never be used.
 * @see java.util.concurrent.ExecutorService
 * @author Marc Ewert 14.08.14
 */
public final class TaskExecutor {

	private LoggerComponent m_logger;
	private final ExecutorService m_executor;
	private final String m_name;

	// Constructors
	/**
	 * Creates a new TaskExecutor
	 * @param p_name
	 *            Identifier for debug prints
	 * @param p_threads
	 *            Number of Threads to create
	 */
	public TaskExecutor(final String p_name, final int p_threads, final LoggerComponent p_logger) {
		m_logger = p_logger;
		m_name = p_name;

		m_executor = Executors.newFixedThreadPool(p_threads, new ExecutorThreadFactory());
	}

	// Methods
	/**
	 * Add a task to the queue to be executed
	 * @param p_runnable
	 *            Task to be executed
	 */
	public void execute(final Runnable p_runnable) {
		try {
			m_executor.execute(p_runnable);
		} catch (final RejectedExecutionException e) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass().getSimpleName(), m_name + ":" + e.getMessage());
			// #endif /* LOGGER >= ERROR */
		}
	}

	/**
	 * Initiate a graceful shutdown of the thread pool
	 */
	public void shutdown() {
		m_executor.shutdown();
	}

	/**
	 * Waits until thread pool is terminated
	 * @return whether the shut-down is finished or not
	 * @throws InterruptedException
	 *             if awaiting termination was interrupted
	 */
	public boolean awaitTermination() throws InterruptedException {
		return m_executor.awaitTermination(100, TimeUnit.MILLISECONDS);
	}

	/**
	 * Creates new Threads for the TaskExecutor
	 * @author Marc Ewert 01.10.14
	 */
	private class ExecutorThreadFactory implements ThreadFactory {

		private final AtomicInteger m_threadNumber;

		/**
		 * Creates a new ExecutorThreadFactory
		 */
		ExecutorThreadFactory() {
			m_threadNumber = new AtomicInteger(1);
		}

		@Override
		public Thread newThread(final Runnable p_runnable) {
			String name;
			Thread thread;

			name = m_name + "-thread-" + m_threadNumber.getAndIncrement();
			thread = new Thread(p_runnable, name);
			thread.setDaemon(true);
			thread.setPriority(Thread.NORM_PRIORITY);

			return thread;
		}
	}
}

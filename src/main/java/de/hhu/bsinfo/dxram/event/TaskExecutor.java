/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.event;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Access to a singleton of an ExecutorService
 * for usage inside of the network package
 * NOTE:
 * The currently running TaskScript has to stay in the task map.
 * Otherwise the Queue will never be used.
 *
 * @author Marc Ewert, mark.ewert@hhu.de, 14.08.14
 * @see java.util.concurrent.ExecutorService
 */
final class TaskExecutor {
    private final Logger LOGGER;

    private final ExecutorService m_executor;
    private final String m_name;

    // Constructors

    /**
     * Creates a new TaskExecutor
     *
     * @param p_name
     *         Identifier for debug prints
     * @param p_threads
     *         Number of Threads to create
     */
    TaskExecutor(final String p_name, final int p_threads) {
        LOGGER = LogManager.getFormatterLogger(TaskExecutor.class.getCanonicalName() + ' ' + p_name);
        m_name = p_name;

        m_executor = Executors.newFixedThreadPool(p_threads, new ExecutorThreadFactory());
    }

    // Methods

    /**
     * Add a task to the queue to be executed
     *
     * @param p_runnable
     *         TaskScript to be executed
     */
    public void execute(final Runnable p_runnable) {
        try {
            m_executor.execute(p_runnable);
        } catch (final RejectedExecutionException e) {

            LOGGER.error(e);

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
     *
     * @return whether the shut-down is finished or not
     * @throws InterruptedException
     *         if awaiting termination was interrupted
     */
    boolean awaitTermination() throws InterruptedException {
        return m_executor.awaitTermination(100, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates new Threads for the TaskExecutor
     *
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

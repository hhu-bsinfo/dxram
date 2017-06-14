/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.ethnet.core;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The MessageCreator builds messages and forwards them to the MessageHandlers.
 * Uses a ring-buffer implementation for incoming buffers.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.05.2016
 */
public class MessageCreator extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MessageCreator.class.getSimpleName());

    // Attributes
    private volatile boolean m_shutdown;

    private Object[] m_buffer;
    private int m_size;
    private int m_maxBytes;
    private long m_currentBytes;

    private volatile int m_posFront;
    private volatile int m_posBack;
    private ReentrantLock m_lock;

    /**
     * Creates an instance of MessageCreator
     *
     * @param p_osBufferSize
     *         the incoming buffer size
     */
    public MessageCreator(final int p_osBufferSize) {
        m_size = 2 * (2 * 1024 + 1);
        m_buffer = new Object[m_size * 2];
        m_maxBytes = p_osBufferSize * 8;
        m_currentBytes = 0;

        m_posFront = 0;
        m_posBack = 0;
        m_lock = new ReentrantLock(false);
    }

    /**
     * Returns whether the ring-buffer is empty or not.
     *
     * @return whether the ring-buffer is empty or not
     */
    public boolean isEmpty() {
        return m_posFront == m_posBack;
    }

    /**
     * Returns whether the ring-buffer is full or not.
     *
     * @return whether the ring-buffer is full or not
     */
    public boolean isFull() {
        return (m_posBack + 2) % m_size == m_posFront % m_size;
    }

    /**
     * Returns the number of pending buffers.
     *
     * @return the number of pending buffers
     */
    public int size() {
        return (m_posBack - m_posFront) / 2;
    }

    /**
     * When an object implementing interface {@code Runnable} is used
     * to create a thread, starting the thread causes the object's {@code run} method to be called in that
     * separately executing
     * thread.
     * The general contract of the method {@code run} is that it may take any action whatsoever.
     *
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        Object[] job;
        AbstractConnection connection;
        ByteBuffer buffer;

        job = new Object[2];
        while (!m_shutdown) {
            if (popJob(job)) {
                connection = (AbstractConnection) job[0];
                buffer = (ByteBuffer) job[1];
                connection.getPipeIn().processBuffer(buffer);
                connection.returnProcessedBuffer(buffer);
            } else {
                LockSupport.park();
            }
        }
    }

    /**
     * Shutdown
     */
    public void shutdown() {
        // #if LOGGER == INFO
        LOGGER.info("Message creator shutdown...");
        // #endif /* LOGGER == INFO */

        m_shutdown = true;

        try {
            // wait a moment for the thread to shut down (if it can)
            Thread.sleep(100);
        } catch (final InterruptedException ignore) {

        }

        interrupt();
        LockSupport.unpark(this);
        try {
            join();
        } catch (final InterruptedException ignore) {

        }
    }

    /**
     * Adds a job (connection and incoming buffer) at the end of the buffer.
     *
     * @param p_connection
     *         the connection associated with the buffer
     * @param p_buffer
     *         the incoming buffer
     * @return whether the job was added or not
     */
    public boolean pushJob(final AbstractConnection p_connection, final ByteBuffer p_buffer) {
        boolean locked = false;

        if (m_posFront != m_posBack) {
            m_lock.lock();
            locked = true;
        }

        if ((m_posBack + 2) % m_size == m_posFront % m_size || m_currentBytes >= m_maxBytes) {
            // Return without adding the job if queue is full or too many bytes are pending
            if (locked) {
                m_lock.unlock();
            }

            return false;
        }

        m_buffer[m_posBack % m_size] = p_connection;
        m_buffer[(m_posBack + 1) % m_size] = p_buffer;
        m_currentBytes += p_buffer.remaining();
        m_posBack += 2;

        if (locked) {
            m_lock.unlock();
        }
        LockSupport.unpark(this);

        return true;
    }

    /**
     * Gets a job (connection and incoming buffer) from the beginning of the buffer.
     *
     * @param p_job
     *         the job array to be filled
     * @return whether the job array was filled or not
     */
    private boolean popJob(final Object[] p_job) {
        m_lock.lock();

        if (m_posFront == m_posBack) {
            // Ring-buffer is empty.
            m_lock.unlock();
            return false;
        }

        p_job[0] = m_buffer[m_posFront % m_size];
        p_job[1] = m_buffer[(m_posFront + 1) % m_size];
        m_currentBytes -= ((ByteBuffer) p_job[1]).remaining();
        m_posFront += 2;
        m_lock.unlock();

        return true;
    }
}

/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.ethnet;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The MessageCreator builds messages and forwards them to the MessageHandlers.
 * Uses a ring-buffer implementation for incoming buffers.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.05.2016
 */
class MessageCreator extends Thread {

    // Constants
    private static final long MAX_PENDING_BYTES = Integer.MAX_VALUE;

    // Attributes
    private volatile boolean m_shutdown;

    private Object[] m_buffer;
    private int m_size;
    private long m_currentBytes;

    private int m_posFront;
    private int m_posBack;
    private ReentrantLock m_lock;
    private Condition m_cond;

    /**
     * Creates an instance of IncomingBufferStorageAndMessageCreator
     *
     * @param p_incomingBufferLimitPerConnection
     *     the maximal number of pending incoming buffers
     */
    MessageCreator(final int p_incomingBufferLimitPerConnection) {
        m_size = p_incomingBufferLimitPerConnection * ConnectionManager.MAX_CONNECTIONS;
        m_buffer = new Object[m_size * 2];
        m_currentBytes = 0;

        m_posFront = 0;
        m_posBack = 0;
        m_lock = new ReentrantLock(false);
        m_cond = m_lock.newCondition();
    }

    /**
     * Returns whether the ring-buffer is empty or not.
     *
     * @return whether the ring-buffer is empty or not
     */
    public boolean isEmpty() {
        boolean ret;

        m_lock.lock();
        ret = m_posFront == m_posBack;
        m_lock.unlock();

        return ret;
    }

    /**
     * Returns whether the ring-buffer is full or not.
     *
     * @return whether the ring-buffer is full or not
     */
    public boolean isFull() {
        boolean ret;

        m_lock.lock();
        ret = (m_posBack + 1) % m_size == m_posFront % m_size;
        m_lock.unlock();

        return ret;
    }

    /**
     * Returns whether the ring-buffer is full or not.
     * Result might be out-dated as there is no lock acquired.
     *
     * @return whether the ring-buffer is full or not
     */
    boolean isFullLazy() {
        return (m_posBack + 1) % m_size == m_posFront % m_size;
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
        NIOConnection connection;
        ByteBuffer buffer;

        job = new Object[2];
        while (!m_shutdown) {
            m_lock.lock();
            if (popJob(job)) {
                m_lock.unlock();
                connection = (NIOConnection) job[0];
                buffer = (ByteBuffer) job[1];
                connection.processBuffer(buffer);
            } else {
                try {
                    m_cond.await();
                } catch (final InterruptedException ignored) {
                    break;
                }
                m_lock.unlock();
            }
        }
    }

    /**
     * Shutdown
     */
    protected void shutdown() {
        m_shutdown = true;

        try {
            // wait a moment for the thread to shut down (if it can)
            Thread.sleep(100);
        } catch (final InterruptedException ignore) {

        }

        interrupt();
        try {
            join();
        } catch (final InterruptedException ignore) {

        }
    }

    /**
     * Returns the number of pending buffers.
     *
     * @return the number of pending buffers
     */
    protected int size() {
        int ret;

        m_lock.lock();
        if (m_posFront <= m_posBack) {
            ret = (m_posBack - m_posFront) / 2;
        } else {
            ret = (m_buffer.length - m_posFront + m_posBack) / 2;
        }
        m_lock.unlock();

        return ret;
    }

    /**
     * Adds a job (connection and incoming buffer) at the end of the buffer.
     *
     * @param p_connection
     *     the connection associated with the buffer
     * @param p_buffer
     *     the incoming buffer
     * @return whether the job was added or not
     */
    boolean pushJob(final NIOConnection p_connection, final ByteBuffer p_buffer) {
        m_lock.lock();
        int posBack = m_posBack;

        if ((posBack + 1) % m_size == m_posFront % m_size || m_currentBytes >= MAX_PENDING_BYTES) {
            // Return without adding the job if queue is full or too many bytes are pending
            m_lock.unlock();
            return false;
        }

        int posBack2 = posBack % m_size;
        m_buffer[posBack2 * 2 % m_buffer.length] = p_connection;
        m_buffer[(posBack2 * 2 + 1) % m_buffer.length] = p_buffer;
        m_posBack = posBack + 1;

        m_currentBytes += p_buffer.remaining();

        m_cond.signalAll();
        m_lock.unlock();

        return true;
    }

    /**
     * Gets a job (connection and incoming buffer) from the beginning of the buffer.
     *
     * @param p_job
     *     the job array to be filled
     * @return whether the job array was filled or not
     */
    private boolean popJob(final Object[] p_job) {
        int posFront = m_posFront;
        int posFront2 = posFront % m_size;

        if (posFront2 == m_posBack % m_size) {
            // Ring-buffer is empty.
            return false;
        }

        p_job[0] = m_buffer[posFront2 * 2 % m_buffer.length];
        p_job[1] = m_buffer[(posFront2 * 2 + 1) % m_buffer.length];
        m_posFront = posFront + 1;

        m_currentBytes -= ((ByteBuffer) p_job[1]).remaining();

        return true;
    }
}

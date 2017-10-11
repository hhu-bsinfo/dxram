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

package de.hhu.bsinfo.dxnet.loopback;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages the network connections
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 */
class LoopbackSendThread extends Thread {

    // Constants
    private static final Logger LOGGER = LogManager.getFormatterLogger(LoopbackSendThread.class.getSimpleName());

    // Attributes
    private AtomicBoolean m_send;
    private volatile LoopbackConnection m_connection;
    private volatile boolean m_running;

    // Constructors

    /**
     * Creates an instance of LoopbackSendThread
     *
     * @param p_connectionTimeout
     *         the connection timeout
     * @param p_osBufferSize
     *         the size of incoming and outgoing buffers
     */
    LoopbackSendThread(final LoopbackConnectionManager p_connectionManager, final int p_connectionTimeout, final int p_osBufferSize) {
        m_send = new AtomicBoolean(false);
        m_running = true;
    }

    @Override
    public void run() {
        long time;

        // Wait until connection was created
        while (m_connection == null) {
            try {
                Thread.sleep(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        while (m_running) {
            time = System.nanoTime();
            while (!m_send.compareAndSet(true, false)) {
                //Thread.yield();
                LockSupport.parkNanos(1);

                if (System.nanoTime() - time > 1000 * 1000) {
                    break;
                }
            }

            try {
                m_connection.getPipeOut().write();
            } catch (final IOException ignore) {

            }
        }
    }

    protected void trigger(LoopbackConnection p_connection) {
        m_connection = p_connection;
        m_send.set(true);
    }

    /**
     * Closes the Worker
     */
    protected void close() {
        m_running = false;
    }
}

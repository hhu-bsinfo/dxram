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

import de.hhu.bsinfo.utils.UnsafeHandler;

/**
 * Manages the network connections
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 */
class LoopbackSendThread extends Thread {

    // Constants
    private static final Logger LOGGER = LogManager.getFormatterLogger(LoopbackSendThread.class.getSimpleName());

    // Attributes
    private final AtomicBoolean m_send1;
    private final AtomicBoolean m_send2;
    private LoopbackConnection m_connection1;
    private LoopbackConnection m_connection2;

    private volatile boolean m_overprovisioining;
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
    LoopbackSendThread(final LoopbackConnectionManager p_connectionManager, final int p_connectionTimeout, final int p_osBufferSize,
            final boolean p_overprovisioning) {
        m_send1 = new AtomicBoolean(false);
        m_send2 = new AtomicBoolean(false);

        m_overprovisioining = p_overprovisioning;
        m_running = true;
    }

    /**
     * Activate parking strategy.
     */
    void activateParking() {
        m_overprovisioining = true;
    }

    @Override
    public void run() {
        long time;

        // Wait until connection was created
        while (m_connection1 == null) { // Is updated by lfence below
            try {
                Thread.sleep(0);
                UnsafeHandler.getInstance().getUnsafe().loadFence();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        boolean sent = false;
        while (m_running) {
            time = System.nanoTime();
            while (m_running) {
                if (m_send2.compareAndSet(true, false)) {
                    try {
                        m_connection2.getPipeOut().write();
                    } catch (final IOException ignore) {

                    }
                    sent = true;
                }

                if (m_send1.compareAndSet(true, false)) {
                    try {
                        m_connection1.getPipeOut().write();
                    } catch (final IOException ignore) {

                    }
                    sent = true;
                }

                if (sent) {
                    sent = false;
                    continue;
                }

                if (m_overprovisioining) {
                    LockSupport.parkNanos(1);
                }

                if (System.nanoTime() - time > 1000 * 1000) {
                    try {
                        if (m_connection2 != null) {
                            m_connection2.getPipeOut().write();
                        }

                        m_connection1.getPipeOut().write();
                    } catch (final IOException ignore) {

                    }
                    break;
                }
            }

        }
    }

    protected void trigger(LoopbackConnection p_connection) {
        // Access m_connection1 and m_connection2 without synchronization as it set once (in synchronize block), only
        if (p_connection == m_connection1) {
            m_send1.set(true);
            return;
        }
        if (p_connection == m_connection2) {
            m_send2.set(true);
            return;
        }

        synchronized (m_send1) {
            if (m_connection1 == null) {
                m_connection1 = p_connection;
                m_send1.set(true);
                return;
            }
            if (m_connection2 == null && p_connection != m_connection1) { // m_connection1 might have been set meanwhile
                m_connection2 = p_connection;
                m_send2.set(true);
                return;
            }

            if (p_connection == m_connection1) {
                m_send1.set(true);
                return;
            }
            if (p_connection == m_connection2) {
                m_send2.set(true);
            }
        }
    }

    /**
     * Closes the Worker
     */

    protected void close() {
        m_running = false;
    }
}

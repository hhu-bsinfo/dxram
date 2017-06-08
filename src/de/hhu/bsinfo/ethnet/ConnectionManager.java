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

package de.hhu.bsinfo.ethnet;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.net.events.ConnectionLostEvent;
import de.hhu.bsinfo.ethnet.AbstractConnection.DataReceiver;
import de.hhu.bsinfo.ethnet.AbstractConnectionCreator.ConnectionCreatorListener;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Manages the network connections
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 */
final class ConnectionManager implements ConnectionCreatorListener {

    // Constants
    static final int MAX_CONNECTIONS = 1000;

    private static final Logger LOGGER = LogManager.getFormatterLogger(ConnectionManager.class.getSimpleName());

    // Attributes
    private AbstractConnection[] m_connections;
    private int m_openConnections;

    private AbstractConnectionCreator m_creator;
    private ConnectionCreatorHelperThread m_connectionCreatorHelperThread;
    private DataReceiver m_connectionListener;

    private boolean m_deactivated;
    private volatile boolean m_closed;

    private ReentrantLock m_connectionCreationLock;

    // Constructors

    /**
     * Creates an instance of ConnectionStore
     *
     * @param p_creator
     *     the ConnectionCreator
     * @param p_listener
     *     the ConnectionListener
     */
    ConnectionManager(final AbstractConnectionCreator p_creator, final DataReceiver p_listener) {
        m_connections = new AbstractConnection[65536];
        m_openConnections = 0;

        m_creator = p_creator;
        m_creator.setListener(this);
        m_connectionListener = p_listener;

        m_deactivated = false;
        m_closed = false;

        m_connectionCreationLock = new ReentrantLock(false);

        // Start connection creator helper thread
        m_connectionCreatorHelperThread = new ConnectionCreatorHelperThread();
        m_connectionCreatorHelperThread.setName("Network: ConnectionCreatorHelperThread");
        m_connectionCreatorHelperThread.start();
    }

    /**
     * Closes the ConnectionManager
     */
    public void close() {
        m_creator.prepareClosure();

        // cleanup all opened connections
        closeAllConnections();

        m_closed = true;
        m_creator.close();

        m_connectionCreatorHelperThread.interrupt();
        try {
            m_connectionCreatorHelperThread.join();
        } catch (final InterruptedException ignore) {

        }

        for (int i = 0; i < m_connections.length; i++) {
            m_connections[i] = null;
        }
    }

    /**
     * A new connection must be created
     *
     * @param p_destination
     *     the remote NodeID
     * @note is called by selector thread only
     */
    @Override
    public void createConnection(final short p_destination, final SocketChannel p_channel) {
        m_connectionCreatorHelperThread.pushJob(new CreationJob(p_destination, p_channel));
    }

    /**
     * A connection was closed
     *
     * @param p_connection
     *     the closed connection
     * @note is called by selector thread only
     */
    @Override
    public void connectionClosed(final AbstractConnection p_connection) {
        m_connectionCreatorHelperThread.pushJob(new ClosureJob(p_connection));
    }

    /**
     * Returns the status of all connections
     *
     * @return the statuses
     */
    String getConnectionStatuses() {
        String ret = "";

        m_connectionCreationLock.lock();
        for (int i = 0; i < 65536; i++) {
            if (m_connections[i] != null) {
                ret += m_connections[i].toString();
            }
        }
        m_connectionCreationLock.unlock();

        return ret;
    }

    // Methods

    /**
     * Activates the connection manager
     */
    void activate() {
        m_connectionCreationLock.lock();
        m_deactivated = false;
        m_connectionCreationLock.unlock();
    }

    /**
     * Deactivates the connection manager
     */
    void deactivate() {
        m_deactivated = true;
    }

    /**
     * Get the connection for the given destination
     *
     * @param p_destination
     *     the destination
     * @return the connection
     * @throws IOException
     *     if the connection could not be get
     */
    AbstractConnection getConnection(final short p_destination) throws IOException {
        AbstractConnection ret;

        assert p_destination != NodeID.INVALID_ID;

        ret = m_connections[p_destination & 0xFFFF];
        if ((ret == null || !ret.isOutgoingConnected()) && !m_deactivated) {
            m_connectionCreationLock.lock();

            ret = m_connections[p_destination & 0xFFFF];
            if ((ret == null || !ret.isOutgoingConnected()) && !m_deactivated) {
                if (m_openConnections == MAX_CONNECTIONS) {
                    dismissRandomConnection();
                }

                try {
                    if (ret == null) {
                        ret = m_creator.createConnection(p_destination);
                    } else {
                        m_creator.createOutgoingChannel(p_destination, ret);
                    }
                } catch (final IOException e) {
                    m_connectionCreationLock.unlock();

                    throw e;
                }

                if (ret != null) {
                    ret.setListener(m_connectionListener);
                    m_connections[p_destination & 0xFFFF] = ret;
                    m_openConnections++;
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.warn("Connection creation was aborted. No listener was registered.");
                    // #endif /* LOGGER >= ERROR */
                }
            }
            m_connectionCreationLock.unlock();
        }

        return ret;
    }

    /**
     * Closes all connections
     */
    private void closeAllConnections() {
        AbstractConnection connection = null;

        m_connectionCreationLock.lock();
        for (int i = 0; i < 65536; i++) {
            if (m_connections[i] != null) {
                connection = m_connections[i & 0xFFFF];
                m_connections[connection.getDestination() & 0xFFFF] = null;

                connection.close();
                connection.cleanup();
            }
        }

        // Wait for last connection being closed by selector (for NIO)
        while (connection != null && (connection.isOutgoingConnected() || connection.isIncomingConnected())) {
            connection.wakeup();
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }
        m_openConnections = 0;
        m_connectionCreationLock.unlock();
    }

    /**
     * Dismiss the connection randomly
     */
    private void dismissRandomConnection() {
        int random = -1;
        AbstractConnection dismiss = null;
        Random rand;

        rand = new Random();
        while (dismiss == null) {
            random = rand.nextInt(m_connections.length);
            dismiss = m_connections[random & 0xFFFF];
        }
        // #if LOGGER >= WARN
        LOGGER.warn("Removing " + NodeID.toHexString((short) random));
        // #endif /* LOGGER >= WARN */

        m_connections[random & 0xFFFF] = null;
        m_openConnections--;

        dismiss.close();
    }

    /**
     * Helper class to encapsulate a job
     *
     * @author Kevin Beineke 22.06.2016
     */
    private static class Job {
        private byte m_id;

        /**
         * Creates an instance of Job
         *
         * @param p_id
         *     the static job identification
         */
        protected Job(final byte p_id) {
            m_id = p_id;
        }

        /**
         * Returns the job identification
         *
         * @return the job ID
         */
        public byte getID() {
            return m_id;
        }
    }

    /**
     * Helper class to encapsulate a job
     *
     * @author Kevin Beineke 22.06.2016
     */
    private static final class CreationJob extends Job {
        private short m_destination;
        private SocketChannel m_channel;

        /**
         * Creates an instance of Job
         *
         * @param p_destination
         *     the NodeID of destination
         * @param p_channel
         *     the already established SocketChannel
         */
        private CreationJob(final short p_destination, final SocketChannel p_channel) {
            super((byte) 0);
            m_destination = p_destination;
            m_channel = p_channel;
        }

        /**
         * Returns the destination
         *
         * @return the NodeID
         */
        public short getDestination() {
            return m_destination;
        }

        /**
         * Returns the SocketChannel
         *
         * @return the SocketChannel
         */
        SocketChannel getSocketChannel() {
            return m_channel;
        }
    }

    /**
     * Helper class to encapsulate a job
     *
     * @author Kevin Beineke 22.06.2016
     */
    private static final class ClosureJob extends Job {
        private AbstractConnection m_connection;

        /**
         * Creates an instance of Job
         *
         * @param p_connection
         *     the AbstractConnection
         */
        private ClosureJob(final AbstractConnection p_connection) {
            super((byte) 1);
            m_connection = p_connection;
        }

        /**
         * Returns the connection
         *
         * @return the AbstractConnection
         */
        public AbstractConnection getConnection() {
            return m_connection;
        }
    }

    /**
     * Helper thread that asynchronously executes commands for selector thread to avoid blocking it
     *
     * @author Kevin Beineke 22.06.2016
     */
    private class ConnectionCreatorHelperThread extends Thread {

        private ArrayDeque<Job> m_jobs = new ArrayDeque<Job>();
        private ReentrantLock m_lock = new ReentrantLock(false);
        private Condition m_jobAvailableCondition = m_lock.newCondition();

        @Override
        public void run() {
            short destination;
            AbstractConnection connection;
            Job job;

            while (!m_closed) {
                if (m_deactivated) {
                    Thread.yield();
                    continue;
                }

                m_lock.lock();
                while (m_jobs.isEmpty()) {
                    try {
                        m_jobAvailableCondition.await();
                    } catch (final InterruptedException ignored) {
                        return;
                    }
                }

                job = m_jobs.pop();
                m_lock.unlock();

                if (job.getID() == 0) {
                    // 0: Create and add connection
                    CreationJob creationJob = (CreationJob) job;
                    destination = creationJob.getDestination();
                    SocketChannel channel = creationJob.getSocketChannel();

                    m_connectionCreationLock.lock();
                    if (m_openConnections == MAX_CONNECTIONS) {
                        dismissRandomConnection();
                    }

                    connection = m_connections[destination & 0xFFFF];
                    try {
                        if (connection == null) {
                            connection = m_creator.createConnection(destination, channel);
                            m_connections[destination & 0xFFFF] = connection;
                            m_openConnections++;
                        } else {
                            m_creator.bindIncomingChannel(channel, connection);
                        }
                        connection.setListener(m_connectionListener);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    m_connectionCreationLock.unlock();
                } else {
                    // 1: Connection was closed by NIOSelectorThread (connection was faulty) -> Remove it
                    ClosureJob closeJob = (ClosureJob) job;
                    connection = closeJob.getConnection();

                    m_connectionCreationLock.lock();
                    AbstractConnection tmp = m_connections[connection.getDestination() & 0xFFFF];
                    if (connection.equals(tmp)) {
                        m_connections[connection.getDestination() & 0xFFFF] = null;
                        m_openConnections--;

                        connection.cleanup();
                    }
                    m_connectionCreationLock.unlock();

                    // Trigger failure handling for remote node over faulty connection
                    NetworkHandler.getEventHandler().fireEvent(new ConnectionLostEvent(getClass().getSimpleName(), connection.getDestination()));
                }
            }
        }

        /**
         * Push new job
         *
         * @param p_job
         *     the new job to add
         */
        private void pushJob(final Job p_job) {
            m_lock.lock();
            m_jobs.push(p_job);
            m_jobAvailableCondition.signalAll();
            m_lock.unlock();
        }
    }
}

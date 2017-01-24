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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.net.events.ConnectionLostEvent;
import de.hhu.bsinfo.ethnet.AbstractConnection.DataReceiver;
import de.hhu.bsinfo.ethnet.AbstractConnectionCreator.ConnectionCreatorListener;

/**
 * Manages the network connections
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 */
final class ConnectionManager implements ConnectionCreatorListener {

    // Constants
    static final int MAX_CONNECTIONS = 1000;

    // Attributes
    private AbstractConnection[] m_connections;
    private ArrayList<AbstractConnection> m_connectionList;

    private AbstractConnectionCreator m_creator;
    private ConnectionCreatorHelperThread m_connectionCreatorHelperThread;
    private DataReceiver m_connectionListener;

    private boolean m_deactivated;
    private volatile boolean m_closed;

    private boolean m_waiting;
    private short m_waitingFor;

    private Condition m_connectionCreatedCondition;
    private ReentrantLock m_connectionCreationLock;
    private ReentrantLock m_getConnectionLock;

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
        m_connectionList = new ArrayList<AbstractConnection>(MAX_CONNECTIONS);

        m_creator = p_creator;
        m_creator.setListener(this);
        m_connectionListener = p_listener;

        m_deactivated = false;
        m_closed = false;
        m_waiting = false;
        m_waitingFor = -1;

        m_connectionCreationLock = new ReentrantLock(false);
        m_connectionCreatedCondition = m_connectionCreationLock.newCondition();
        m_getConnectionLock = new ReentrantLock(false);

        // Start connection creator helper thread
        m_connectionCreatorHelperThread = new ConnectionCreatorHelperThread();
        m_connectionCreatorHelperThread.setName("Network: ConnectionCreatorHelperThread");
        m_connectionCreatorHelperThread.start();
    }

    /**
     * Returns the status of all connections
     *
     * @return the statuses
     */
    String getConnectionStatuses() {
        String ret = "";

        m_connectionCreationLock.lock();
        Iterator<AbstractConnection> iter = m_connectionList.iterator();
        while (iter.hasNext()) {
            ret += iter.next().toString();
        }
        m_connectionCreationLock.unlock();

        return ret;
    }

    /**
     * Closes the ConnectionManager
     */
    public void close() {
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
    public void createConnection(final short p_destination) {
        m_connectionCreatorHelperThread.pushJob(new Job((byte) 0, p_destination));
    }

    /**
     * A new connection was created
     *
     * @param p_connection
     *     the new connection
     * @note is called by selector thread only
     */
    @Override
    public void connectionCreated(final AbstractConnection p_connection) {
        p_connection.setListener(m_connectionListener);
        m_connectionCreatorHelperThread.pushJob(new Job((byte) 1, p_connection));
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
        m_connectionCreatorHelperThread.pushJob(new Job((byte) 2, p_connection));
    }

    // Methods

    /**
     * Checks if there is a congested connection
     *
     * @return whether there is congested connection or not
     */
    boolean atLeastOneConnectionIsCongested() {
        boolean ret = false;

        m_connectionCreationLock.lock();
        Iterator<AbstractConnection> iter = m_connectionList.iterator();
        while (iter.hasNext()) {
            if (iter.next().isCongested()) {
                ret = true;
                break;
            }
        }
        m_connectionCreationLock.unlock();

        return ret;
    }

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
        if (ret == null && !m_deactivated) {
            m_getConnectionLock.lock();
            m_connectionCreationLock.lock();

            ret = m_connections[p_destination & 0xFFFF];
            if (ret == null && !m_deactivated) {

                while (m_creator.keyIsPending() || m_waiting) {
                    m_waiting = true;
                    m_waitingFor = -1;
                    try {
                        // Wait for a connection to be finished or one ms if the pending key was closed
                        m_connectionCreatedCondition.await(1, TimeUnit.MILLISECONDS);
                    } catch (final InterruptedException ignored) {
                    }
                }

                ret = m_connections[p_destination & 0xFFFF];
                if (ret == null && !m_deactivated) {
                    /*-if (m_connectionList.size() == MAX_CONNECTIONS) {
                        dismissRandomConnection();
                    }*/

                    try {
                        ret = m_creator.createConnection(p_destination);
                    } catch (final IOException e) {
                        m_connectionCreationLock.unlock();
                        m_getConnectionLock.unlock();

                        throw e;
                    }

                    if (ret == null) {
                        // This node's NodeID is smaller -> Remote node was triggered to create connection
                        // Only one application thread can be in this section!
                        m_waiting = true;
                        m_waitingFor = p_destination;
                        while (m_waiting) {
                            try {
                                m_connectionCreatedCondition.await();
                            } catch (final InterruptedException ignored) {
                            }
                        }
                        m_connectionCreationLock.unlock();
                        m_getConnectionLock.unlock();

                        return getConnection(p_destination);
                    } else {
                        // This node's NodeID is greater -> Keep established connection
                        ret.setListener(m_connectionListener);
                        addConnection(ret);
                    }
                }
            }
            m_connectionCreationLock.unlock();
            m_getConnectionLock.unlock();
        }

        return ret;
    }

    /**
     * Closes all connections
     */
    private void closeAllConnections() {
        AbstractConnection connection = null;

        m_connectionCreationLock.lock();
        Iterator<AbstractConnection> iter = m_connectionList.iterator();
        while (iter.hasNext()) {
            connection = iter.next();
            if (connection != null && connection.isConnected()) {
                connection = m_connections[connection.getDestination() & 0xFFFF];
                m_connections[connection.getDestination() & 0xFFFF] = null;

                connection.close();
                connection.cleanup();
            }
        }
        // Wait for last connection being closed by selector (for NIO)
        while (connection != null && connection.isConnected()) {
            connection.wakeup();
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }
        m_connectionCreationLock.unlock();
    }

    /**
     * Add a new connection. Use duplicate consensus if there is already a connection for the specific NodeID.
     *
     * @param p_connection
     *     the new connection
     */
    private void addConnection(final AbstractConnection p_connection) {
        short remoteNodeID;

        // TODO: If maximum number of connections is reached, locally deleting connection does not impact remote node
        // TODO: Double connections problems
        // TODO:
        // TODO:
        // TODO:

        remoteNodeID = p_connection.getDestination();

        if (m_connectionList.size() == MAX_CONNECTIONS) {
            dismissRandomConnection();
        }

        // No entry for this NodeID -> insert connection
        m_connections[remoteNodeID & 0xFFFF] = p_connection;
        m_connectionList.add(p_connection);
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
        System.out.println("Removing " + NodeID.toHexString((short) random));
        m_connections[random & 0xFFFF] = null;
        m_connectionList.remove(dismiss);

        dismiss.close();
    }

    /**
     * Helper class to encapsulate a job
     *
     * @author Kevin Beineke 22.06.2016
     */
    private static final class Job {
        private byte m_id;
        private Object m_data;

        /**
         * Creates an instance of Job
         *
         * @param p_id
         *     the static job identification
         * @param p_data
         *     the data (NodeID of destination or AbstractConnection depending on job)
         */
        private Job(final byte p_id, final Object p_data) {
            m_id = p_id;
            m_data = p_data;
        }

        /**
         * Returns the job identification
         *
         * @return the job ID
         */
        public byte getID() {
            return m_id;
        }

        /**
         * Returns the data
         *
         * @return the NodeID or AbstractConnection
         */
        public Object getData() {
            return m_data;
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
                    destination = (short) job.getData();

                    m_connectionCreationLock.lock();
                    if (m_connections[destination & 0xFFFF] == null) {
                        try {
                            connection = m_creator.createConnection(destination);
                            connection.setListener(m_connectionListener);

                            addConnection(connection);
                            if (m_waiting && (m_waitingFor == connection.getDestination() || m_waitingFor == -1)) {
                                m_waiting = false;
                                m_connectionCreatedCondition.signalAll();
                            }
                        } catch (final IOException e) { /* Ignore as this node does not know the failed node */ }
                    }
                    m_connectionCreationLock.unlock();
                } else if (job.getID() == 1) {
                    // 1: Connection was created -> Add it
                    connection = (AbstractConnection) job.getData();

                    m_connectionCreationLock.lock();
                    addConnection(connection);
                    if (m_waiting && (m_waitingFor == connection.getDestination() || m_waitingFor == -1)) {
                        m_waiting = false;
                        m_connectionCreatedCondition.signalAll();
                    }
                    m_connectionCreationLock.unlock();
                } else {
                    // 2: Connection was closed by NIOSelectorThread (connection was faulty) -> Remove it
                    connection = (AbstractConnection) job.getData();

                    m_connectionCreationLock.lock();
                    AbstractConnection tmp = m_connections[connection.getDestination() & 0xFFFF];
                    if (connection.equals(tmp)) {
                        m_connections[connection.getDestination() & 0xFFFF] = null;
                        m_connectionList.remove(tmp);

                        connection.cleanup();
                        // TODO: Inform and update system
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

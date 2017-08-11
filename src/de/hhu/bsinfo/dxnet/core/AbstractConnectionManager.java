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

package de.hhu.bsinfo.dxnet.core;

import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.ConnectionManagerListener;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Abstract class for a connection manager. Manage connections to keep only a max amount of connections
 * opened to reduce the resource footprint. Furthermore, depending on the transport type implemented,
 * handle connection creation, opening, closing and deletion.
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.08.2017
 */
public abstract class AbstractConnectionManager {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractConnectionManager.class.getSimpleName());

    protected final AbstractConnection[] m_connections;
    protected final ReentrantLock m_connectionCreationLock;

    protected int m_openConnections;

    protected ConnectionManagerListener m_listener;

    private final int m_maxConnections;

    /**
     * Constructor
     *
     * @param p_maxConnections
     *         Max number of connections to keep active simultaneously
     */
    protected AbstractConnectionManager(final int p_maxConnections) {
        m_maxConnections = p_maxConnections;
        m_connections = new AbstractConnection[65536];
        m_connectionCreationLock = new ReentrantLock(false);

        m_openConnections = 0;
    }

    /**
     * Set connection manager listener to receive callbacks when a node connected or disconnected
     */
    public void setListener(final ConnectionManagerListener p_listener) {
        m_listener = p_listener;
    }

    /**
     * Returns the status of all connections
     *
     * @return Status of all connections (debug string)
     */
    public String getConnectionStatuses() {
        StringBuilder ret = new StringBuilder();

        m_connectionCreationLock.lock();
        for (int i = 0; i < 65536; i++) {
            if (m_connections[i] != null) {
                ret.append(m_connections[i]);
            }
        }
        m_connectionCreationLock.unlock();

        return ret.toString();
    }

    /**
     * Get the connection for the given destination
     *
     * @param p_destination
     *         Node id of the destination
     * @return A valid reference if the connection is available, null otherwise
     * @throws NetworkException
     *         If the connection does not exist and creating the connection failed
     */
    public AbstractConnection getConnection(final short p_destination) throws NetworkException {
        AbstractConnection ret;

        assert p_destination != NodeID.INVALID_ID;

        ret = m_connections[p_destination & 0xFFFF];
        if (ret == null || !ret.getPipeOut().isConnected()) {
            m_connectionCreationLock.lock();

            ret = m_connections[p_destination & 0xFFFF];
            if (ret == null || !ret.getPipeOut().isConnected()) {
                if (m_openConnections == m_maxConnections) {
                    dismissRandomConnection();
                }

                try {
                    ret = createConnection(p_destination, ret);
                } catch (final NetworkException e) {
                    m_connectionCreationLock.unlock();

                    throw e;
                }

                if (ret != null) {
                    // #if LOGGER >= DEBUG
                    LOGGER.debug("Connection created: 0x%X", p_destination);
                    // #endif /* LOGGER >= DEBUG */

                    m_connections[p_destination & 0xFFFF] = ret;
                    m_openConnections++;
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.warn("Connection creation was aborted!");
                    // #endif /* LOGGER >= ERROR */
                }
            }
            m_connectionCreationLock.unlock();
        }

        return ret;
    }

    /**
     * Closes all active connections and cleans up the connection manager
     */
    public void close() {
        closeAllConnections();

        for (int i = 0; i < m_connections.length; i++) {
            m_connections[i] = null;
        }
    }

    /**
     * Creates a new connection to the given destination
     *
     * @param p_destination
     *         Node id of the destination
     * @param p_existingConnection
     *         Parameter required for NIO implementation
     * @return New connection to the specified destination
     * @throws NetworkException
     *         If the connection could not be created
     */
    protected abstract AbstractConnection createConnection(final short p_destination, final AbstractConnection p_existingConnection) throws NetworkException;

    /**
     * Close an active connection
     *
     * @param p_connection
     *         Connection to close
     * @param p_removeConnection
     *         True to remove the connection resources currently allocated as well or false to keep them to allow re-opening the connection quickly
     */
    protected abstract void closeConnection(final AbstractConnection p_connection, final boolean p_removeConnection);

    /**
     * Close all connections
     */
    private void closeAllConnections() {
        AbstractConnection connection = null;

        m_connectionCreationLock.lock();
        for (int i = 0; i < 65536; i++) {
            if (m_connections[i] != null) {
                connection = m_connections[i & 0xFFFF];
                m_connections[connection.getDestinationNodeID() & 0xFFFF] = null;

                connection.close(false);
            }
        }

        // Wait for last connection being closed by selector (for NIO)
        while (connection != null && (connection.getPipeOut().isConnected() || connection.getPipeIn().isConnected())) {
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
     * Dismiss a connection randomly
     */
    protected void dismissRandomConnection() {
        int random = -1;
        AbstractConnection dismiss = null;
        Random rand;

        rand = new Random();
        while (dismiss == null) {
            random = rand.nextInt(m_connections.length);
            dismiss = m_connections[random & 0xFFFF];
        }
        // #if LOGGER >= WARN
        LOGGER.warn("Removing 0x%X", (short) random);
        // #endif /* LOGGER >= WARN */

        m_connections[random & 0xFFFF] = null;
        m_openConnections--;

        dismiss.close(false);
    }
}

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
import java.nio.channels.SocketChannel;

/**
 * Creates new network connections
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 */
abstract class AbstractConnectionCreator {

    // Attributes
    private ConnectionCreatorListener m_listener;

    // Constructors

    /**
     * Creates an instance of AbstractConnectionCreator
     */
    AbstractConnectionCreator() {
        m_listener = null;
    }

    // Setters

    /**
     * Returns the selector status
     *
     * @return the selector status
     */
    public abstract String getSelectorStatus();

    // Methods

    /**
     * Creates a new connection to the given destination and opens the outgoing socket channel
     *
     * @param p_destination
     *     the destination
     * @return a new connection
     * @throws IOException
     *     if the connection could not be created
     */
    public abstract AbstractConnection createConnection(short p_destination) throws IOException;

    /**
     * Creates a new connection to the given destination and connects the incoming socket channel
     *
     * @param p_destination
     *     the destination
     * @param p_channel
     *     the already opened inconing socket channel
     * @return a new connection
     * @throws IOException
     *     if the connection could not be created
     */
    public abstract AbstractConnection createConnection(final short p_destination, final SocketChannel p_channel) throws IOException;

    /**
     * Opens the outgoing socket channel and connects it to the already existing connection
     *
     * @param p_destination
     *     the destination
     * @param p_connection
     *     the already existing connection
     * @throws IOException
     *     if the connection could not be created
     */
    public abstract void createOutgoingChannel(final short p_destination, final AbstractConnection p_connection) throws IOException;

    /**
     * Connects the opened incoming socket channel with the already existing connection
     *
     * @param p_channel
     *     the open incoming socket channel
     * @param p_connection
     *     the already existing connection
     * @throws IOException
     *     if the connection could not be created
     */
    public abstract void bindIncomingChannel(final SocketChannel p_channel, final AbstractConnection p_connection) throws IOException;

    /**
     * Closes the given connection
     *
     * @param p_connection
     *     the connection
     * @param p_informConnectionManager
     *     whether to inform the connection manager or not
     */
    public abstract void closeConnection(final AbstractConnection p_connection, final boolean p_informConnectionManager);

    /**
     * Closes the creator and frees unused resources
     */
    public void close() {
    }

    /**
     * Check if there a remote node tries to open a connection currently
     *
     * @return whether a remote node opens a connection to this node currently or not
     */
    public boolean keyIsPending() {
        return true;
    }

    /**
     * Initializes the creator
     *
     * @param p_listenPort
     *     the listen port
     */
    protected void initialize(final int p_listenPort) {
    }

    /**
     * Prepares closure of the creator
     */
    void prepareClosure() {
    }

    /**
     * Sets the ConnectionCreatorListener
     *
     * @param p_listener
     *     the ConnectionCreatorListener
     */
    final void setListener(final ConnectionCreatorListener p_listener) {
        m_listener = p_listener;
    }

    /**
     * Informs the ConnectionCreatorListener to create a new connection
     *
     * @param p_destination
     *     the remote NodeID
     */
    final void fireCreateConnection(final short p_destination, final SocketChannel p_channel) {
        if (m_listener != null) {
            m_listener.createConnection(p_destination, p_channel);
        }
    }

    /**
     * Informs the ConnectionCreatorListener about a closed connection
     *
     * @param p_connection
     *     the closed connection
     */
    final void fireConnectionClosed(final AbstractConnection p_connection) {
        if (m_listener != null) {
            m_listener.connectionClosed(p_connection);
        }
    }

    // Classes

    /**
     * Methods for reacting to new or closed connections
     *
     * @author Florian Klein
     *         18.03.2012
     */
    interface ConnectionCreatorListener {

        // Methods

        /**
         * A new connection must be created
         *
         * @param p_destination
         *     the remote NodeID
         */
        void createConnection(short p_destination, SocketChannel p_channel);

        /**
         * A connection was closed
         *
         * @param p_connection
         *     the closed connection
         */
        void connectionClosed(AbstractConnection p_connection);

    }

}

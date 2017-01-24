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
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages the network connections
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 */
class NIOSelector extends Thread {

    private static final Logger LOGGER = LogManager.getFormatterLogger(NIOSelector.class.getSimpleName());

    // Constants
    private static final int READWRITE_MASK = 5;
    private static final int CLOSE = 32;

    // Attributes
    private ServerSocketChannel m_serverChannel;
    private Selector m_selector;

    private NIOConnectionCreator m_connectionCreator;
    private NIOInterface m_nioInterface;

    private int m_connectionTimeout;
    private LinkedHashSet<ChangeOperationsRequest> m_changeRequests;
    private ReentrantLock m_changeLock;

    private volatile boolean m_running;

    // Constructors

    /**
     * Creates an instance of NIOSelector
     *
     * @param p_connectionCreator
     *     the NIOConnectionCreator
     * @param p_nioInterface
     *     the NIOInterface to send/receive data
     * @param p_connectionTimeout
     *     the connection timeout
     * @param p_port
     *     the port
     */
    NIOSelector(final NIOConnectionCreator p_connectionCreator, final NIOInterface p_nioInterface, final int p_port, final int p_connectionTimeout) {
        m_serverChannel = null;
        m_selector = null;

        m_nioInterface = p_nioInterface;
        m_connectionCreator = p_connectionCreator;

        m_connectionTimeout = p_connectionTimeout;
        m_changeRequests = new LinkedHashSet<ChangeOperationsRequest>();
        m_changeLock = new ReentrantLock(false);

        m_running = false;

        // Create Selector on ServerSocketChannel
        IOException exception = null;
        for (int i = 0; i < 10; i++) {
            try {
                m_selector = Selector.open();
                m_serverChannel = ServerSocketChannel.open();
                m_serverChannel.configureBlocking(false);
                m_serverChannel.socket().bind(new InetSocketAddress(p_port));
                m_serverChannel.register(m_selector, SelectionKey.OP_ACCEPT);

                m_running = true;

                exception = null;
                break;
            } catch (final IOException e) {
                exception = e;

                // #if LOGGER >= ERROR
                LOGGER.error("Could not bind network address. Retry in 1s");
                // #endif /* LOGGER >= ERROR */

                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException ignored) {
                }
            }
        }

        if (exception != null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not create network channel!");
            // #endif /* LOGGER >= ERROR */
        }
    }

    // Getter

    /**
     * Returns the Selector
     *
     * @return the Selector
     */
    Selector getSelector() {
        return m_selector;
    }

    // Methods
    @Override
    public String toString() {
        String ret = "Current keys: ";

        Set<SelectionKey> selected = m_selector.keys();
        Iterator<SelectionKey> iterator = selected.iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            if (key.attachment() != null) {
                ret += '[' + NodeID.toHexString(((NIOConnection) key.attachment()).getDestination()) + ", " + key.interestOps() + "] ";
            }
        }

        return ret;
    }

    @Override
    public void run() {
        int interest;
        NIOConnection connection;
        ChangeOperationsRequest changeRequest;
        Iterator<SelectionKey> iterator;
        Set<SelectionKey> selected;
        SelectionKey key;

        LinkedHashSet<ChangeOperationsRequest> delayedCloseOperations = new LinkedHashSet<ChangeOperationsRequest>();

        while (m_running) {
            m_changeLock.lock();
            Iterator<ChangeOperationsRequest> iter;
            while (!m_changeRequests.isEmpty()) {
                iter = m_changeRequests.iterator();
                changeRequest = iter.next();
                iter.remove();

                connection = changeRequest.getConnection();
                interest = changeRequest.getOperations();
                if ((interest & READWRITE_MASK) != 0) {
                    try {
                        // Either READ (is never registered), WRITE or READ | WRITE -> change interest only
                        key = connection.getChannel().keyFor(m_selector);
                        if (key != null && key.interestOps() != READWRITE_MASK) {
                            // Key might be null if connection was closed during shutdown or due to closing a duplicate connection
                            // If key interest is READ | WRITE the interest must not be overwritten with WRITE as both incoming
                            // buffers might be filled causing a deadlock
                            key.interestOps(interest);
                        }
                    } catch (final CancelledKeyException e) {
                        // Ignore
                    }
                } else if (interest == CLOSE) {
                    // CLOSE -> close connection
                    // Only close connection if since request at least the time for two connection timeouts has elapsed
                    if (System.currentTimeMillis() - connection.getClosingTimestamp() > 2 * m_connectionTimeout) {
                        // #if LOGGER >= DEBUG
                        try {
                            LOGGER.debug("Closing connection to 0x%X;%s", connection.getDestination(), connection.getChannel().getRemoteAddress());
                        } catch (final IOException ignored) {
                        }
                        // #endif /* LOGGER >= DEBUG */
                        // Close connection
                        m_connectionCreator.closeConnection(connection, false);
                    } else {
                        // Delay connection closure
                        delayedCloseOperations.add(changeRequest);
                    }
                } else {
                    // CONNECT -> register with connection as attachment (ACCEPT is registered directly)
                    try {
                        connection.getChannel().register(m_selector, interest, connection);
                    } catch (final ClosedChannelException e) {
                        // #if LOGGER >= DEBUG
                        LOGGER.debug("Could not change operations!");
                        // #endif /* LOGGER >= DEBUG */
                    }
                }
            }
            if (!delayedCloseOperations.isEmpty()) {
                m_changeRequests.addAll(delayedCloseOperations);
                delayedCloseOperations.clear();
            }
            m_changeLock.unlock();

            try {
                // Wait for network action
                if (m_selector.select() > 0 && m_selector.isOpen()) {
                    selected = m_selector.selectedKeys();
                    iterator = selected.iterator();

                    while (iterator.hasNext()) {
                        key = iterator.next();
                        iterator.remove();
                        if (key.isValid()) {
                            if (key.isAcceptable()) {
                                accept();
                            } else {
                                dispatch(key);
                            }
                        }
                    }
                    selected.clear();
                }
            } catch (final ClosedSelectorException e) {
                // Ignore!
            } catch (final IOException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Key selection failed!");
                // #endif /* LOGGER >= ERROR */
            }
        }
    }

    /**
     * Closes the Worker
     */
    protected void close() {
        m_running = false;

        try {
            m_serverChannel.close();
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Unable to close channel!");
            // #endif /* LOGGER >= ERROR */
        }
        // #if LOGGER >= INFO
        LOGGER.info("Closing ServerSocketChannel successful");
        // #endif /* LOGGER >= INFO */

        try {
            m_selector.close();
        } catch (final IOException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Unable to shutdown selector!");
            // #endif /* LOGGER >= ERROR */
        }
        // #if LOGGER >= INFO
        LOGGER.info("Shutdown of Selector successful");
        // #endif /* LOGGER >= INFO */
    }

    /**
     * Append the given ChangeOperationsRequest to the Queue
     *
     * @param p_request
     *     the ChangeOperationsRequest
     */
    void changeOperationInterestAsync(final ChangeOperationsRequest p_request) {
        boolean res;

        m_changeLock.lock();
        res = m_changeRequests.add(p_request);
        m_changeLock.unlock();

        if (res) {
            m_selector.wakeup();
        }
    }

    /**
     * Append the given NIOConnection to the Queue
     *
     * @param p_connection
     *     the NIOConnection to close
     */
    void closeConnectionAsync(final NIOConnection p_connection) {
        m_changeLock.lock();
        m_changeRequests.add(new ChangeOperationsRequest(p_connection, CLOSE));
        m_changeLock.unlock();

        m_selector.wakeup();
    }

    /**
     * Accept a new incoming connection
     *
     * @throws IOException
     *     if the new connection could not be accesses
     */
    private void accept() throws IOException {
        SocketChannel channel;

        channel = m_serverChannel.accept();
        channel.configureBlocking(false);

        channel.register(m_selector, SelectionKey.OP_READ);
    }

    /**
     * Execute key by creating a new connection, reading from channel or writing to channel
     *
     * @param p_key
     *     the current key
     */
    private void dispatch(final SelectionKey p_key) {
        boolean complete;
        boolean successful;
        NIOConnection connection;

        connection = (NIOConnection) p_key.attachment();
        if (p_key.isValid()) {
            try {
                if (p_key.isReadable()) {
                    if (connection == null) {
                        // Channel was accepted but not used already -> Read NodeID, create NIOConnection and attach to key
                        m_connectionCreator.createConnection((SocketChannel) p_key.channel());
                    } else {
                        try {
                            successful = m_nioInterface.read(connection);
                        } catch (final IOException e) {
                            // #if LOGGER >= DEBUG
                            LOGGER.debug("Could not read from channel (0x%X)!", connection.getDestination());
                            // #endif /* LOGGER >= DEBUG */

                            successful = false;
                        }
                        if (!successful) {
                            m_connectionCreator.closeConnection(connection, true);
                        }
                    }
                } else if (p_key.isWritable()) {
                    if (connection == null) {
                        // #if LOGGER >= ERROR
                        LOGGER.error("If connection is null key has to be either readable or connectable!");
                        // #endif /* LOGGER >= ERROR */

                        m_connectionCreator.closeConnection(null, true);
                    }
                    try {
                        complete = m_nioInterface.write(connection);
                    } catch (final IOException ignored) {
                        if (connection != null) {
                            // #if LOGGER >= DEBUG
                            LOGGER.debug("Could not write to channel (0x%X)!", connection.getDestination());
                            // #endif /* LOGGER >= DEBUG */
                        } else {
                            // #if LOGGER >= DEBUG
                            LOGGER.debug("Could not write to channel!");
                            // #endif /* LOGGER >= DEBUG */
                        }

                        m_connectionCreator.closeConnection(connection, true);

                        complete = false;
                    }

                    if (!complete) {
                        // If there is still data left to write on this connection, add another write request
                        m_changeLock.lock();
                        m_changeRequests.add(new ChangeOperationsRequest(connection, SelectionKey.OP_WRITE | SelectionKey.OP_READ));
                        m_changeLock.unlock();
                    }
                    // Set interest to READ after writing; do not if channel was blocked and data is left
                    p_key.interestOps(SelectionKey.OP_READ);
                } else if (p_key.isConnectable()) {
                    NIOInterface.connect(connection);
                }
            } catch (final IOException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not access channel properly!");
                // #endif /* LOGGER >= ERROR */
            }
        }
    }
}

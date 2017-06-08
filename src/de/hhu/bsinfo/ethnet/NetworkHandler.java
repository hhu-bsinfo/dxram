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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.ethnet.AbstractConnection.DataReceiver;
import de.hhu.bsinfo.utils.event.EventInterface;

/**
 * Access the network through Java NIO
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 * @author Marc Ewert, marc.ewert@hhu.de, 14.08.2014
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.11.2015
 */
public final class NetworkHandler implements DataReceiver {

    private static final Logger LOGGER = LogManager.getFormatterLogger(NetworkHandler.class.getSimpleName());
    private static final int DEFAULT_MESSAGE_STORE_SIZE = 100;
    private static final int EXCLUSIVE_MESSAGE_STORE_SIZE = 20;

    // Attributes
    private static EventInterface ms_eventInterface;

    private MessageReceiver[][] m_receivers;

    private final DefaultMessageHandlerPool m_defaultMessageHandlerPool;
    private final ExclusiveMessageHandler m_exclusiveMessageHandler;

    private MessageDirectory m_messageDirectory;
    private AbstractConnectionCreator m_connectionCreator;
    private ConnectionManager m_manager;
    private ReentrantLock m_receiversLock;

    private NodeMap m_nodeMap;

    private AtomicLongArray m_lastFailures;

    private int m_timeOut;

    // Constructors

    /**
     * Creates an instance of NetworkHandler
     *
     * @param p_numMessageHandlerThreads
     *     the number of default message handler (+ one exclusive message handler)
     * @param p_requestMapSize
     *     the number of entries in the request map
     * @param p_requestTimeOut
     *     the request time out in ms
     */
    public NetworkHandler(final int p_numMessageHandlerThreads, final int p_requestMapSize, final int p_requestTimeOut) {
        RequestMap.initialize(p_requestMapSize);

        m_timeOut = p_requestTimeOut;
        m_receivers = new MessageReceiver[100][100];
        m_receiversLock = new ReentrantLock(false);

        m_defaultMessageHandlerPool = new DefaultMessageHandlerPool(p_numMessageHandlerThreads);

        m_exclusiveMessageHandler = new ExclusiveMessageHandler();
        m_exclusiveMessageHandler.setName("Network: ExclusiveMessageHandler");
        m_exclusiveMessageHandler.start();

        m_messageDirectory = new MessageDirectory(p_requestTimeOut);

        m_lastFailures = new AtomicLongArray(65536);
    }

    /**
     * Returns the EventInterface
     *
     * @return the EventInterface
     */
    static EventInterface getEventHandler() {
        return ms_eventInterface;
    }

    /**
     * Sets the EventInterface
     *
     * @param p_event
     *     the EventInterface
     */
    public static void setEventHandler(final EventInterface p_event) {
        ms_eventInterface = p_event;

    }

    /**
     * Returns the status of the network module
     *
     * @return the status
     */
    public String getStatus() {
        String str = "";

        str += m_manager.getConnectionStatuses();
        str += m_connectionCreator.getSelectorStatus();

        return str;
    }

    /**
     * Registers a message type
     *
     * @param p_type
     *     the unique type
     * @param p_subtype
     *     the unique subtype
     * @param p_class
     *     the calling class
     */
    public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class) {
        boolean ret;

        ret = m_messageDirectory.register(p_type, p_subtype, p_class);

        // #if LOGGER >= WARN
        if (!ret) {
            LOGGER.warn("Registering network message %s for type %s and subtype %s failed, type and subtype already used", p_class.getSimpleName(), p_type,
                p_subtype);
        }
        // #endif /* LOGGER >= WARN */
    }

    // Methods

    /**
     * Initializes the network handler
     *
     * @param p_ownNodeID
     *     the own NodeID
     * @param p_nodeMap
     *     the node map
     * @param p_osBufferSize
     *     the size of incoming and outgoing buffers
     * @param p_flowControlWindowSize
     *     the maximal number of ByteBuffer to schedule for sending/receiving
     * @param p_connectionTimeout
     *     the connection timeout
     */
    public void initialize(final short p_ownNodeID, final NodeMap p_nodeMap, final int p_osBufferSize, final int p_flowControlWindowSize,
            final int p_connectionTimeout) {

        // #if LOGGER == TRACE
        LOGGER.trace("Entering initialize");
        // #endif /* LOGGER == TRACE */

        m_nodeMap = p_nodeMap;

        m_connectionCreator = new NIOConnectionCreator(m_messageDirectory, m_nodeMap, p_osBufferSize, p_flowControlWindowSize, p_connectionTimeout);
        m_connectionCreator.initialize(p_nodeMap.getAddress(p_ownNodeID).getPort());
        m_manager = new ConnectionManager(m_connectionCreator, this);

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting initialize");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Activates the connection manager
     */
    public void activateConnectionManager() {
        m_manager.activate();
    }

    /**
     * Deactivates the connection manager
     */
    public void deactivateConnectionManager() {
        m_manager.deactivate();
    }

    /**
     * Closes the network handler
     */
    public void close() {
        // Shutdown default message handler(s)
        m_defaultMessageHandlerPool.shutdown();

        // Shutdown exclusive message handler
        LockSupport.unpark(m_exclusiveMessageHandler);
        m_exclusiveMessageHandler.interrupt();
        m_exclusiveMessageHandler.shutdown();
        try {
            m_exclusiveMessageHandler.join();
            // #if LOGGER >= INFO
            LOGGER.info("Shutdown of ExclusiveMessageHandler successful");
            // #endif /* LOGGER >= INFO */
        } catch (final InterruptedException e) {
            // #if LOGGER >= WARN
            LOGGER.warn("Could not wait for exclusive message handler to finish. Interrupted");
            // #endif /* LOGGER >= WARN */
        }

        // Close connection manager (shuts down selector thread, too)
        m_manager.close();
    }

    /**
     * Registers a message receiver
     *
     * @param p_type
     *     the message type
     * @param p_subtype
     *     the message subtype
     * @param p_receiver
     *     the receiver
     */
    public void register(final byte p_type, final byte p_subtype, final MessageReceiver p_receiver) {
        if (p_receiver != null) {
            m_receiversLock.lock();
            // enlarge array
            if (m_receivers.length <= p_type) {
                final MessageReceiver[][] newArray = new MessageReceiver[p_type + 1][];
                System.arraycopy(m_receivers, 0, newArray, 0, m_receivers.length);
                m_receivers = newArray;
            }

            // create new sub array when it is not existing until now
            if (m_receivers[p_type] == null) {
                m_receivers[p_type] = new MessageReceiver[p_subtype + 1];
            }

            // enlarge subtype array
            if (m_receivers[p_type].length <= p_subtype) {
                final MessageReceiver[] newArray = new MessageReceiver[p_subtype + 1];
                System.arraycopy(m_receivers[p_type], 0, newArray, 0, m_receivers[p_type].length);
                m_receivers[p_type] = newArray;
            }

            if (m_receivers[p_type][p_subtype] != null) {
                // #if LOGGER >= WARN
                LOGGER.warn("Receiver for %d %d is already registered", p_type, p_subtype);
                // #endif /* LOGGER >= WARN */
            }
            m_receivers[p_type][p_subtype] = p_receiver;

            // #if LOGGER >= TRACE
            LOGGER.trace("Added new MessageReceiver %s for %d %d", p_receiver.getClass(), p_type, p_subtype);
            // #endif /* LOGGER >= TRACE */
            m_receiversLock.unlock();
        }
    }

    /**
     * Unregisters a message receiver
     *
     * @param p_type
     *     the message type
     * @param p_subtype
     *     the message subtype
     * @param p_receiver
     *     the receiver
     */
    public void unregister(final byte p_type, final byte p_subtype, final MessageReceiver p_receiver) {
        if (p_receiver != null) {
            m_receiversLock.lock();
            m_receivers[p_type][p_subtype] = null;
            m_receiversLock.unlock();
        }
    }

    /**
     * Connects a node.
     *
     * @param p_nodeID
     *     Node to connect
     * @throws NetworkException
     *     If sending the message failed
     */
    public void connectNode(final short p_nodeID) throws NetworkException {
        // #if LOGGER == TRACE
        LOGGER.trace("Entering connectNode with: p_nodeID=0x%X", p_nodeID);
        // #endif /* LOGGER == TRACE */

        try {
            if (m_manager.getConnection(p_nodeID) == null) {
                throw new IOException("Connection to " + NodeID.toHexString(p_nodeID) + " could not be established");
            }
        } catch (final IOException e) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("IOException during connection lookup", e);
            // #endif /* LOGGER >= DEBUG */
            throw new NetworkDestinationUnreachableException(p_nodeID);
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting connectNode");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Sends a message
     *
     * @param p_message
     *     the message to send
     * @throws NetworkException
     *     If sending the message failed
     */
    public void sendMessage(final AbstractMessage p_message) throws NetworkException {
        AbstractConnection connection;

        p_message.beforeSend();

        // #if LOGGER == TRACE
        LOGGER.trace("Entering sendMessage with: p_message=%s", p_message);
        // #endif /* LOGGER == TRACE */

        if (p_message.getDestination() == m_nodeMap.getOwnNodeID()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid destination 0x%X. No loopback allowed.", p_message.getDestination());
            // #endif /* LOGGER >= ERROR */
        } else {
            try {
                connection = m_manager.getConnection(p_message.getDestination());
            } catch (final IOException e) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Connection invalid. Ignoring connection exceptions regarding 0x%X during the next second!", p_message.getDestination());
                // #endif /* LOGGER >= DEBUG */
                throw new NetworkDestinationUnreachableException(p_message.getDestination());
            }
            try {
                if (connection != null) {
                    connection.write(p_message);
                } else {
                    long timestamp = m_lastFailures.get(p_message.getDestination() & 0xFFFF);
                    if (timestamp == 0 || timestamp + 1000 < System.currentTimeMillis()) {
                        m_lastFailures.set(p_message.getDestination() & 0xFFFF, System.currentTimeMillis());

                        // #if LOGGER >= DEBUG
                        LOGGER.debug("Connection invalid. Ignoring connection exceptions regarding 0x%X during the next second!", p_message.getDestination());
                        // #endif /* LOGGER >= DEBUG */
                        throw new NetworkDestinationUnreachableException(p_message.getDestination());
                    } else {
                        return;
                    }
                }
            } catch (final NetworkException e) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Sending data failed ", e);
                // #endif /* LOGGER >= DEBUG */
                throw new NetworkException("Sending data failed ", e);
            }
        }

        p_message.afterSend();

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting sendMessage");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Handles an incoming Message
     *
     * @param p_message
     *     the incoming Message
     */
    @Override
    public void newMessage(final AbstractMessage p_message) {
        // #if LOGGER == TRACE
        LOGGER.trace("Received new message: %s", p_message);
        // #endif /* LOGGER == TRACE */

        if (!p_message.isExclusive()) {
            m_defaultMessageHandlerPool.newMessage(p_message);
        } else {
            m_exclusiveMessageHandler.newMessage(p_message);
        }
    }

    @Override
    public void newMessages(final AbstractMessage[] p_messages) {
        // #if LOGGER == TRACE
        LOGGER.trace("Received new messages");
        // #endif /* LOGGER == TRACE */

        if (!p_messages[0].isExclusive()) {
            m_defaultMessageHandlerPool.newMessages(p_messages);
        } else {
            m_exclusiveMessageHandler.newMessages(p_messages);
        }
    }

    // Classes
    /**
     * Distributes incoming default messages
     *
     * @author Kevin Beineke 19.07.2016
     */
    private final class DefaultMessageHandlerPool {

        private final MessageStore m_defaultMessages;
        // Attributes
        private DefaultMessageHandler[] m_threads;
        private ReentrantLock m_defaultMessagesLock;

        // Constructors

        /**
         * Creates an instance of DefaultMessageHandlerPool
         *
         * @param p_numMessageHandlerThreads
         *     the number of default message handler
         */
        private DefaultMessageHandlerPool(final int p_numMessageHandlerThreads) {
            m_defaultMessages = new MessageStore(DEFAULT_MESSAGE_STORE_SIZE);
            m_defaultMessagesLock = new ReentrantLock(false);

            // #if LOGGER >= INFO
            LOGGER.info("Network: DefaultMessageHandlerPool: Initialising %d threads", p_numMessageHandlerThreads);
            // #endif /* LOGGER >= INFO */

            DefaultMessageHandler t;
            m_threads = new DefaultMessageHandler[p_numMessageHandlerThreads];
            for (int i = 0; i < m_threads.length; i++) {
                t = new DefaultMessageHandler(m_defaultMessages, m_defaultMessagesLock);
                t.setName("Network: DefaultMessageHandler " + (i + 1));
                m_threads[i] = t;
                t.start();
            }
        }

        // Methods

        /**
         * Closes alle default message handler
         */
        private void shutdown() {
            DefaultMessageHandler t;
            for (int i = 0; i < m_threads.length; i++) {
                t = m_threads[i];
                LockSupport.unpark(t);
                t.interrupt();
                t.shutdown();

                try {
                    t.join();
                    // #if LOGGER >= INFO
                    LOGGER.info("Shutdown of DefaultMessageHandler %d successful", i + 1);
                    // #endif /* LOGGER >= INFO */
                } catch (final InterruptedException e) {
                    // #if LOGGER >= WARN
                    LOGGER.warn("Could not wait for default message handler to finish. Interrupted");
                    // #endif /* LOGGER >= WARN */
                }
            }
        }

        /**
         * Enqueue a new message for delivering
         *
         * @param p_message
         *     the message
         */
        private void newMessage(final AbstractMessage p_message) {
            // Ignore network test messages (e.g. ping after response delay)
            if (p_message.getType() != 0 || p_message.getSubtype() != 0) {
                m_defaultMessagesLock.lock();
                while (!m_defaultMessages.pushMessage(p_message)) {
                    m_defaultMessagesLock.unlock();
                    for (Thread thread : m_threads) {
                        LockSupport.unpark(thread);
                    }
                    Thread.yield();
                    m_defaultMessagesLock.lock();
                }
                m_defaultMessagesLock.unlock();
                for (Thread thread : m_threads) {
                    LockSupport.unpark(thread);
                }
            }
        }

        private void newMessages(final AbstractMessage[] p_messages) {
            m_defaultMessagesLock.lock();
            for (AbstractMessage message : p_messages) {
                if (message == null) {
                    break;
                }

                // Ignore network test messages (e.g. ping after response delay)
                if (message.getType() != 0 || message.getSubtype() != 0) {
                    while (!m_defaultMessages.pushMessage(message)) {
                        m_defaultMessagesLock.unlock();
                        for (Thread thread : m_threads) {
                            LockSupport.unpark(thread);
                        }
                        Thread.yield();
                        m_defaultMessagesLock.lock();
                    }
                }
            }
            m_defaultMessagesLock.unlock();
            for (Thread thread : m_threads) {
                LockSupport.unpark(thread);
            }
        }
    }

    /**
     * Executes incoming default messages
     *
     * @author Kevin Beineke 19.07.2016
     */
    private final class DefaultMessageHandler extends Thread {

        // Attributes
        private MessageStore m_defaultMessages;
        private ReentrantLock m_defaultMessagesLock;
        private volatile boolean m_shutdown;

        // Constructors

        /**
         * Creates an instance of DefaultMessageHandler
         *
         * @param p_queue
         *     the message queue
         * @param p_lock
         *     the lock for accessing message queue
         */
        private DefaultMessageHandler(final MessageStore p_queue, final ReentrantLock p_lock) {
            m_defaultMessages = p_queue;
            m_defaultMessagesLock = p_lock;
        }

        // Methods
        @Override
        public void run() {
            long time;
            AbstractMessage message = null;
            MessageReceiver messageReceiver;

            while (!m_shutdown) {
                while (message == null && !m_shutdown) {
                    m_defaultMessagesLock.lock();
                    if (m_defaultMessages.isEmpty()) {
                        m_defaultMessagesLock.unlock();
                        LockSupport.park();
                        m_defaultMessagesLock.lock();
                    }

                    message = m_defaultMessages.popMessage();
                    m_defaultMessagesLock.unlock();
                }

                if (m_shutdown) {
                    break;
                }

                messageReceiver = m_receivers[message.getType()][message.getSubtype()];

                // Try again in a loop, if receivers were not registered. Stop if request timeout is reached as answering later has no effect
                if (messageReceiver == null) {
                    // #if LOGGER >= WARN
                    LOGGER.warn("Message receiver null for %d, %d! Waiting...", + message.getType(), message.getSubtype());
                    // #endif /* LOGGER >= WARN */
                    time = System.currentTimeMillis();
                    while (messageReceiver == null && System.currentTimeMillis() < time + m_timeOut) {
                        m_receiversLock.lock();
                        messageReceiver = m_receivers[message.getType()][message.getSubtype()];
                        m_receiversLock.unlock();
                    }
                }

                if (messageReceiver != null) {
                    messageReceiver.onIncomingMessage(message);
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("No message receiver was registered for %d, %d!", message.getType(), message.getSubtype());
                    // #endif /* LOGGER >= ERROR */
                }
                message = null;
            }
        }

        /**
         * Closes the handler
         */
        private void shutdown() {
            m_shutdown = true;
        }
    }

    /**
     * Executes incoming exclusive messages
     *
     * @author Kevin Beineke 19.07.2016
     */
    private class ExclusiveMessageHandler extends Thread {

        // Attributes
        private final MessageStore m_exclusiveMessages;
        private ReentrantLock m_exclusiveMessagesLock;
        private volatile boolean m_shutdown;

        // Constructors

        /**
         * Creates an instance of MessageHandler
         */
        ExclusiveMessageHandler() {
            m_exclusiveMessages = new MessageStore(EXCLUSIVE_MESSAGE_STORE_SIZE);
            m_exclusiveMessagesLock = new ReentrantLock(false);
        }

        // Methods

        /**
         * Closes the handler
         */
        public void shutdown() {
            m_shutdown = true;
        }

        @Override
        public void run() {
            long time;
            AbstractMessage message = null;
            MessageReceiver messageReceiver;

            while (!m_shutdown) {
                while (message == null && !m_shutdown) {
                    m_exclusiveMessagesLock.lock();
                    if (m_exclusiveMessages.isEmpty()) {
                        m_exclusiveMessagesLock.unlock();
                        LockSupport.park();
                        m_exclusiveMessagesLock.lock();
                    }

                    message = m_exclusiveMessages.popMessage();
                    m_exclusiveMessagesLock.unlock();
                }

                if (m_shutdown) {
                    break;
                }

                messageReceiver = m_receivers[message.getType()][message.getSubtype()];

                // Try again in a loop, if receivers were not registered. Stop if request timeout is reached as answering later has no effect
                if (messageReceiver == null) {
                    // #if LOGGER >= WARN
                    LOGGER.warn("Message receiver null for %d, %d! Waiting...", + message.getType(), message.getSubtype());
                    // #endif /* LOGGER >= WARN */
                    time = System.currentTimeMillis();
                    while (messageReceiver == null && System.currentTimeMillis() < time + m_timeOut) {
                        m_receiversLock.lock();
                        messageReceiver = m_receivers[message.getType()][message.getSubtype()];
                        m_receiversLock.unlock();
                    }
                }

                if (messageReceiver != null) {
                    messageReceiver.onIncomingMessage(message);
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("No message receiver was registered for %d, %d", message.getType(), message.getSubtype());
                    // #endif /* LOGGER >= ERROR */
                }
                message = null;
            }
        }

        /**
         * Enqueue a new message for delivering
         *
         * @param p_message
         *     the message
         */
        void newMessage(final AbstractMessage p_message) {
            m_exclusiveMessagesLock.lock();
            while(!m_exclusiveMessages.pushMessage(p_message)) {
                m_exclusiveMessagesLock.unlock();
                LockSupport.unpark(this);
                Thread.yield();
                m_exclusiveMessagesLock.lock();
            }
            m_exclusiveMessagesLock.unlock();
            LockSupport.unpark(this);
        }

        void newMessages(final AbstractMessage[] p_messages) {
            m_exclusiveMessagesLock.lock();
            for (AbstractMessage message : p_messages) {
                if (message == null) {
                    break;
                }

                while (!m_exclusiveMessages.pushMessage(message)) {
                    m_exclusiveMessagesLock.unlock();
                    LockSupport.unpark(this);
                    Thread.yield();
                    m_exclusiveMessagesLock.lock();
                }
            }
            m_exclusiveMessagesLock.unlock();
            LockSupport.unpark(this);
        }
    }

    /**
     * Methods for reacting on incoming Messages
     *
     * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
     */
    public interface MessageReceiver {

        // Methods

        /**
         * Handles an incoming Message
         *
         * @param p_message
         *     the Message
         */
        void onIncomingMessage(AbstractMessage p_message);

    }
}

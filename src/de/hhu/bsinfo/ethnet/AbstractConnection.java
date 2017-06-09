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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.NodeID;

/**
 * Represents a network connections
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 * @author Marc Ewert, marc.ewert@hhu.de, 14.10.2014
 */
abstract class AbstractConnection {

    /**
     * Represents the steps in the creation process
     *
     * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
     */
    private enum Step {

        // Constants
        READ_HEADER, READ_PAYLOAD, DONE

    }

    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractConnection.class.getSimpleName());
    // Attributes
    private final DataHandler m_dataHandler;
    private final ByteStreamInterpreter m_streamInterpreter;
    private short m_destination;
    private NodeMap m_nodeMap;
    private MessageDirectory m_messageDirectory;
    private RequestMap m_requestMap;
    private volatile DataReceiver m_listener;
    private long m_creationTimestamp;
    private long m_lastAccessTimestamp;
    private long m_closingTimestamp;
    private int m_unconfirmedBytes;
    private int m_receivedBytes;
    private int m_sentMessages;
    private int m_receivedMessages;
    private int m_flowControlWindowSize;
    private ReentrantLock m_flowControlCondLock;
    private Condition m_flowControlCond;

    private volatile boolean m_outgoingConnected;
    private volatile boolean m_incomingConnected;

    // Constructors

    /**
     * Creates an instance of AbstractConnection
     *
     * @param p_destination
     *         the destination
     * @param p_nodeMap
     *         the node map
     * @param p_messageDirectory
     *         the message directory
     * @param p_requestMap
     *         the request map
     * @param p_flowControlWindowSize
     *         the maximal number of ByteBuffer to schedule for sending/receiving
     */
    AbstractConnection(final short p_destination, final NodeMap p_nodeMap, final MessageDirectory p_messageDirectory, final RequestMap p_requestMap,
            final int p_flowControlWindowSize) {
        assert p_destination != NodeID.INVALID_ID;

        m_dataHandler = new DataHandler();
        m_streamInterpreter = new ByteStreamInterpreter();

        m_destination = p_destination;
        m_nodeMap = p_nodeMap;
        m_requestMap = p_requestMap;
        m_messageDirectory = p_messageDirectory;

        m_outgoingConnected = false;
        m_incomingConnected = false;

        m_creationTimestamp = System.currentTimeMillis();
        m_lastAccessTimestamp = 0;
        m_closingTimestamp = -1;

        m_flowControlWindowSize = p_flowControlWindowSize;
        m_flowControlCondLock = new ReentrantLock(false);
        m_flowControlCond = m_flowControlCondLock.newCondition();
    }

    /**
     * Returns whether the outgoing socket channel is open or not
     *
     * @return true if outgoing socket is open
     */
    protected abstract boolean isOutgoingOpen();

    /**
     * Returns whether the incoming socket channel is open or not
     *
     * @return true if incoming socket is open
     */
    protected abstract boolean isIncomingOpen();

    /**
     * Returns the size of input and output queues
     *
     * @return the queue sizes
     */
    protected abstract String getInputOutputQueueLength();

    /**
     * Writes data to the connection
     *
     * @param p_message
     *         the AbstractMessage to send
     * @throws NetworkException
     *         if message buffer is too small
     */
    protected abstract void doWrite(AbstractMessage p_message) throws NetworkException;

    /**
     * Writes flow control data to the connection without delay
     *
     * @throws NetworkException
     *         if message buffer is too small
     */
    protected abstract void doFlowControlWrite() throws NetworkException;

    /**
     * Returns whether there is data left to send in output queue
     *
     * @return whether the output queue is empty (false) or not (true)
     */
    protected abstract boolean dataLeftToWrite();

    /**
     * Closes the connection immediately
     */
    protected abstract void doClose();

    /**
     * Closes the connection when there is no data left in transfer
     */
    protected abstract void doCloseGracefully();

    /**
     * Wakes up the connection manager (e.g. Selector for NIO)
     */
    protected abstract void wakeup();

    /**
     * Get the destination
     *
     * @return the destination
     */
    public final short getDestination() {
        return m_destination;
    }

    /**
     * Get the creation timestamp
     *
     * @return the creation timestamp
     */
    public final long getCreationTimestamp() {
        return m_creationTimestamp;
    }

    /**
     * Get the timestamp of the last access
     *
     * @return the timestamp of the last access
     */
    public final long getLastAccessTimestamp() {
        return m_lastAccessTimestamp;
    }

    /**
     * Get the String representation
     *
     * @return the String representation
     */
    @Override
    public String toString() {
        String ret;

        m_flowControlCondLock.lock();
        ret = getClass().getSimpleName() + '[' + NodeID.toHexString(m_destination) + ", outgoing: " + (isOutgoingOpen() ? "open" : "not open") +
                ", incoming: " + (isIncomingOpen() ? "open" : "not open") + ", sent(messages): " + m_sentMessages + ", received(messages): " +
                m_receivedMessages + ", unconfirmed(b): " + m_unconfirmedBytes + ", received_to_confirm(b): " + m_receivedBytes + ", buffer queues: " +
                getInputOutputQueueLength();
        m_flowControlCondLock.unlock();

        return ret;
    }

    /**
     * Writes data to the connection
     *
     * @param p_message
     *         the AbstractMessage to send
     */
    protected final void write(final AbstractMessage p_message) {
        m_flowControlCondLock.lock();
        while (m_unconfirmedBytes > m_flowControlWindowSize) {
            try {
                if (!m_flowControlCond.await(1000, TimeUnit.MILLISECONDS)) {
                    // #if LOGGER >= WARN
                    LOGGER.warn("Flow control message is overdue for node: 0x%X", m_destination);
                    // #endif /* LOGGER >= WARN */
                }
            } catch (final InterruptedException e) { /* ignore */ }
        }
        m_unconfirmedBytes += p_message.getPayloadLength() + AbstractMessage.HEADER_SIZE;
        m_sentMessages++;
        m_flowControlCondLock.unlock();

        try {
            doWrite(p_message);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not send message: %s\n%s", p_message, e);
            // #endif /* LOGGER >= ERROR */

            return;
        }

        m_lastAccessTimestamp = System.currentTimeMillis();
    }

    /**
     * Closes the connection immediately
     */
    protected final void close() {
        doClose();
    }

    /**
     * Called when the connection was closed.
     */
    protected void cleanup() {
    }

    /**
     * Get current number of confirmed bytes and reset for flow control
     *
     * @return the number of confirmed bytes
     */
    int getAndResetConfirmedBytes() {
        int ret;

        m_flowControlCondLock.lock();
        ret = m_receivedBytes;

        // Reset received bytes counter
        m_receivedBytes = 0;
        m_flowControlCondLock.unlock();

        return ret;
    }

    void handleFlowControlMessage(final int p_confirmedBytes) {
        m_dataHandler.handleFlowControlMessage(p_confirmedBytes);
    }

    /**
     * Checks if the connection is connected for outgoing traffic
     *
     * @return true if the connection is connected, false otherwise
     */
    final boolean isOutgoingConnected() {
        return m_outgoingConnected;
    }

    /**
     * Checks if the connection is connected for incoming traffic
     *
     * @return true if the connection is connected, false otherwise
     */
    final boolean isIncomingConnected() {
        return m_incomingConnected;
    }

    /**
     * Marks the connection as (not) connected
     *
     * @param p_connected
     *         if true the connection is marked as connected, otherwise the connections marked as not connected
     */
    final void setConnected(final boolean p_connected, final boolean p_outgoing) {
        if (p_outgoing) {
            m_outgoingConnected = p_connected;
        } else {
            m_incomingConnected = p_connected;
        }
    }

    /**
     * Get node map
     *
     * @return the NodeMap
     */
    final NodeMap getNodeMap() {
        return m_nodeMap;
    }

    /**
     * Get the closing timestamp
     *
     * @return the closing timestamp
     */
    final long getClosingTimestamp() {
        return m_closingTimestamp;
    }

    /**
     * Set the ConnectionListener
     *
     * @param p_listener
     *         the ConnectionListener
     */
    final void setListener(final DataReceiver p_listener) {
        m_listener = p_listener;
    }

    /**
     * Set the closing timestamp
     */
    final void setClosingTimestamp() {
        m_closingTimestamp = System.currentTimeMillis();
    }

    /**
     * Forward buffer to DataHandler to fill byte stream and create messages.
     *
     * @param p_buffer
     *         the new buffer
     */
    final void processBuffer(final ByteBuffer p_buffer) {
        m_dataHandler.processBuffer(p_buffer);
    }

    /**
     * Closes the connection when there is no data left in transfer
     */
    final void closeGracefully() {
        doCloseGracefully();
    }

    // Classes

    /**
     * Creates ByteBuffers containing AbstractMessages from ByteBuffer-Chunks
     *
     * @author Florian Klein 09.03.2012
     * @author Marc Ewert 28.10.2014
     */
    private static class ByteStreamInterpreter {

        // TODO: Further evaluate direct byte buffer (in comments)

        // Attributes
        private ByteBuffer m_headerBytes;
        private ByteBuffer m_messageBytes;

        private int m_payloadSize;

        private Step m_step;
        private boolean m_wasCopied;

        // Constructors

        /**
         * Creates an instance of MessageCreator
         */
        ByteStreamInterpreter() {
            m_headerBytes = ByteBuffer.allocate(AbstractMessage.HEADER_SIZE);
            m_payloadSize = 0;
            clear();
        }

        // Getters

        /**
         * Clear all data
         */
        public void clear() {
            m_headerBytes.clear();
            m_payloadSize = 0;
            m_step = Step.READ_HEADER;
            m_wasCopied = false;
        }

        /**
         * Updates the current data
         *
         * @param p_buffer
         *         the ByteBuffer with new data
         */
        public void update(final ByteBuffer p_buffer) {
            assert p_buffer != null;

            while (m_step != Step.DONE && p_buffer.hasRemaining()) {
                switch (m_step) {
                    case READ_HEADER:
                        readHeader(p_buffer);
                        break;
                    case READ_PAYLOAD:
                        readPayload(p_buffer);
                        break;
                    default:
                        break;
                }
            }
        }

        // Methods

        /**
         * Get the created Message
         *
         * @return the created Message
         */
        final ByteBuffer getMessageBuffer() {
            return m_messageBytes;
        }

        /**
         * Returns the payload size
         *
         * @return the payload size
         */
        final int getPayloadSize() {
            return m_payloadSize;
        }

        /**
         * Returns whether the message buffer was copied or not
         *
         * @return true if a new allocated buffer was used, false otherwise
         */
        final boolean bufferWasCopied() {
            return m_wasCopied;
        }

        /**
         * Checks if Message is complete
         *
         * @return true if the Message is complete, false otherwise
         */
        boolean isMessageComplete() {
            return m_step == Step.DONE;
        }

        /**
         * Reads the remaining message header
         *
         * @param p_buffer
         *         the ByteBuffer with the data
         */
        private void readHeader(final ByteBuffer p_buffer) {
            try {
                final int remaining = m_headerBytes.remaining();

                if (p_buffer.remaining() < remaining) {
                    // Header is split -> copy header and payload incrementally in a new byte buffer
                    m_wasCopied = true;
                    m_headerBytes.put(p_buffer);
                    // Header partially filled
                } else {
                    if (m_headerBytes.position() != 0) {
                        // Header is split and the remaining bytes are in this buffer
                        m_headerBytes.put(p_buffer.array(), p_buffer.position(), remaining);
                        p_buffer.position(p_buffer.position() + remaining);

                        /*int limit = p_buffer.limit();
                        p_buffer.limit(p_buffer.position() + remaining);
                        m_headerBytes.put(p_buffer);
                        p_buffer.limit(limit);*/

                        // Header complete

                        // Read payload size (copied at the end of m_headerBytes before)
                        m_payloadSize = m_headerBytes.getInt(m_headerBytes.limit() - AbstractMessage.PAYLOAD_SIZE_LENGTH);

                        // Create message buffer and copy header into (without payload size)
                        m_messageBytes = ByteBuffer.allocate(AbstractMessage.HEADER_SIZE - AbstractMessage.PAYLOAD_SIZE_LENGTH + m_payloadSize);
                        m_messageBytes.put(m_headerBytes.array(), 0, AbstractMessage.HEADER_SIZE - AbstractMessage.PAYLOAD_SIZE_LENGTH);

                        /*m_headerBytes.position(0);
                        m_headerBytes.limit(m_headerBytes.capacity() - AbstractMessage.PAYLOAD_SIZE_LENGTH);
                        m_messageBytes.put(m_headerBytes);*/

                        if (m_payloadSize == 0) {
                            // There is no payload -> message complete
                            m_step = Step.DONE;
                            m_messageBytes.flip();
                        } else {
                            // Payload must be read next
                            m_step = Step.READ_PAYLOAD;
                        }
                    } else {
                        int currentPosition = p_buffer.position();

                        // Read payload size
                        m_payloadSize = p_buffer.getInt(currentPosition + AbstractMessage.HEADER_SIZE - AbstractMessage.PAYLOAD_SIZE_LENGTH);
                        if (currentPosition + m_payloadSize + AbstractMessage.HEADER_SIZE <= p_buffer.limit()) {
                            // Complete message is in this buffer -> avoid copying by using this buffer for de-serialization
                            m_messageBytes = p_buffer;
                            m_step = Step.DONE;
                            m_messageBytes.position(currentPosition);
                        } else {
                            // Create message buffer and copy header into (without payload size)
                            m_wasCopied = true;

                            m_messageBytes = ByteBuffer.allocate(AbstractMessage.HEADER_SIZE - AbstractMessage.PAYLOAD_SIZE_LENGTH + m_payloadSize);
                            m_messageBytes.put(p_buffer.array(), p_buffer.position(), AbstractMessage.HEADER_SIZE - AbstractMessage.PAYLOAD_SIZE_LENGTH);
                            p_buffer.position(p_buffer.position() + remaining);

                            /*int limit = p_buffer.limit();
                            p_buffer.position(currentPosition);
                            p_buffer.limit(currentPosition + AbstractMessage.HEADER_SIZE - AbstractMessage.PAYLOAD_SIZE_LENGTH);
                            m_messageBytes.put(p_buffer);
                            p_buffer.limit(limit);
                            p_buffer.getInt();*/

                            // Payload must be read next
                            m_step = Step.READ_PAYLOAD;
                        }
                    }
                }
            } catch (final Exception e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Unable to read message header ", e);
                // #endif /* LOGGER >= ERROR */
                clear();
            }
        }

        /**
         * Reads the message payload
         *
         * @param p_buffer
         *         the ByteBuffer with the data
         */
        private void readPayload(final ByteBuffer p_buffer) {
            try {
                final int remaining = m_messageBytes.remaining();

                if (p_buffer.remaining() < remaining) {
                    m_messageBytes.put(p_buffer);
                } else {
                    m_messageBytes.put(p_buffer.array(), p_buffer.position(), remaining);
                    p_buffer.position(p_buffer.position() + remaining);

                    /*int limit = p_buffer.limit();
                    p_buffer.limit(p_buffer.position() + remaining);
                    m_messageBytes.put(p_buffer);
                    p_buffer.limit(limit);*/

                    m_step = Step.DONE;
                    m_messageBytes.flip();
                }
            } catch (final Exception e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Unable to read message payload ", e);
                // #endif /* LOGGER >= ERROR */
                clear();
            }
        }
    }

    /**
     * Reacts on incoming data
     *
     * @author Florian Klein 23.07.2013
     * @author Marc Ewert 16.09.2014
     */
    private class DataHandler {

        // Attributes
        AbstractMessage[] m_normalMessages = new AbstractMessage[25];
        AbstractMessage[] m_exclusiveMessages = new AbstractMessage[25];

        /**
         * Default constructor
         */
        DataHandler() {
        }

        // Methods

        /**
         * Adds a buffer to byte stream and creates a message if all data was gathered.
         *
         * @param p_buffer
         *         the new buffer
         */
        void processBuffer(final ByteBuffer p_buffer) {
            int counterNormal = 0;
            int counterExclusive = 0;
            AbstractMessage currentMessage;

            // Flow-control
            m_flowControlCondLock.lock();
            m_receivedBytes += p_buffer.remaining();
            if (m_receivedBytes > m_flowControlWindowSize * 0.8) {
                sendFlowControlData();
            }
            m_flowControlCondLock.unlock();

            m_lastAccessTimestamp = System.currentTimeMillis();

            while (p_buffer.hasRemaining()) {
                m_streamInterpreter.update(p_buffer);

                if (m_streamInterpreter.isMessageComplete()) {
                    currentMessage =
                            createMessage(m_streamInterpreter.getMessageBuffer(), m_streamInterpreter.getPayloadSize(), m_streamInterpreter.bufferWasCopied());

                    if (currentMessage != null) {
                        currentMessage.setDestination(m_nodeMap.getOwnNodeID());
                        currentMessage.setSource(m_destination);

                        if (currentMessage.isResponse()) {
                            m_requestMap.fulfill((AbstractResponse) currentMessage);
                        } else {
                            if (!currentMessage.isExclusive()) {
                                m_normalMessages[counterNormal++] = currentMessage;
                                if (counterNormal == m_normalMessages.length) {
                                    deliverMessages(m_normalMessages);
                                    Arrays.fill(m_normalMessages, null);
                                    counterNormal = 0;
                                }
                            } else {
                                m_exclusiveMessages[counterExclusive++] = currentMessage;
                                if (counterExclusive == m_exclusiveMessages.length) {
                                    deliverMessages(m_exclusiveMessages);
                                    Arrays.fill(m_exclusiveMessages, null);
                                    counterExclusive = 0;
                                }
                            }
                        }
                        m_receivedMessages++;
                    }
                    m_streamInterpreter.clear();
                }
            }

            if (counterNormal > 0) {
                deliverMessages(m_normalMessages);
                Arrays.fill(m_normalMessages, 0, counterNormal, null);
            }
            if (counterExclusive > 0) {
                deliverMessages(m_exclusiveMessages);
                Arrays.fill(m_exclusiveMessages, 0, counterExclusive, null);
            }
        }

        /**
         * Handles a received flow control data
         *
         * @param p_confirmedBytes
         *         the number of confirmed bytes
         */
        void handleFlowControlMessage(final int p_confirmedBytes) {
            m_flowControlCondLock.lock();
            m_unconfirmedBytes -= p_confirmedBytes;

            m_flowControlCond.signalAll();
            m_flowControlCondLock.unlock();
        }

        /**
         * Informs the ConnectionListener about a new message
         *
         * @param p_message
         *         the new message
         */
        private void deliverMessage(final AbstractMessage p_message) {

            if (m_listener == null) {
                // #if LOGGER >= ERROR
                LOGGER.error("No listener registered. Waiting...");
                // #endif /* LOGGER >= ERROR */

                while (m_listener == null) {
                    Thread.yield();
                }
            }
            m_listener.newMessage(p_message);
        }

        /**
         * Informs the ConnectionListener about new messages
         *
         * @param p_messages
         *         the new messages
         */
        private void deliverMessages(final AbstractMessage[] p_messages) {

            if (m_listener == null) {
                // #if LOGGER >= ERROR
                LOGGER.error("No listener registered. Waiting...");
                // #endif /* LOGGER >= ERROR */

                while (m_listener == null) {
                    Thread.yield();
                }
            }
            m_listener.newMessages(p_messages);
        }

        /**
         * Confirm received bytes for the other node
         */
        private void sendFlowControlData() {
            try {
                doFlowControlWrite();
            } catch (final NetworkException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not send flow control message", e);
                // #endif /* LOGGER >= ERROR */
            }
        }

        /**
         * Create a message from a given buffer
         *
         * @param p_buffer
         *         buffer containing a message
         * @return message
         */
        private AbstractMessage createMessage(final ByteBuffer p_buffer, final int p_payloadSize, final boolean p_wasCopied) {
            int readBytes = 0;
            int initialBufferPosition = p_buffer.position();
            AbstractMessage message = null;
            AbstractRequest request;
            AbstractResponse response;

            try {
                message = AbstractMessage.createMessageHeader(p_buffer, m_messageDirectory);

                if (p_buffer.limit() > p_payloadSize + AbstractMessage.HEADER_SIZE - AbstractMessage.PAYLOAD_SIZE_LENGTH) {
                    // Skip field for payload size if the message buffer was not copied
                    p_buffer.getInt();
                    readBytes = -4;
                }

                // hack:
                // to avoid copying data multiple times, some responses use the same objects provided
                // with the request to directly write the data to them instead of creating a temporary
                // object in the response, de-serializing the data and then copying from the temporary object
                // to the object that should receive the data in the first place. (example DXRAM: get request/response)
                // This is only possible, if we have a reference to the original request within the response
                // while reading from the network byte buffer. But in this low level stage, we (usually) don't have
                // access to requests/responses. So we exploit the request map to get our corresponding request
                // before de-serializing the network buffer for every request.
                if (message.isResponse()) {
                    response = (AbstractResponse) message;
                    request = m_requestMap.getRequest(response);
                    if (request == null) {
                        p_buffer.position(p_buffer.position() + p_payloadSize);
                        // Request is not available, probably because of a time-out
                        return null;
                    }
                    response.setCorrespondingRequest(request);
                }

                try {
                    message.readPayload(p_buffer, p_payloadSize, p_wasCopied);
                } catch (final BufferUnderflowException e) {
                    throw new IOException("Read beyond message buffer", e);
                }

                readBytes += p_buffer.position() - initialBufferPosition;
                int calculatedPayloadSize = message.getPayloadLength() + AbstractMessage.HEADER_SIZE - AbstractMessage.PAYLOAD_SIZE_LENGTH;
                if (readBytes < calculatedPayloadSize) {
                    throw new IOException("Message buffer is too large: " + calculatedPayloadSize + " > " + readBytes + " (read payload without metadata: " +
                            (readBytes - AbstractMessage.HEADER_SIZE + AbstractMessage.PAYLOAD_SIZE_LENGTH) + " bytes)");
                }
            } catch (final Exception e) {
                // #if LOGGER >= ERROR
                if (message != null) {
                    LOGGER.error("Unable to create message: %s", message, e);
                } else {
                    LOGGER.error("Unable to create message", e);
                }
                // #endif /* LOGGER >= ERROR */
            }

            return message;
        }
    }

    /**
     * Manages for reacting to connections
     *
     * @author Marc Ewert 11.04.2014
     */
    interface DataReceiver {

        // Methods

        /**
         * New messsage is available
         *
         * @param p_message
         *         the message which has been received
         */
        void newMessage(AbstractMessage p_message);

        void newMessages(AbstractMessage[] p_messages);

    }
}

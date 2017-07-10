package de.hhu.bsinfo.net.core;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.NodeID;

/**
 * Created by nothaas on 6/9/17.
 */
public abstract class AbstractPipeIn {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractPipeIn.class.getSimpleName());

    private final short m_ownNodeID;
    private final short m_destinationNodeID;
    private volatile boolean m_isConnected;
    private final AbstractFlowControl m_flowControl;

    private final DataReceiver m_listener;
    private final MessageDirectory m_messageDirectory;
    private final RequestMap m_requestMap;

    private final ByteStreamInterpreter m_streamInterpreter;

    private long m_receivedMessages;

    private final AbstractMessage[] m_normalMessages = new AbstractMessage[25];
    private final AbstractMessage[] m_exclusiveMessages = new AbstractMessage[25];

    protected AbstractPipeIn(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl,
            final MessageDirectory p_messageDirectory, final RequestMap p_requestMap, final DataReceiver p_dataReceiver, final boolean p_useDirectBuffer) {
        m_ownNodeID = p_ownNodeId;
        m_destinationNodeID = p_destinationNodeId;
        m_flowControl = p_flowControl;

        m_listener = p_dataReceiver;
        m_messageDirectory = p_messageDirectory;
        m_requestMap = p_requestMap;

        m_streamInterpreter = new ByteStreamInterpreter(p_useDirectBuffer);
    }

    short getOwnNodeID() {
        return m_ownNodeID;
    }

    public short getDestinationNodeID() {
        return m_destinationNodeID;
    }

    public boolean isConnected() {
        return m_isConnected;
    }

    public void setConnected(final boolean p_connected) {
        m_isConnected = p_connected;
    }

    public long getReceivedMessageCount() {
        return m_receivedMessages;
    }

    protected AbstractFlowControl getFlowControl() {
        return m_flowControl;
    }

    public abstract boolean isOpen();

    public abstract void returnProcessedBuffer(final ByteBuffer p_buffer);

    /**
     * Adds a buffer to byte stream and creates a message if all data was gathered.
     *
     * @param p_buffer
     *         the new buffer
     */
    void processBuffer(final ByteBuffer p_buffer, AbstractMessageImporter p_importer) {
        int counterNormal = 0;
        int counterExclusive = 0;
        AbstractMessage currentMessage;

        m_flowControl.dataReceived(p_buffer.remaining());

        while (p_buffer.hasRemaining()) {
            m_streamInterpreter.update(p_buffer);

            if (m_streamInterpreter.isMessageComplete()) {
                currentMessage =
                        createMessage(m_streamInterpreter.getMessageBuffer(), m_streamInterpreter.getPayloadSize(), m_streamInterpreter.bufferWasCopied(),
                                p_importer);

                if (currentMessage != null) {
                    currentMessage.setDestination(m_ownNodeID);
                    currentMessage.setSource(m_destinationNodeID);

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

    @Override
    public String toString() {
        return "PipeIn[m_ownNodeID " + NodeID.toHexString(m_ownNodeID) + ", m_destinationNodeID " + NodeID.toHexString(m_destinationNodeID) +
                ", m_flowControl " + m_flowControl + ", m_receivedMessages " + m_receivedMessages + ']';
    }

    /**
     * Informs the ConnectionListener about new messages
     *
     * @param p_messages
     *         the new messages
     */
    private void deliverMessages(final AbstractMessage[] p_messages) {
        m_listener.newMessages(p_messages);
    }

    /**
     * Create a message from a given buffer
     *
     * @param p_buffer
     *         buffer containing a message
     * @return message
     */
    private AbstractMessage createMessage(final ByteBuffer p_buffer, final int p_payloadSize, final boolean p_wasCopied,
            final AbstractMessageImporter p_importer) {
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
                p_importer.setBuffer(p_buffer.array());
                p_importer.setPosition(p_buffer.position());
                message.readPayload(p_importer, p_buffer, p_payloadSize, p_wasCopied);
                p_buffer.position(p_importer.getPosition());
            } catch (final BufferUnderflowException e) {
                throw new IOException("Read beyond message buffer: p_buffer " + p_buffer + ", p_payloadSize " + p_payloadSize + ", p_wasCopied " + p_wasCopied,
                        e);
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

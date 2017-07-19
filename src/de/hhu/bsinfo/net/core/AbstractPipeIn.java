package de.hhu.bsinfo.net.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.nio.NIOMessageImporterCollection;
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

    private AbstractMessageImporterCollection m_importers;

    private long m_receivedMessages;

    private AbstractMessage m_currentMessage;
    private MessageHeader m_currentHeader = new MessageHeader();

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

        m_importers = new NIOMessageImporterCollection();
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
    void processBuffer(final ByteBuffer p_buffer) {
        int counterNormal = 0;
        int counterExclusive = 0;

        m_flowControl.dataReceived(p_buffer.remaining());

        // System.out.println("Incoming buffer: " + p_buffer);
        while (p_buffer.hasRemaining()) {
            if (m_currentMessage != null || readHeader(p_buffer)) {
                try {
                    if (m_currentMessage == null) {
                        m_currentMessage = createMessage();
                    }

                    if (readPayload(p_buffer)) {
                        if (m_currentMessage.isResponse()) {
                            m_requestMap.fulfill((AbstractResponse) m_currentMessage);
                        } else {
                            if (!m_currentMessage.isExclusive()) {
                                m_normalMessages[counterNormal++] = m_currentMessage;
                                if (counterNormal == m_normalMessages.length) {
                                    deliverMessages(m_normalMessages);
                                    Arrays.fill(m_normalMessages, null);
                                    counterNormal = 0;
                                }
                            } else {
                                m_exclusiveMessages[counterExclusive++] = m_currentMessage;
                                if (counterExclusive == m_exclusiveMessages.length) {
                                    deliverMessages(m_exclusiveMessages);
                                    Arrays.fill(m_exclusiveMessages, null);
                                    counterExclusive = 0;
                                }
                            }
                        }
                        m_currentMessage = null;
                        m_currentHeader.clear();
                        m_receivedMessages++;
                    }
                } catch (final Exception e) {
                    // #if LOGGER >= ERROR
                    if (m_currentMessage != null) {
                        LOGGER.error("Unable to create message: %s", m_currentMessage, e);
                    } else {
                        LOGGER.error("Unable to create message", e);
                    }
                    // #endif /* LOGGER >= ERROR */
                }
            } else {
                // System.out.println("Message header unfinished");
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

    private boolean readHeader(final ByteBuffer p_buffer) {
        AbstractMessageImporter importer;
        importer = m_importers.getImporter(p_buffer.remaining() < AbstractMessage.HEADER_SIZE);
        importer.setBuffer(p_buffer.array(), p_buffer.position(), p_buffer.limit());

        //System.out.println("Reading header; " + p_buffer);

        try {
            importer.importObject(m_currentHeader);
        } catch (final ArrayIndexOutOfBoundsException e) {
            // Header is incomplete continue later
            // System.out.println("Out of bounds during reading header; " + p_buffer);
            //System.out.println("Out of bounds during reading header; position: " + p_buffer.remaining());
            p_buffer.position(importer.getPosition());
            m_importers.returnImporter(importer, false);
            return false;
        }
        p_buffer.position(importer.getPosition());
        m_importers.returnImporter(importer, true);
        //System.out.println("Finished reading header; " + p_buffer);

        return true;
    }

    private AbstractMessage createMessage() throws NetworkException {
        AbstractMessage ret;
        byte type = m_currentHeader.getType();
        byte subtype = m_currentHeader.getSubtype();

        if (type == Messages.NETWORK_MESSAGES_TYPE && subtype == Messages.SUBTYPE_INVALID_MESSAGE) {
            throw new NetworkException("Invalid message type 0, subtype 0, most likely corrupted message/buffer");
        }

        try {
            ret = m_messageDirectory.getInstance(type, subtype);
        } catch (final Exception e) {
            throw new NetworkException("Unable to create message of type " + type + ", subtype " + subtype + ". Type is missing in message directory", e);
        }

        ret.initialize(m_currentHeader);
        ret.setDestination(m_ownNodeID);
        ret.setSource(m_destinationNodeID);
        // System.out.println("Created message: " + ret);

        return ret;
    }

    /**
     * Create a message from a given buffer
     *
     * @param p_buffer
     *         buffer containing a message
     * @return message
     */
    private boolean readPayload(final ByteBuffer p_buffer) throws IOException {
        int readBytes = 0;
        int initialBufferPosition = p_buffer.position();
        int payloadSize = m_currentHeader.getPayloadSize();
        AbstractRequest request;
        AbstractResponse response;
        AbstractMessageImporter importer;

        //System.out.println("Reading payload; " + p_buffer + ", " + payloadSize);

        // hack:
        // to avoid copying data multiple times, some responses use the same objects provided
        // with the request to directly write the data to them instead of creating a temporary
        // object in the response, de-serializing the data and then copying from the temporary object
        // to the object that should receive the data in the first place. (example DXRAM: get request/response)
        // This is only possible, if we have a reference to the original request within the response
        // while reading from the network byte buffer. But in this low level stage, we (usually) don't have
        // access to requests/responses. So we exploit the request map to get our corresponding request
        // before de-serializing the network buffer for every request.
        if (m_currentMessage.isResponse()) {
            response = (AbstractResponse) m_currentMessage;
            request = m_requestMap.getRequest(response);
            if (request == null) {

                System.out.println(
                        "Request null for " + m_currentMessage.getType() + "," + m_currentHeader.getSubtype() + "; " + m_currentMessage.getMessageID() + ", " +
                                m_currentMessage.isResponse() + ", " + m_currentMessage.isExclusive());

                p_buffer.position(Math.max(p_buffer.position() + payloadSize, p_buffer.limit()));
                // Request is not available, probably because of a time-out
                // System.out.println("Aborting reading payload: Request not available");
                m_currentMessage = null;
                m_currentHeader.clear();
                return false;
            }
            response.setCorrespondingRequest(request);
        }

        importer = m_importers.getImporter(p_buffer.remaining() < payloadSize);
        importer.setBuffer(p_buffer.array(), p_buffer.position(), p_buffer.limit());
        try {
            m_currentMessage.readPayload(importer, payloadSize);
        } catch (final ArrayIndexOutOfBoundsException e) {
            // Message is incomplete -> continue later
            p_buffer.position(importer.getPosition());
            m_importers.returnImporter(importer, false);
            // System.out.println("Aborting reading payload: Out of bounds; " + p_buffer);
            return false;
        }
        p_buffer.position(importer.getPosition());
        m_importers.returnImporter(importer, true);
        // System.out.println("Finished reading payload; " + p_buffer + ", " + initialBufferPosition + "; " + m_currentMessage.getPayloadLength());

        // FIXME:
        /*readBytes += p_buffer.position() - initialBufferPosition;
        int calculatedPayloadSize = m_currentMessage.getPayloadLength();
        if (readBytes < calculatedPayloadSize) {
            throw new IOException("Message buffer is too large: " + calculatedPayloadSize + " > " + readBytes + " (payload in bytes)");
        }*/

        return true;
    }
}

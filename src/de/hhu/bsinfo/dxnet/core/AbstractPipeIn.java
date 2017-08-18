package de.hhu.bsinfo.dxnet.core;

import java.util.Arrays;

import de.hhu.bsinfo.dxnet.MessageHandlers;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.utils.NodeID;
import de.hhu.bsinfo.utils.UnsafeMemory;

/**
 * Endpoint for incoming data on a connection.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.06.2017
 */
public abstract class AbstractPipeIn {
    private static final StatisticsOperation SOP_PROCESS = StatisticsRecorderManager.getOperation(AbstractPipeIn.class, "ProcessBuffer");
    private static final StatisticsOperation SOP_DELIVER = StatisticsRecorderManager.getOperation(AbstractPipeIn.class, "DeliverMessage");
    private static final StatisticsOperation SOP_FULFILL = StatisticsRecorderManager.getOperation(AbstractPipeIn.class, "FulfillRequest");

    private final short m_ownNodeID;
    private final short m_destinationNodeID;
    private volatile boolean m_isConnected;
    private final AbstractFlowControl m_flowControl;

    private final MessageHandlers m_messageHandlers;
    private final MessageDirectory m_messageDirectory;
    private final RequestMap m_requestMap;

    private long m_receivedMessages;

    private MessageImporterCollection m_importers;
    private int m_currentPosition;
    private Message m_currentMessage;
    private MessageHeader m_currentHeader = new MessageHeader();

    private final Message[] m_normalMessages = new Message[25];
    private final Message[] m_exclusiveMessages = new Message[25];

    /**
     * Constructor
     *
     * @param p_ownNodeId
     *         Node id of the current instance (receiver)
     * @param p_destinationNodeId
     *         Node id of the destination to receive data from
     * @param p_flowControl
     *         FlowControl instance of the connection
     * @param p_messageDirectory
     *         MessageDirectory instance
     * @param p_requestMap
     *         RequestMap instance
     * @param p_messageHandlers
     *         MessageHandlers instance
     */
    protected AbstractPipeIn(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl,
            final MessageDirectory p_messageDirectory, final RequestMap p_requestMap, final MessageHandlers p_messageHandlers) {
        m_ownNodeID = p_ownNodeId;
        m_destinationNodeID = p_destinationNodeId;
        m_flowControl = p_flowControl;

        m_importers = new MessageImporterCollection();

        m_messageHandlers = p_messageHandlers;
        m_messageDirectory = p_messageDirectory;
        m_requestMap = p_requestMap;
    }

    /**
     * Get the node id of the destination to receive data from
     */
    public short getDestinationNodeID() {
        return m_destinationNodeID;
    }

    /**
     * Check if the pipe is connected to the remote
     */
    public boolean isConnected() {
        return m_isConnected;
    }

    /**
     * Set the pipe connected
     */
    public void setConnected(final boolean p_connected) {
        m_isConnected = p_connected;
    }

    @Override
    public String toString() {
        return "PipeIn[m_ownNodeID " + NodeID.toHexString(m_ownNodeID) + ", m_destinationNodeID " + NodeID.toHexString(m_destinationNodeID) +
                ", m_flowControl " + m_flowControl + ", m_receivedMessages " + m_receivedMessages + ']';
    }

    /**
     * Return a processed buffer (to a buffer pool)
     *
     * @param p_obj
     *         Buffer object to return (implementation dependant)
     * @param p_bufferHandle
     *         Buffer handle to return (implementation dependant)
     */
    public abstract void returnProcessedBuffer(final Object p_obj, final long p_bufferHandle);

    /**
     * Check if the pipe is opened
     */
    public abstract boolean isOpen();

    /**
     * Get the FlowControl instance connected to the pipe
     */
    protected AbstractFlowControl getFlowControl() {
        return m_flowControl;
    }

    /**
     * Get the node id of the current instance
     */
    short getOwnNodeID() {
        return m_ownNodeID;
    }

    /**
     * Process an incoming buffer.
     *
     * @param p_addr
     *         (Unsafe) address to the buffer
     * @param p_bytesAvailable
     *         Number of bytes available for processing
     */
    void processBuffer(final long p_addr, final int p_bytesAvailable) throws NetworkException {
        int bytesRead;
        int counterNormal = 0;
        int counterExclusive = 0;

        // #ifdef STATISTICS
        SOP_PROCESS.enter();
        // #endif /* STATISTICS */

        m_flowControl.dataReceived(p_bytesAvailable);

        m_currentPosition = 0;
        while (p_bytesAvailable > m_currentPosition) {
            if (m_currentMessage == null) {
                bytesRead = readHeader(p_addr, p_bytesAvailable, Message.HEADER_SIZE);

                if (bytesRead < Message.HEADER_SIZE) {
                    // end of current data stream in importer, incomplete header
                    break;
                }
                m_currentMessage = createMessage(p_addr, p_bytesAvailable);
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
            if (m_currentMessage.isResponse()) {
                Response response = (Response) m_currentMessage;
                Request request = m_requestMap.getRequest(response);
                if (request == null) {
                    // System.out.println(
                    //        "Request null for " + m_currentMessage.getType() + ',' + m_currentHeader.getSubtype() + "; " + m_currentMessage.getMessageID() +
                    //                ", " + m_currentMessage.isResponse() + ", " + m_currentMessage.isExclusive());

                    // Request is not available, probably because of a time-out
                    // Skip payload and throw away the (now) useless response
                    m_currentPosition += m_currentHeader.getPayloadSize();
                    m_currentHeader.clear();
                    m_currentMessage = null;
                    continue;
                }

                response.setCorrespondingRequest(request);
            }

            bytesRead = readPayload(p_addr, p_bytesAvailable, m_currentHeader.getPayloadSize());

            if (bytesRead < m_currentHeader.getPayloadSize()) {
                // end of current data stream on importer, incomplete payload
                break;
            }

            if (m_currentMessage.isResponse()) {
                // #ifdef STATISTICS
                SOP_FULFILL.enter();
                // #endif /* STATISTICS */

                m_requestMap.fulfill((Response) m_currentMessage);

                // #ifdef STATISTICS
                SOP_FULFILL.leave();
                // #endif /* STATISTICS */
            } else {
                if (!m_currentMessage.isExclusive()) {
                    m_normalMessages[counterNormal++] = m_currentMessage;
                    if (counterNormal == m_normalMessages.length) {

                        // #ifdef STATISTICS
                        SOP_DELIVER.enter();
                        // #endif /* STATISTICS */

                        m_messageHandlers.newMessages(m_normalMessages);
                        Arrays.fill(m_normalMessages, null);
                        counterNormal = 0;

                        // #ifdef STATISTICS
                        SOP_DELIVER.leave();
                        // #endif /* STATISTICS */

                    }
                } else {
                    m_exclusiveMessages[counterExclusive++] = m_currentMessage;
                    if (counterExclusive == m_exclusiveMessages.length) {

                        // #ifdef STATISTICS
                        SOP_DELIVER.enter();
                        // #endif /* STATISTICS */

                        m_messageHandlers.newMessages(m_exclusiveMessages);
                        Arrays.fill(m_exclusiveMessages, null);
                        counterExclusive = 0;

                        // #ifdef STATISTICS
                        SOP_DELIVER.leave();
                        // #endif /* STATISTICS */

                    }
                }
            }
            m_currentMessage = null;
            m_currentHeader.clear();
            m_receivedMessages++;
        }

        // #ifdef STATISTICS
        SOP_DELIVER.enter();
        // #endif /* STATISTICS */

        if (counterNormal > 0) {
            m_messageHandlers.newMessages(m_normalMessages);
            Arrays.fill(m_normalMessages, 0, counterNormal, null);
        }

        if (counterExclusive > 0) {
            m_messageHandlers.newMessages(m_exclusiveMessages);
            Arrays.fill(m_exclusiveMessages, 0, counterExclusive, null);
        }

        // #ifdef STATISTICS
        SOP_DELIVER.leave();
        // #endif /* STATISTICS */

        // #ifdef STATISTICS
        SOP_PROCESS.leave();
        // #endif /* STATISTICS */
    }

    /**
     * Create a message from a given buffer
     *
     * @param p_addr
     *         (Unsafe) address to buffer
     * @param p_bytesAvailable
     *         Number of bytes available in buffer
     * @return New message instance created from the buffer(s)
     * @throws NetworkException
     *         If creating/reading/deserializing the message failed
     */
    private Message createMessage(final long p_addr, final int p_bytesAvailable) throws NetworkException {
        Message ret;
        byte type = m_currentHeader.getType();
        byte subtype = m_currentHeader.getSubtype();

        if (type == Messages.NETWORK_MESSAGES_TYPE && subtype == Messages.SUBTYPE_INVALID_MESSAGE) {
            StringBuilder builder = new StringBuilder();
            int len = p_bytesAvailable;

            if (len > 1024) {
                len = 1024;
            }

            for (int i = m_currentPosition - 10; i < m_currentPosition - 10 + len; i++) {
                builder.append(Integer.toHexString(UnsafeMemory.readByte(p_addr + i) & 0xFF));
                builder.append(' ');
            }

            throw new NetworkException(
                    "Invalid message type 0, subtype 0, most likely corrupted message/buffer. Buffer section: " + builder + "\nImporterCollectionState:\n" +
                            m_importers);
        }

        try {
            ret = m_messageDirectory.getInstance(type, subtype);
        } catch (final Exception e) {
            StringBuilder builder = new StringBuilder();
            int len = p_bytesAvailable;

            if (len > 1024) {
                len = 1024;
            }

            for (int i = m_currentPosition - 10; i < m_currentPosition - 10 + len; i++) {
                builder.append(Integer.toHexString(UnsafeMemory.readByte(p_addr + i) & 0xFF));
                builder.append(' ');
            }

            throw new NetworkException(
                    "Unable to create message of type " + type + ", subtype " + subtype + ". Type is missing in message directory. Buffer section: " + builder +
                            "\nImporterCollectionState:\n" + m_importers, e);
        }

        ret.initialize(m_currentHeader);
        ret.setDestination(m_ownNodeID);
        ret.setSource(m_destinationNodeID);

        return ret;
    }

    /**
     * Read the header of a message from a buffer
     *
     * @param p_addr
     *         (Unsafe) address of the buffer
     * @param p_bytesAvailable
     *         Number of bytes available in the buffer
     * @param p_bytesToRead
     *         Number of bytes to read from the buffer
     * @return Number of bytes actually read from the buffer
     */
    private int readHeader(final long p_addr, final int p_bytesAvailable, final int p_bytesToRead) {
        AbstractMessageImporter importer = m_importers.getImporter(p_addr, m_currentPosition, p_bytesAvailable, p_bytesToRead);

        try {
            importer.importObject(m_currentHeader);
        } catch (final ArrayIndexOutOfBoundsException e) {
            // Header is incomplete continue later
            m_currentPosition = importer.getPosition();
            return m_importers.returnImporter(importer, false);
        }

        m_currentPosition = importer.getPosition();
        return m_importers.returnImporter(importer, true);
    }

    /**
     * Read the payload of a message from a buffer
     *
     * @param p_addr
     *         (Unsafe) address of a buffer
     * @param p_bytesAvailable
     *         Number of bytes available in the buffer
     * @param p_bytesToRead
     *         Number of bytes to read from the buffer
     * @return Number of bytes actually read from the buffer
     */
    private int readPayload(final long p_addr, final int p_bytesAvailable, final int p_bytesToRead) {
        AbstractMessageImporter importer = m_importers.getImporter(p_addr, m_currentPosition, p_bytesAvailable, p_bytesToRead);

        try {
            m_currentMessage.readPayload(importer, p_bytesToRead);
        } catch (final ArrayIndexOutOfBoundsException e) {
            // Message is incomplete -> continue later
            m_currentPosition = importer.getPosition();
            return m_importers.returnImporter(importer, false);
        }

        int readBytes = importer.getNumberOfReadBytes();
        if (readBytes < p_bytesToRead) {
            throw new NetworkRuntimeException("Message buffer is too large: " + p_bytesToRead + " > " + readBytes + " (payload in bytes), current Message: " +
                    m_currentMessage.getClass().getName() + ", importer type: " + importer.getClass().getSimpleName() + ", importer detail: " + importer +
                    "\nImporterCollectionState:\n" + m_importers);
        }

        m_importers.returnImporter(importer, true);

        m_currentPosition = importer.getPosition();

        return readBytes;
    }
}

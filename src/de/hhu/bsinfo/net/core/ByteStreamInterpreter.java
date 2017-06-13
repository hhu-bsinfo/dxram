package de.hhu.bsinfo.net.core;

import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Creates ByteBuffers containing AbstractMessages from ByteBuffer-Chunks
 *
 * @author Florian Klein 09.03.2012
 * @author Marc Ewert 28.10.2014
 */
final class ByteStreamInterpreter {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ByteStreamInterpreter.class.getSimpleName());

    // TODO: Further evaluate direct byte buffer (in comments)

    /**
     * Represents the steps in the creation process
     */
    public enum Step {
        READ_HEADER, READ_PAYLOAD, DONE
    }

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

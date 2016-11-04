package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a AskAboutSuccessorRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class AskAboutSuccessorResponse extends AbstractResponse {

    // Attributes
    private short m_successor;

    // Constructors

    /**
     * Creates an instance of AskAboutSuccessorResponse
     */
    public AskAboutSuccessorResponse() {
        super();

        m_successor = -1;
    }

    /**
     * Creates an instance of AskAboutSuccessorResponse
     *
     * @param p_request
     *     the corresponding AskAboutSuccessorRequest
     * @param p_predecessor
     *     the predecessor
     */
    public AskAboutSuccessorResponse(final AskAboutSuccessorRequest p_request, final short p_predecessor) {
        super(p_request, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE);

        assert p_predecessor != 0;

        m_successor = p_predecessor;
    }

    // Getters

    /**
     * Get the predecessor
     *
     * @return the NodeID
     */
    public final short getSuccessor() {
        return m_successor;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_successor);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_successor = p_buffer.getShort();
    }

}

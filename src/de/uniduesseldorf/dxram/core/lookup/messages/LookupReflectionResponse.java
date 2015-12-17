package de.uniduesseldorf.dxram.core.lookup.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a CommandRequest
 * @author Florian Klein 05.07.2014
 */
public class LookupReflectionResponse extends AbstractResponse {

	// Attributes
	private String m_answer;

	// Constructors
	/**
	 * Creates an instance of CommpandResponse
	 */
	public LookupReflectionResponse() {
		super();

		m_answer = null;
	}

	/**
	 * Creates an instance of CommandResponse
	 * @param p_request
	 *            the corresponding CommandRequest
	 * @param p_answer
	 *            the answer
	 */
	public LookupReflectionResponse(final LookupReflectionRequest p_request, final String p_answer) {
		super(p_request, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_RESPONSE);

		m_answer = p_answer;
	}

	// Getters
	/**
	 * Get the answer
	 * @return the answer
	 */
	public final String getAnswer() {
		return m_answer;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		byte[] data = m_answer.getBytes();
		
		p_buffer.putInt(data.length);
		p_buffer.put(data);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		byte[] data = new byte[p_buffer.getInt()];
		
		p_buffer.get(data);
		m_answer = new String(data);
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + m_answer.getBytes().length;
	}

}

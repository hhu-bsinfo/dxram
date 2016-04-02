
package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a LogCommandRequest
 * @author Kevin Beineke 10.09.2015
 */
public class LogCommandResponse extends AbstractResponse {

	// Attributes
	private String m_answer;

	// Constructors
	/**
	 * Creates an instance of LogCommandResponse
	 */
	public LogCommandResponse() {
		super();

		m_answer = null;
	}

	/**
	 * Creates an instance of LogCommandResponse
	 * @param p_request
	 *            the corresponding LogCommandRequest
	 * @param p_answer
	 *            the answer
	 */
	public LogCommandResponse(final LogCommandRequest p_request, final String p_answer) {
		super(p_request, LogMessages.SUBTYPE_LOG_COMMAND_RESPONSE);

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
		final byte[] bytes = m_answer.getBytes();

		p_buffer.putInt(bytes.length);
		p_buffer.put(bytes);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		final int size = p_buffer.getInt();
		final byte[] bytes = new byte[size];
		p_buffer.get(bytes);

		m_answer = new String(bytes);
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + m_answer.getBytes().length;
	}

}


package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Request for command
 * @author Kevin Beineke 10.09.2015
 */
public class LogCommandRequest extends AbstractRequest {

	// Attributes
	private String m_cmd;

	// Constructors
	/**
	 * Creates an instance of LogCommandRequest
	 */
	public LogCommandRequest() {
		super();
		m_cmd = null;
	}

	/**
	 * Creates an instance of LogCommandRequest
	 * @param p_destination
	 *            the destination
	 * @param p_cmd
	 *            the command
	 */
	public LogCommandRequest(final short p_destination, final String p_cmd) {
		super(p_destination, LogMessages.TYPE, LogMessages.SUBTYPE_LOG_COMMAND_REQUEST);

		m_cmd = p_cmd;
	}

	/**
	 * Get the command
	 * @return the command
	 */
	public final String getArgument() {
		return m_cmd;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		final byte[] bytes = m_cmd.getBytes();

		p_buffer.putInt(bytes.length);
		p_buffer.put(bytes);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		final int size = p_buffer.getInt();
		final byte[] bytes = new byte[size];
		p_buffer.get(bytes);

		m_cmd = new String(bytes);
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + m_cmd.getBytes().length;
	}

}

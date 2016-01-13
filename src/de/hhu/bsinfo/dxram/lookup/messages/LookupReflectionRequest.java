package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractRequest;
import de.hhu.bsinfo.utils.Contract;

/**
 * Request for command
 * @author Michael Schoettner 20.8.2015
 */
public class LookupReflectionRequest extends AbstractRequest {

	// Attributes
	private String m_cmd;

	// Constructors
	/**
	 * Creates an instance of CommandRequest
	 */
	public LookupReflectionRequest() {
		super();
		m_cmd = null;
	}

	/**
	 * Creates an instance of CommandRequest
	 * @param p_destination
	 *            the destination
	 * @param p_cmd
	 *            the command
	 */
	public LookupReflectionRequest(final short p_destination, final String p_cmd) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_REQUEST);
		Contract.checkNotNull(p_cmd, "error: no argument given");
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
		byte[] data = m_cmd.getBytes();
		
		p_buffer.putInt(data.length);
		p_buffer.put(data);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		byte[] data = new byte[p_buffer.getInt()];
		
		p_buffer.get(data);
		m_cmd = new String(data);
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + m_cmd.getBytes().length;
	}

}

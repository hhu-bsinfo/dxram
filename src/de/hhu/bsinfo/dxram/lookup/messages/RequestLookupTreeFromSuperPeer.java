
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractRequest;

public class RequestLookupTreeFromSuperPeer extends AbstractRequest {

	// Attributes
	private short m_nidToGetTreeFrom;

	// Constructors

	/**
	 * Created because compiler
	 */
	public RequestLookupTreeFromSuperPeer() {
		super();
	}

	/**
	 * Creates an instance of LogMessage
	 */
	public RequestLookupTreeFromSuperPeer(final short p_destination, final short p_nidToGetTreeFrom) {

		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_REQUEST_LOOK_UP_TREE_FROM_SERVER);

		m_nidToGetTreeFrom = p_nidToGetTreeFrom;
	}

	public short getTreeNodeID() {
		return m_nidToGetTreeFrom;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {

		p_buffer.putShort(m_nidToGetTreeFrom);

	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {

		m_nidToGetTreeFrom = p_buffer.getShort();

	}

	@Override
	protected final int getPayloadLength() {

		return Short.BYTES;
	}

}


package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Request for getting the ChunkID to corresponding id on a remote node
 * @author Florian Klein
 *         09.03.2012
 */
public class GetChunkIDForNameserviceEntryRequest extends AbstractRequest {

	// Attributes
	private int m_id;

	// Constructors
	/**
	 * Creates an instance of GetChunkIDRequest
	 */
	public GetChunkIDForNameserviceEntryRequest() {
		super();

		m_id = -1;
	}

	/**
	 * Creates an instance of GetChunkIDRequest
	 * @param p_destination
	 *            the destination
	 * @param p_id
	 *            the id
	 */
	public GetChunkIDForNameserviceEntryRequest(final short p_destination, final int p_id) {
		super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
				LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST);

		m_id = p_id;
	}

	// Getters
	/**
	 * Get the id to store
	 * @return the id to store
	 */
	public final int getID() {
		return m_id;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_id);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_id = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES;
	}

}

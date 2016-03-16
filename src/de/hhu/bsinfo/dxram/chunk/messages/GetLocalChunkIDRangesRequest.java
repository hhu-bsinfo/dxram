package de.hhu.bsinfo.dxram.chunk.messages;

import de.hhu.bsinfo.menet.AbstractRequest;

public class GetLocalChunkIDRangesRequest extends AbstractRequest {
	/**
	 * Creates an instance of GetLocalChunkIDRangesRequest.
	 * This constructor is used when receiving this message.
	 */
	public GetLocalChunkIDRangesRequest() {
		super();
	}

	/**
	 * Creates an instance of GetLocalChunkIDRangesRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public GetLocalChunkIDRangesRequest(final short p_destination) {
		super(p_destination, ChunkMessages.TYPE, ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST);
	}
}

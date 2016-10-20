
package de.hhu.bsinfo.dxram.chunk.messages;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Request for getting the chunk id ranges of migrated locally stored chunk ids from another node.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class GetMigratedChunkIDRangesRequest extends AbstractRequest {
	/**
	 * Creates an instance of GetMigratedChunkIDRangesRequest.
	 * This constructor is used when receiving this message.
	 */
	public GetMigratedChunkIDRangesRequest() {
		super();
	}

	/**
	 * Creates an instance of GetMigratedChunkIDRangesRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public GetMigratedChunkIDRangesRequest(final short p_destination) {
		super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE,
				ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_REQUEST);
	}
}

package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Created by nothaas on 6/10/16.
 */
public class PingMessage extends AbstractMessage {
	/**
	 * Creates an instance of VerticesForNextFrontierRequest.
	 * This constructor is used when receiving this message.
	 */
	public PingMessage() {
		super();
	}

	/**
	 * Creates an instance of VerticesForNextFrontierRequest
	 *
	 * @param p_destination the destination
	 */
	public PingMessage(final short p_destination) {
		super(p_destination, BFSMessages.TYPE, BFSMessages.SUBTYPE_PING_MESSAGE);

	}
}

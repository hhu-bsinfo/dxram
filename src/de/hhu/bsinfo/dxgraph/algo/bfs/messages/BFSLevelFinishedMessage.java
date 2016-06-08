package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Created by nothaas on 6/2/16.
 */
public class BFSLevelFinishedMessage extends AbstractMessage {
	/**
	 * Creates an instance of BFSLevelFinishedMessage.
	 * This constructor is used when receiving this message.
	 */
	public BFSLevelFinishedMessage() {
		super();
	}

	/**
	 * Creates an instance of VerticesForNextFrontierRequest
	 *
	 * @param p_destination the destination
	 */
	public BFSLevelFinishedMessage(final short p_destination) {
		super(p_destination, BFSMessages.TYPE, BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE);
	}
}

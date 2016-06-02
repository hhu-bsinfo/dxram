package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Created by nothaas on 6/2/16.
 */
public class BFSTerminateMessage extends AbstractMessage {
	/**
	 * Creates an instance of BFSTerminateMessage.
	 * This constructor is used when receiving this message.
	 */
	public BFSTerminateMessage() {
		super();
	}

	/**
	 * Creates an instance of BFSTerminateMessage
	 *
	 * @param p_destination the destination
	 */
	public BFSTerminateMessage(final short p_destination, final boolean p_nextFrontierEmpty) {
		super(p_destination, BFSMessages.TYPE, BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE);

		setStatusCode((byte) (p_nextFrontierEmpty ? 1 : 0));
	}

	public boolean isNextFrontierEmpty() {
		return getStatusCode() == 1;
	}
}

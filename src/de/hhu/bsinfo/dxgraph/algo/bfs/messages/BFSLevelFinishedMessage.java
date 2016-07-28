package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message broadcasted by one bfs peer to all other participating peers when
 * the current peer has finished his iteration.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 19.05.16
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
	 * Creates an instance of BFSLevelFinishedMessage
	 *
	 * @param p_destination the destination
	 */
	public BFSLevelFinishedMessage(final short p_destination) {
		super(p_destination, BFSMessages.TYPE, BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE);
	}
}

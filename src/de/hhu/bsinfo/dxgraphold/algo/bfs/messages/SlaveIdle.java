package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

public class SlaveIdle extends AbstractMessage {

	/**
	 * Creates an instance of SalvesRunningWithWork.
	 * This constructor is used when receiving this message.
	 */
	public SlaveIdle() {
		super();
	}

	/**
	 * Creates an instance of SalvesRunningWithWork
	 * @param p_destination
	 *            the destination
	 */
	public SlaveIdle(final short p_destination) {
		super(p_destination, BFSMessages.TYPE, BFSMessages.SUBTYPE_SLAVE_IDLE);

	}
}

package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

public class SlaveRunning extends AbstractMessage {

	/**
	 * Creates an instance of SalvesRunningWithWork.
	 * This constructor is used when receiving this message.
	 */
	public SlaveRunning() {
		super();
	}

	/**
	 * Creates an instance of SalvesRunningWithWork
	 * @param p_destination
	 *            the destination
	 */
	public SlaveRunning(final short p_destination) {
		super(p_destination, BFSMessages.TYPE, BFSMessages.SUBTYPE_SLAVE_RUNNING);

	}
}

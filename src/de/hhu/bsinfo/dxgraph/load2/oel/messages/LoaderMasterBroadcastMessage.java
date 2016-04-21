package de.hhu.bsinfo.dxgraph.load2.oel.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * This message is sent/broadcasted to all peers in intervals
 * when waiting for the slaves to sign on. That way the slaves
 * get to know the master and can send the sign on message.
 * 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 10.02.16
 */
public class LoaderMasterBroadcastMessage extends AbstractMessage {
	/**
	 * Creates an instance of LoaderMasterBroadcastMessage.
	 * This constructor is used when receiving this message.
	 */
	public LoaderMasterBroadcastMessage() {
		super();
	}

	/**
	 * Creates an instance of LoaderMasterBroadcastMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public LoaderMasterBroadcastMessage(final short p_destination) {
		super(p_destination, GraphLoaderOrderedEdgeListMessages.TYPE, GraphLoaderOrderedEdgeListMessages.SUB_TYPE_MASTER_BROADCAST);
	}
}


package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Ping Superpeer Message
 * @author Kevin Beineke
 *         06.09.2012
 */
public class PingSuperpeerMessage extends AbstractMessage {

	// Constructors
	/**
	 * Creates an instance of PingSuperpeerMessage
	 */
	public PingSuperpeerMessage() {
		super();
	}

	/**
	 * Creates an instance of PingSuperpeerMessage
	 * @param p_destination
	 *            the destination
	 */
	public PingSuperpeerMessage(final short p_destination) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE);
	}

}

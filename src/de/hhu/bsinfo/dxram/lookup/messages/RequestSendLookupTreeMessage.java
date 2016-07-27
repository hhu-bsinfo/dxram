
package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

public class RequestSendLookupTreeMessage extends AbstractMessage {

	// Constructors

	/**
	 * Created because compiler
	 */
	public RequestSendLookupTreeMessage() {
		super();
	}

	/**
	 * Creates an instance of LogMessage
	 */
	public RequestSendLookupTreeMessage(final short p_destination) {

		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_REQUEST_SEND_LOOK_UP_TREE);

	}

}

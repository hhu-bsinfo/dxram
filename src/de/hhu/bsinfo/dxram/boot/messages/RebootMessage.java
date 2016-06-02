
package de.hhu.bsinfo.dxram.boot.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message to trigger a soft reboot of DXRAM
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 02.05.16
 */
public class RebootMessage extends AbstractMessage {
	/**
	 * Creates an instance of RebootMessage.
	 * This constructor is used when receiving this message.
	 */
	public RebootMessage() {
		super();
	}

	/**
	 * Creates an instance of RebootMessage
	 * @param p_destination
	 *            the destination
	 */
	public RebootMessage(final short p_destination) {
		super(p_destination, BootMessages.TYPE, BootMessages.SUBTYPE_REBOOT_MESSAGE);
	}
}

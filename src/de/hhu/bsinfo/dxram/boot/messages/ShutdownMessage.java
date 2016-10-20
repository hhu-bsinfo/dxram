
package de.hhu.bsinfo.dxram.boot.messages;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message to trigger a soft shutdown of DXRAM
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 02.05.16
 */
public class ShutdownMessage extends AbstractMessage {
	/**
	 * Creates an instance of ShutdownMessage.
	 * This constructor is used when receiving this message.
	 */
	public ShutdownMessage() {
		super();
	}

	/**
	 * Creates an instance of ShutdownMessage
	 * @param p_destination
	 *            the destination
	 * @param p_hardShutdown
	 *            True if the whole application running DXRAM has to exit, false for DXRAM only shutdown
	 */
	public ShutdownMessage(final short p_destination, final boolean p_hardShutdown) {
		super(p_destination, DXRAMMessageTypes.BOOT_MESSAGES_TYPE, BootMessages.SUBTYPE_SHUTDOWN_MESSAGE);

		setStatusCode((byte) (p_hardShutdown ? 1 : 0));
	}

	/**
	 * Check if the shutdown is a hard shutdown (full application).
	 * @return True for hard shutdown, false for soft (DXRAM only) shutdown.
	 */
	public boolean isHardShutdown() {
		return getStatusCode() > 0 ? true : false;
	}
}

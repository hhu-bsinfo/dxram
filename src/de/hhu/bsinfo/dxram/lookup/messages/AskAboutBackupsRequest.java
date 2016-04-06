
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Ask About Backups Request
 * @author Kevin Beineke
 *         06.09.2012
 */
public class AskAboutBackupsRequest extends AbstractRequest {

	// Attributes
	private ArrayList<Short> m_peers;

	// Constructors
	/**
	 * Creates an instance of AskAboutBackupsRequest
	 */
	public AskAboutBackupsRequest() {
		super();

		m_peers = null;
	}

	/**
	 * Creates an instance of AskAboutBackupsRequest
	 * @param p_destination
	 *            the destination
	 * @param p_peers
	 *            all peers for which this superpeer stores backups
	 */
	public AskAboutBackupsRequest(final short p_destination, final ArrayList<Short> p_peers) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST);

		m_peers = p_peers;
	}

	// Getters
	/**
	 * Get the peers for which the superpeer stores backups
	 * @return the peers
	 */
	public final ArrayList<Short> getPeers() {
		return m_peers;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		if (m_peers == null || m_peers.size() == 0) {
			p_buffer.putInt(0);
		} else {
			p_buffer.putInt(m_peers.size());
			for (short peer : m_peers) {
				p_buffer.putShort(peer);
			}
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int length;

		m_peers = new ArrayList<Short>();
		length = p_buffer.getInt();
		for (int i = 0; i < length; i++) {
			m_peers.add(p_buffer.getShort());
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		int ret;

		ret = Integer.BYTES;
		if (m_peers != null && m_peers.size() > 0) {
			ret += Short.BYTES * m_peers.size();
		}

		return ret;
	}

}

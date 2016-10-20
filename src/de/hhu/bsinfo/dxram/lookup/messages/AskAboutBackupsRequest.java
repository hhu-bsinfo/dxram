
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Ask About Backups Request
 * @author Kevin Beineke
 *         06.09.2012
 */
public class AskAboutBackupsRequest extends AbstractRequest {

	// Attributes
	private ArrayList<Short> m_peers;
	private int m_numberOfNameserviceEntries;
	private int m_numberOfStorages;
	private int m_numberOfBarriers;

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
	 * @param p_numberOfNameserviceEntries
	 *            the number of expected nameservice entries
	 * @param p_numberOfStorages
	 *            the number of expected storages
	 * @param p_numberOfBarriers
	 *            the number of expected barriers
	 */
	public AskAboutBackupsRequest(final short p_destination, final ArrayList<Short> p_peers,
			final int p_numberOfNameserviceEntries,
			final int p_numberOfStorages, final int p_numberOfBarriers) {
		super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST);

		m_peers = p_peers;
		m_numberOfNameserviceEntries = p_numberOfNameserviceEntries;
		m_numberOfStorages = p_numberOfStorages;
		m_numberOfBarriers = p_numberOfBarriers;
	}

	// Getters
	/**
	 * Get the peers for which the superpeer stores backups
	 * @return the peers
	 */
	public final ArrayList<Short> getPeers() {
		return m_peers;
	}

	/**
	 * Get the expected number of nameservice entries
	 * @return the peers
	 */
	public final int getNumberOfNameserviceEntries() {
		return m_numberOfNameserviceEntries;
	}

	/**
	 * Get the expected number of storages
	 * @return the peers
	 */
	public final int getNumberOfStorages() {
		return m_numberOfStorages;
	}

	/**
	 * Get the expected number of barriers
	 * @return the peers
	 */
	public final int getNumberOfBarriers() {
		return m_numberOfBarriers;
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
		p_buffer.putInt(m_numberOfNameserviceEntries);
		p_buffer.putInt(m_numberOfStorages);
		p_buffer.putInt(m_numberOfBarriers);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int length;

		m_peers = new ArrayList<Short>();
		length = p_buffer.getInt();
		for (int i = 0; i < length; i++) {
			m_peers.add(p_buffer.getShort());
		}
		m_numberOfNameserviceEntries = p_buffer.getInt();
		m_numberOfStorages = p_buffer.getInt();
		m_numberOfBarriers = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		int ret;

		ret = Integer.BYTES;
		if (m_peers != null && m_peers.size() > 0) {
			ret += Short.BYTES * m_peers.size();
		}
		ret += 3 * Integer.BYTES;

		return ret;
	}

}

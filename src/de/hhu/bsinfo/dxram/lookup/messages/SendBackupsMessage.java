
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.lookup.overlay.LookupTree;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Send Backups Message
 * @author Kevin Beineke
 *         06.09.2012
 */
public class SendBackupsMessage extends AbstractMessage {

	// Attributes
	private ArrayList<LookupTree> m_trees;
	private byte[] m_mappings;

	// Constructors
	/**
	 * Creates an instance of SendBackupsMessage
	 */
	public SendBackupsMessage() {
		super();

		m_trees = null;
		m_mappings = null;
	}

	/**
	 * Creates an instance of SendBackupsMessage
	 * @param p_destination
	 *            the destination
	 * @param p_mappings
	 *            the id mappings
	 * @param p_trees
	 *            the CIDTrees
	 */
	public SendBackupsMessage(final short p_destination, final byte[] p_mappings, final ArrayList<LookupTree> p_trees) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE);

		m_mappings = p_mappings;
		m_trees = p_trees;
	}

	// Getters
	/**
	 * Get CIDTrees
	 * @return the CIDTrees
	 */
	public final ArrayList<LookupTree> getCIDTrees() {
		return m_trees;
	}

	/**
	 * Get id mappings
	 * @return the byte array
	 */
	public final byte[] getMappings() {
		return m_mappings;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		if (m_mappings == null || m_mappings.length == 0) {
			p_buffer.putInt(0);
		} else {
			p_buffer.putInt(m_mappings.length);
			p_buffer.put(m_mappings);
		}

		if (m_trees == null) {
			p_buffer.putInt(0);
		} else {
			p_buffer.putInt(m_trees.size());
			for (LookupTree tree : m_trees) {
				LookupTree.writeCIDTree(p_buffer, tree);
			}
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int length;

		length = p_buffer.getInt();
		if (length != 0) {
			m_mappings = new byte[length];
			p_buffer.get(m_mappings);
		}

		m_trees = new ArrayList<LookupTree>(p_buffer.getInt());
		for (int i = 0; i < m_trees.size(); i++) {
			m_trees.add(LookupTree.readCIDTree(p_buffer));
		}
	}

	@Override
	protected final int getPayloadLength() {
		int ret;

		ret = Integer.BYTES;
		if (m_mappings != null && m_mappings.length > 0) {
			ret += m_mappings.length;
		}

		ret += Integer.BYTES;
		if (m_trees != null && m_trees.size() > 0) {
			for (LookupTree tree : m_trees) {
				ret += LookupTree.getCIDTreeWriteLength(tree);
			}
		}

		return ret;
	}

}

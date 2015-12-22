package de.uniduesseldorf.dxram.core.lookup.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.uniduesseldorf.dxram.core.lookup.storage.LookupTree;
import de.uniduesseldorf.menet.AbstractMessage;

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
	public SendBackupsMessage(final short p_source, final short p_destination, final byte[] p_mappings, final ArrayList<LookupTree> p_trees) {
		super(p_source, p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE);

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
		if (m_mappings == null) {
			p_buffer.put((byte) 0); 
		} else {
			p_buffer.put((byte) 1);
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
		if (p_buffer.get() != 0) {
			m_mappings = new byte[p_buffer.getInt()];
			p_buffer.get(m_mappings);
		}

		m_trees = new ArrayList<LookupTree>(p_buffer.getInt());
		for (int i = 0; i < m_trees.size(); i++) {
			m_trees.add(LookupTree.readCIDTree(p_buffer));
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		int ret;

		ret = Byte.BYTES;
		if (m_mappings != null) {
			ret += Integer.BYTES + m_mappings.length;
		}

		ret += Integer.BYTES;
		if (m_trees != null) {
			for (LookupTree tree : m_trees) {
				ret += LookupTree.getCIDTreeWriteLength(tree);
			}
		}

		return ret;
	}

}

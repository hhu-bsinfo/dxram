
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.lookup.overlay.LookupTree;
import de.hhu.bsinfo.menet.AbstractMessage;

public class SendLookupTreeMessage extends AbstractMessage {

	// Attributes
	private ArrayList<LookupTree> m_trees;

	// Constructors
	/**
	 * Creates an instance of SendBackupsMessage
	 */
	public SendLookupTreeMessage() {
		super();

		m_trees = null;
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
	public SendLookupTreeMessage(final short p_destination, final ArrayList<LookupTree> p_trees) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_LOOK_UP_TREE);

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

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {

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

		m_trees = new ArrayList<LookupTree>(p_buffer.getInt());
		for (int i = 0; i < m_trees.size(); i++) {
			m_trees.add(LookupTree.readCIDTree(p_buffer));
		}
	}

	@Override
	protected final int getPayloadLength() {
		int ret = 0;

		ret += Integer.BYTES;
		if (m_trees != null && m_trees.size() > 0) {
			for (LookupTree tree : m_trees) {
				ret += LookupTree.getCIDTreeWriteLength(tree);
			}
		}

		return ret;
	}

}

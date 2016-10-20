
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.lookup.overlay.storage.LookupTree;
import de.hhu.bsinfo.ethnet.AbstractResponse;

public class GetLookupTreeResponse extends AbstractResponse {

	// Attributes
	LookupTree m_tree;

	// Constructors
	/**
	 * Creates an instance of SendBackupsMessage
	 */
	public GetLookupTreeResponse() {
		super();

		m_tree = null;
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
	public GetLookupTreeResponse(final GetLookupTreeRequest p_request, final LookupTree p_trees) {
		super(p_request, LookupMessages.SUBTYPE_GET_LOOKUP_TREE_RESPONSE);

		m_tree = p_trees;
	}

	// Getters
	/**
	 * Get CIDTrees
	 * @return the CIDTrees
	 */
	public final LookupTree getCIDTree() {
		return m_tree;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {

		LookupTree.writeLookupTree(p_buffer, m_tree);

	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {

		// m_trees = new ArrayList<LookupTree>(p_buffer.getInt());
		// for (int i = 0; i < m_trees.size(); i++) {
		// m_trees.add(LookupTree.readCIDTree(p_buffer));
		// }

		m_tree = LookupTree.readLookupTree(p_buffer);
	}

	@Override
	protected final int getPayloadLength() {
		int ret;

		// ret += Integer.BYTES;
		// if (m_trees != null && m_trees.size() > 0) {
		// for (LookupTree tree : m_trees) {
		// ret += LookupTree.getCIDTreeWriteLength(tree);
		// }
		// }
		ret = LookupTree.getLookupTreeWriteLength(m_tree);

		return ret;
	}

}

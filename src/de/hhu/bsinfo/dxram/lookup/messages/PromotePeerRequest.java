package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.lookup.storage.LookupTree;
import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Promote Peer Request
 * @author Kevin Beineke
 *         06.09.2012
 */
public class PromotePeerRequest extends AbstractRequest {

	// Attributes
	private short m_predecessor;
	private short m_successor;
	private short m_replacement;
	private byte[] m_mappings;
	private ArrayList<Short> m_superpeers;
	private ArrayList<Short> m_peers;
	private ArrayList<LookupTree> m_trees;

	// Constructors
	/**
	 * Creates an instance of PromotePeerRequest
	 */
	public PromotePeerRequest() {
		super();

		m_predecessor = -1;
		m_successor = -1;
		m_replacement = -1;
		m_mappings = null;
		m_superpeers = null;
		m_peers = null;
		m_trees = null;
	}

	/**
	 * Creates an instance of PromotePeerRequest
	 * @param p_destination
	 *            the destination
	 * @param p_predecessor
	 *            the predecessor
	 * @param p_successor
	 *            the successor
	 * @param p_replacement
	 *            the peer that should store p_destination's chunks
	 * @param p_mappings
	 *            the id mappings
	 * @param p_superpeers
	 *            the finger superpeers
	 * @param p_peers
	 *            the peers the superpeer is responsible for
	 * @param p_trees
	 *            the CIDTrees of the peers
	 */
	public PromotePeerRequest(final short p_destination, final short p_predecessor, final short p_successor, final short p_replacement,
			final byte[] p_mappings, final ArrayList<Short> p_superpeers, final ArrayList<Short> p_peers, final ArrayList<LookupTree> p_trees) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_PROMOTE_PEER_REQUEST);

		m_predecessor = p_predecessor;
		m_successor = p_successor;
		m_superpeers = p_superpeers;
		m_mappings = p_mappings;
		m_replacement = p_replacement;
		m_peers = p_peers;
		m_trees = p_trees;
	}

	// Getters
	/**
	 * Get predecessor
	 * @return the NodeID
	 */
	public final short getPredecessor() {
		return m_predecessor;
	}

	/**
	 * Get successor
	 * @return the NodeID
	 */
	public final short getSuccessor() {
		return m_successor;
	}

	/**
	 * Get replacement
	 * @return the NodeID
	 */
	public final short getReplacement() {
		return m_replacement;
	}

	/**
	 * Get id mappings
	 * @return the byte array
	 */
	public final byte[] getMappings() {
		return m_mappings;
	}

	/**
	 * Get superpeers
	 * @return the NodeIDs
	 */
	public final ArrayList<Short> getSuperpeers() {
		return m_superpeers;
	}

	/**
	 * Get peers
	 * @return the NodeIDs
	 */
	public final ArrayList<Short> getPeers() {
		return m_peers;
	}

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
		p_buffer.putShort(m_predecessor);
		p_buffer.putShort(m_successor);
		p_buffer.putShort(m_replacement);

		if (m_mappings == null) {
			p_buffer.put((byte) 0);
		} else {
			p_buffer.put((byte) 1);
			p_buffer.putInt(m_mappings.length);
			p_buffer.put(m_mappings);
		}

		p_buffer.putInt(m_superpeers.size());
		for (short superpeer : m_superpeers) {
			p_buffer.putShort(superpeer);
		}

		if (m_peers == null) {
			p_buffer.putInt(0);
		} else {
			p_buffer.putInt(m_peers.size());
			for (short peer : m_peers) {
				p_buffer.putShort(peer);
			}
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
		m_predecessor = p_buffer.getShort();
		m_successor = p_buffer.getShort();
		m_replacement = p_buffer.getShort();

		if (p_buffer.get() != 0) {
			m_mappings = new byte[p_buffer.getInt()];
			p_buffer.get(m_mappings);
		}

		m_superpeers = new ArrayList<Short>(p_buffer.getInt());
		for (int i = 0; i < m_superpeers.size(); i++) {
			m_superpeers.add(p_buffer.getShort());
		}

		m_peers = new ArrayList<Short>(p_buffer.getInt());
		for (int i = 0; i < m_peers.size(); i++) {
			m_peers.add(p_buffer.getShort());
		}

		m_trees = new ArrayList<LookupTree>(p_buffer.getInt());
		for (int i = 0; i < m_trees.size(); i++) {
			m_trees.add(LookupTree.readCIDTree(p_buffer));
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		int ret;

		ret = Short.BYTES * 3;

		ret += Byte.BYTES;
		if (m_mappings != null) {
			ret += Integer.BYTES + m_mappings.length;
		}

		ret += Integer.BYTES + Short.BYTES * m_superpeers.size();

		ret += Integer.BYTES;
		if (m_peers != null) {
			ret += Integer.BYTES + Short.BYTES * m_peers.size();
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


package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.lookup.overlay.LookupTree;
import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a JoinRequest
 * @author Kevin Beineke
 *         06.09.2012
 */
public class JoinResponse extends AbstractResponse {

	// Attributes
	private short m_newContactSuperpeer;
	private short m_predecessor;
	private short m_successor;
	private byte[] m_mappings;
	private ArrayList<Short> m_superpeers;
	private ArrayList<Short> m_peers;
	private ArrayList<LookupTree> m_trees;

	// Constructors
	/**
	 * Creates an instance of JoinResponse
	 */
	public JoinResponse() {
		super();

		m_newContactSuperpeer = -1;
		m_predecessor = -1;
		m_successor = -1;
		m_mappings = null;
		m_superpeers = null;
		m_peers = null;
		m_trees = null;
	}

	/**
	 * Creates an instance of JoinResponse
	 * @param p_request
	 *            the corresponding JoinRequest
	 * @param p_newContactSuperpeer
	 *            the superpeer that has to be asked next
	 * @param p_predecessor
	 *            the predecessor
	 * @param p_successor
	 *            the successor
	 * @param p_mappings
	 *            the id mappings
	 * @param p_superpeers
	 *            the finger superpeers
	 * @param p_peers
	 *            the peers the superpeer is responsible for
	 * @param p_trees
	 *            the CIDTrees of the peers
	 */
	public JoinResponse(final JoinRequest p_request, final short p_newContactSuperpeer, final short p_predecessor, final short p_successor,
			final byte[] p_mappings, final ArrayList<Short> p_superpeers, final ArrayList<Short> p_peers, final ArrayList<LookupTree> p_trees) {
		super(p_request, LookupMessages.SUBTYPE_JOIN_RESPONSE);

		m_newContactSuperpeer = p_newContactSuperpeer;
		m_predecessor = p_predecessor;
		m_successor = p_successor;
		m_superpeers = p_superpeers;
		m_mappings = p_mappings;
		m_peers = p_peers;
		m_trees = p_trees;
	}

	// Getters
	/**
	 * Get new contact superpeer
	 * @return the NodeID
	 */
	public final short getNewContactSuperpeer() {
		return m_newContactSuperpeer;
	}

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
	 * Get mappings
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
		if (m_newContactSuperpeer == -1) {
			p_buffer.put((byte) 1);
			p_buffer.putShort(m_predecessor);
			p_buffer.putShort(m_successor);

			if (m_mappings == null || m_mappings.length == 0) {
				p_buffer.putInt(0);
			} else {
				p_buffer.putInt(m_mappings.length);
				p_buffer.put(m_mappings);
			}

			if (m_superpeers == null || m_superpeers.size() == 0) {
				p_buffer.putInt(0);
			} else {
				p_buffer.putInt(m_superpeers.size());
				for (short superpeer : m_superpeers) {
					p_buffer.putShort(superpeer);
				}
			}

			if (m_peers == null || m_peers.size() == 0) {
				p_buffer.putInt(0);
			} else {
				p_buffer.putInt(m_peers.size());
				for (short peer : m_peers) {
					p_buffer.putShort(peer);
				}
			}

			if (m_trees == null || m_trees.size() == 0) {
				p_buffer.putInt(0);
			} else {
				p_buffer.putInt(m_trees.size());
				for (LookupTree tree : m_trees) {
					LookupTree.writeCIDTree(p_buffer, tree);
				}
			}
		} else {
			p_buffer.put((byte) 0);
			p_buffer.putShort(m_newContactSuperpeer);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int length;

		if (p_buffer.get() != 0) {
			m_predecessor = p_buffer.getShort();
			m_successor = p_buffer.getShort();

			length = p_buffer.getInt();
			if (length != 0) {
				m_mappings = new byte[length];
				p_buffer.get(m_mappings);
			}

			m_superpeers = new ArrayList<Short>();
			length = p_buffer.getInt();
			for (int i = 0; i < length; i++) {
				m_superpeers.add(p_buffer.getShort());
			}

			m_peers = new ArrayList<Short>();
			length = p_buffer.getInt();
			for (int i = 0; i < length; i++) {
				m_peers.add(p_buffer.getShort());
			}

			m_trees = new ArrayList<LookupTree>();
			length = p_buffer.getInt();
			for (int i = 0; i < length; i++) {
				m_trees.add(LookupTree.readCIDTree(p_buffer));
			}
		} else {
			m_newContactSuperpeer = p_buffer.getShort();
		}
	}

	@Override
	protected final int getPayloadLength() {
		int ret;

		if (m_newContactSuperpeer == -1) {
			ret = Byte.BYTES + Short.BYTES * 2;

			ret += Integer.BYTES;
			if (m_mappings != null && m_mappings.length > 0) {
				ret += m_mappings.length;
			}

			ret += Integer.BYTES;
			if (m_superpeers != null && m_superpeers.size() > 0) {
				ret += Short.BYTES * m_superpeers.size();
			}

			ret += Integer.BYTES;
			if (m_peers != null && m_peers.size() > 0) {
				ret += Short.BYTES * m_peers.size();
			}

			ret += Integer.BYTES;
			if (m_trees != null && m_trees.size() > 0) {
				for (LookupTree tree : m_trees) {
					ret += LookupTree.getCIDTreeWriteLength(tree);
				}
			}
		} else {
			ret = Byte.BYTES + Short.BYTES;
		}

		return ret;
	}

}

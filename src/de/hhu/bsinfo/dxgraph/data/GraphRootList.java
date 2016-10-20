
package de.hhu.bsinfo.dxgraph.data;

import java.util.Arrays;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * List of root vertex ids used as entry points for various graph algorithms.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class GraphRootList implements DataStructure {
	private long m_id = ChunkID.INVALID_ID;
	private long[] m_roots = new long[0];

	/**
	 * Constructor
	 */
	public GraphRootList() {

	}

	/**
	 * Constructor
	 *
	 * @param p_id Chunk id to assign.
	 */
	public GraphRootList(final long p_id) {
		m_id = p_id;
	}

	/**
	 * Constructor
	 *
	 * @param p_id    Chunk id to assign.
	 * @param p_roots Initial root list to assign,
	 */
	public GraphRootList(final long p_id, final long[] p_roots) {
		m_id = p_id;
		m_roots = p_roots;
	}

	/**
	 * Constructor
	 *
	 * @param p_id       Chunk id to assign.
	 * @param p_numRoots Pre-allocate space for a number of roots.
	 */
	public GraphRootList(final long p_id, final int p_numRoots) {
		m_id = p_id;
		m_roots = new long[p_numRoots];
	}

	/**
	 * Get the list of roots.
	 *
	 * @return List of roots.
	 */
	public long[] getRoots() {
		return m_roots;
	}

	/**
	 * Resize the static allocated root list.
	 *
	 * @param p_count Number of roots to resize the list to.
	 */
	public void setRootCount(final int p_count) {
		if (p_count != m_roots.length) {
			// grow or shrink array
			m_roots = Arrays.copyOf(m_roots, p_count);
		}
	}

	// -----------------------------------------------------------------------------

	@Override
	public long getID() {
		return m_id;
	}

	@Override
	public void setID(final long p_id) {
		m_id = p_id;
	}

	@Override
	public void importObject(final Importer p_importer) {
		int numRoots;

		numRoots = p_importer.readInt();
		m_roots = new long[numRoots];
		p_importer.readLongs(m_roots);
	}

	@Override
	public int sizeofObject() {
		return Integer.BYTES
				+ Long.BYTES * m_roots.length;
	}

	@Override
	public void exportObject(final Exporter p_exporter) {

		p_exporter.writeInt(m_roots.length);
		p_exporter.writeLongs(m_roots);
	}

	@Override
	public String toString() {
		String str = "GraphRootList[m_id " + Long.toHexString(m_id) + ", numRoots "
				+ m_roots.length + "]: ";
		int counter = 0;
		for (Long v : m_roots) {
			str += Long.toHexString(v) + ", ";
			counter++;
			// avoid long strings
			if (counter > 9) {
				break;
			}
		}

		return str;
	}
}

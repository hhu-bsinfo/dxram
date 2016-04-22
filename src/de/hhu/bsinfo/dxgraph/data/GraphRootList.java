
package de.hhu.bsinfo.dxgraph.data;

import java.util.Arrays;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

public class GraphRootList implements DataStructure {
	private long m_id = ChunkID.INVALID_ID;
	private long[] m_roots = new long[0];

	public GraphRootList() {

	}

	public GraphRootList(final long p_id) {
		m_id = p_id;
	}

	public GraphRootList(final long p_id, final long[] p_roots) {
		m_id = p_id;
		m_roots = p_roots;
	}

	public GraphRootList(final long p_id, final int p_numRoots) {
		m_id = p_id;
		m_roots = new long[p_numRoots];
	}

	public long[] getRoots() {
		return m_roots;
	}

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
	public int importObject(final Importer p_importer, final int p_size) {
		int numRoots;

		numRoots = p_importer.readInt();
		m_roots = new long[numRoots];
		p_importer.readLongs(m_roots);

		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return Integer.BYTES
				+ Long.BYTES * m_roots.length;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return true;
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {

		p_exporter.writeInt(m_roots.length);
		p_exporter.writeLongs(m_roots);

		return sizeofObject();
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

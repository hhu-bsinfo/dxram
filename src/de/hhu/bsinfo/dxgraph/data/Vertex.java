
package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

import java.util.Arrays;

/**
 * Object representation of a vertex with a static list of neighbours.
 * The number of neighbours is limited to roughly 2 million entries
 * due to max chunk size being 16MB in DXRAM.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class Vertex implements DataStructure {

	private long m_id = ChunkID.INVALID_ID;
	private boolean m_flagWriteUserdataOnly;

	private int m_userData = -1;
	private long[] m_neighbours = new long[0];

	/**
	 * Constructor
	 */
	public Vertex() {

	}

	/**
	 * Constructor
	 *
	 * @param p_id Chunk id to assign.
	 */
	public Vertex(final long p_id) {
		m_id = p_id;
	}

	/**
	 * Get user data from the vertex.
	 *
	 * @return User data.
	 */
	public int getUserData() {
		return m_userData;
	}

	/**
	 * Set user data for the vertex.
	 *
	 * @param p_userData User data to set.
	 */
	public void setUserData(final int p_userData) {
		m_userData = p_userData;
	}

	/**
	 * Flag this vertex to write the userdata item only on
	 * the next serilization call (performance hack).
	 *
	 * @param p_flag True to write the userdata only, false for whole vertex on next serialization call.
	 */
	public void setWriteUserDataOnly(final boolean p_flag) {
		m_flagWriteUserdataOnly = p_flag;
	}

	/**
	 * Add a new neighbour to the currently existing list.
	 * This will expand the static array by one entry and
	 * add the new neighbour at the end.
	 *
	 * @param p_neighbour Neighbour vertex Id to add.
	 */
	public void addNeighbour(final long p_neighbour) {
		setNeighbourCount(m_neighbours.length + 1);
		m_neighbours[m_neighbours.length - 1] = p_neighbour;
	}

	/**
	 * Get the neighbour array.
	 *
	 * @return Neighbour array with vertex ids.
	 */
	public long[] getNeighbours() {
		return m_neighbours;
	}

	/**
	 * Resize the neighbour array.
	 *
	 * @param p_count Number of neighbours to resize to.
	 */
	public void setNeighbourCount(final int p_count) {
		if (p_count != m_neighbours.length) {
			// grow or shrink array
			m_neighbours = Arrays.copyOf(m_neighbours, p_count);
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
		int numNeighbours;

		m_userData = p_importer.readInt();
		numNeighbours = p_importer.readInt();
		m_neighbours = new long[numNeighbours];
		p_importer.readLongs(m_neighbours);

		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return Integer.BYTES
				+ Integer.BYTES
				+ Long.BYTES * m_neighbours.length;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return true;
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {

		p_exporter.writeInt(m_userData);

		// performance hack for BFS
		if (!m_flagWriteUserdataOnly) {
			p_exporter.writeInt(m_neighbours.length);
			p_exporter.writeLongs(m_neighbours);
		}

		return sizeofObject();
	}

	@Override
	public String toString() {
		String str = "Vertex[m_id " + Long.toHexString(m_id) + ", m_userData " + m_userData + ", numNeighbours "
				+ m_neighbours.length + "]: ";
		int counter = 0;
		for (Long v : m_neighbours) {
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

package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Created by nothaas on 9/8/16.
 */
public class Vertex implements DataStructure {

	private long m_id = ChunkID.INVALID_ID;
	private boolean m_neighborsAreEdgeObjects;
	private boolean m_locked;

	private int m_propertiesCount;
	private Property[] m_properties = new Property[0];
	private int m_neighborsCount;
	private long[] m_neighborIDs = new long[0];

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
		byte flags = p_importer.readByte();
		m_neighborsAreEdgeObjects = (flags & (1 << 1)) > 0;
		m_locked = (flags & (1 << 2)) > 0;

		m_propertiesCount = p_importer.readInt();
		m_properties = new Property[m_propertiesCount];
		for (int i = 0; i < m_propertiesCount; i++) {
			m_properties[i] = PropertyManager.createInstance(p_importer.readShort());
			p_importer.importObject(m_properties[i]);
		}

		m_neighborsCount = p_importer.readInt();
		m_neighborIDs = new long[m_neighborsCount];
		p_importer.readLongs(m_neighborIDs);
	}

	@Override
	public int sizeofObject() {
		int size = 0;

		size += Byte.BYTES;
		for (int i = 0; i < m_propertiesCount; i++) {
			size += Short.BYTES + m_properties[i].sizeofObject();
		}

		size += m_neighborsCount * Long.BYTES;
		return size;
	}

	@Override
	public void exportObject(final Exporter p_exporter) {

		byte flags = 0;
		flags |= m_neighborsAreEdgeObjects ? (1 << 0) : 0;
		flags |= m_locked ? (1 << 1) : 0;

		p_exporter.writeByte(flags);

		p_exporter.writeInt(m_propertiesCount);
		for (int i = 0; i < m_propertiesCount; i++) {
			p_exporter.writeShort(m_properties[i].getID());
			p_exporter.exportObject(m_properties[i]);
		}

		p_exporter.writeInt(m_neighborsCount);
		p_exporter.writeLongs(m_neighborIDs, 0, m_neighborsCount);
	}

	@Override
	public String toString() {
		String str = "Vertex[m_id " + Long.toHexString(m_id)
				+ ", m_neighborsAreEdgeObjects " + m_neighborsAreEdgeObjects
				+ ", m_locked " + m_locked
				+ ", m_propertiesCount " + m_propertiesCount
				+ ", m_neighborsCount " + m_neighborsCount
				+ "]: ";

		return str;
	}
}

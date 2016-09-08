package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Created by nothaas on 9/8/16.
 */
public class Edge implements DataStructure {

	private long m_id = ChunkID.INVALID_ID;

	private long m_fromId = ChunkID.INVALID_ID;
	private long m_toId = ChunkID.INVALID_ID;

	private int m_propertiesCount;
	Property[] m_properties = new Property[0];

	@Override
	public long getID() {
		return m_id;
	}

	@Override
	public void setID(long p_id) {
		m_id = p_id;
	}

	@Override
	public int exportObject(Exporter p_exporter, int p_size) {

		p_exporter.writeLong(m_fromId);
		p_exporter.writeLong(m_toId);

		p_exporter.writeInt(m_propertiesCount);
		for (int i = 0; i < m_propertiesCount; i++) {
			p_exporter.writeShort(m_properties[i].getID());
			p_exporter.exportObject(m_properties[i]);
		}

		return sizeofObject();
	}

	@Override
	public int importObject(Importer p_importer, int p_size) {

		m_fromId = p_importer.readLong();
		m_toId = p_importer.readLong();

		m_propertiesCount = p_importer.readInt();
		m_properties = new Property[m_propertiesCount];
		for (int i = 0; i < m_propertiesCount; i++) {
			m_properties[i] = PropertyManager.createInstance(p_importer.readShort());
			p_importer.importObject(m_properties[i]);
		}

		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		int size = 0;

		size += Long.BYTES * 2;
		for (Property property : m_properties) {
			size += Short.BYTES + property.sizeofObject();
		}

		return size;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return true;
	}
}

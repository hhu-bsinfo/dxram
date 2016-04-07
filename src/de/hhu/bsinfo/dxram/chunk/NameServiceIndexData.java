package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

public class NameServiceIndexData implements DataStructure {

	private static final int MS_NUM_INDICES = 1024;
	
	private long m_id = ChunkID.INVALID_ID;
	private short m_numEntries = 0;
	private int[] m_keys = new int[MS_NUM_INDICES];
	private long[] m_chunkIDs = new long[MS_NUM_INDICES];
	private long m_nextIndexDataChunkId = ChunkID.INVALID_ID;
	
	public NameServiceIndexData() {
	
	}
	
	public boolean insertMapping(final int p_key, final long p_chunkId) {
		if (m_numEntries == MS_NUM_INDICES) {
			return false;
		}
		
		m_keys[m_numEntries] = p_key;
		m_chunkIDs[m_numEntries] = p_chunkId;
		m_numEntries++;
		return true;
	}
	
	public void setNextIndexDataChunk(final long p_chunkID) {
		m_nextIndexDataChunkId = p_chunkID;
	}
	
	@Override
	public long getID() {
		return m_id;
	}

	@Override
	public void setID(long p_id) {
		m_id = p_id;
	}
	
	@Override
	public int importObject(Importer p_importer, int p_size) {
		m_numEntries = p_importer.readShort();
		for (int i = 0; i < MS_NUM_INDICES; i++) {
			m_keys[i] = p_importer.readInt();
			m_chunkIDs[i] = p_importer.readLong();
		}
		m_nextIndexDataChunkId = p_importer.readLong();
		
		return sizeofObject();
	}
	
	@Override
	public int exportObject(Exporter p_exporter, int p_size) {
		p_exporter.writeShort(m_numEntries);
		for (int i = 0; i < MS_NUM_INDICES; i++) {
			p_exporter.writeInt(m_keys[i]);
			p_exporter.writeLong(m_chunkIDs[i]);
		}
		p_exporter.writeLong(m_nextIndexDataChunkId);
		
		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return Short.BYTES + Integer.BYTES * MS_NUM_INDICES + Long.BYTES * MS_NUM_INDICES + Long.BYTES;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return false;
	}
}

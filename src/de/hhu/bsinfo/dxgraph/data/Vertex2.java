package de.hhu.bsinfo.dxgraph.data;

import java.util.Arrays;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

// vertex with a static list list of neighbours
// this limits the number of neighbours that can be stored
// within a single chunk in DXRAM (max chunk size 16MB)
// to roughly 2 million neighbours, which should be fine
// for many applications
public class Vertex2 implements DataStructure
{	
	private long m_id = ChunkID.INVALID_ID;
	private int m_userData = -1;
	private long[] m_neighbours = new long[0];
		
	public Vertex2()
	{

	}
	
	public Vertex2(final long p_id)
	{
		m_id = p_id;
	}
	
	public int getUserData()
	{
		return m_userData;
	}
	
	public void setUserData(final int p_userData)
	{
		m_userData = p_userData;
	}
	
	public long[] getNeighbours() {
		return m_neighbours;
	}
	
	public void setNeighbourCount(final int p_count) {
		if (p_count != m_neighbours.length) {
			// grow or shrink array
			Arrays.copyOf(m_neighbours, p_count);
		}
	}
	
	// -----------------------------------------------------------------------------

	@Override
	public long getID()
	{
		return m_id;
	}
	
	@Override
	public void setID(final long p_id)
	{
		m_id = p_id;
	}

	@Override
	public int importObject(Importer p_importer, int p_size) {
		int numNeighbours;
		
		m_userData = p_importer.readInt();
		numNeighbours = p_importer.readInt();
		m_neighbours = new long[numNeighbours];
		p_importer.readLongs(m_neighbours);
		
		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return 	Integer.BYTES
				+ 	Integer.BYTES
				+	Long.BYTES * m_neighbours.length;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return true;
	}

	@Override
	public int exportObject(Exporter p_exporter, int p_size) {
		
		p_exporter.writeInt(m_userData);
		p_exporter.writeInt(m_neighbours.length);
		p_exporter.writeLongs(m_neighbours);
		
		return sizeofObject();
	}
	
	@Override
	public String toString()
	{
		String str = "Vertex[m_id " + Long.toHexString(m_id) + ", m_userData " + m_userData + ", numNeighbours " + m_neighbours.length + "]: ";
		int counter = 0;
		for (Long v : m_neighbours)
		{
			str += Long.toHexString(v) + ", ";
			counter++;
			// avoid long strings
			if (counter > 9)
				break;
		}
		
		return str;
	}
}

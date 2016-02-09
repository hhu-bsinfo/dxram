package de.hhu.bsinfo.dxgraph.data;

import java.util.ArrayList;
import java.util.List;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

public class Vertex implements DataStructure
{	
	private long m_id = -1;
	private int m_userData = -1;
	private ArrayList<Long> m_neighbours = new ArrayList<Long>();
		
	public Vertex()
	{
		m_id = -1;
		m_userData = -1;
	}
	
	public Vertex(final long p_id)
	{
		m_id = p_id;
		m_userData = -1;
	}
	
	public int getUserData()
	{
		return m_userData;
	}
	
	public void setUserData(final int p_userData)
	{
		m_userData = p_userData;
	}
	
	public List<Long> getNeighbours()
	{
		return m_neighbours;
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
		for (int i = 0; i < numNeighbours; i++)
			m_neighbours.add(p_importer.readLong());
		
		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return 	Integer.BYTES
				+ 	Integer.BYTES
				+	Long.BYTES * m_neighbours.size();
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return true;
	}

	@Override
	public int exportObject(Exporter p_exporter, int p_size) {
		
		p_exporter.writeInt(m_userData);
		p_exporter.writeInt(m_neighbours.size());
		for (int i = 0; i < m_neighbours.size(); i++)
			p_exporter.writeLong(m_neighbours.get(i));
		
		return sizeofObject();
	}
	
	@Override
	public String toString()
	{
		String str = "Vertex[m_id " + Long.toHexString(m_id) + ", m_userData " + m_userData + ", numNeighbours " + m_neighbours.size() + "]: ";
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

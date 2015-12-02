package de.uniduesseldorf.dxgraph;

import java.util.Vector;

import de.uniduesseldorf.dxcompute.DataStructure;
import de.uniduesseldorf.dxcompute.DataStructureReader;
import de.uniduesseldorf.dxcompute.DataStructureWriter;

public class SimpleVertex implements DataStructure
{
	private static int IDX_USER_DATA = 0;
	private static int IDX_NUM_NEIGHBOURS = 4;
	private static int IDX_LIST_NEIGHBOURS = 8;
	
	private long m_id;
	private int m_userData;
	private Vector<Long> m_neighbours = new Vector<Long>();
	
	public static int getSizeWithNeighbours(int numNeighbours)
	{
		assert numNeighbours >= 0;
		
		return 	8
			+	4
			+	8 * numNeighbours;
	}
	
	public SimpleVertex()
	{
		m_id = -1;
		m_userData = -1;
	}
	
	public SimpleVertex(final long p_id)
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
	
	public Vector<Long> getNeighbours()
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
	public void write(DataStructureWriter p_writer) {
		p_writer.putInt(IDX_USER_DATA, m_userData);
		p_writer.putInt(IDX_NUM_NEIGHBOURS, m_neighbours.size());
		for (int i = 0; i < m_neighbours.size(); i++)
			p_writer.putLong(IDX_LIST_NEIGHBOURS + i * 8, m_neighbours.get(i));
	}

	@Override
	public void read(DataStructureReader p_reader) {
		int numNeighbours;
		
		m_userData = p_reader.getInt(IDX_USER_DATA);
		numNeighbours = p_reader.getInt(IDX_NUM_NEIGHBOURS);
		for (int i = 0; i < numNeighbours; i++)
			m_neighbours.add(p_reader.getLong(IDX_LIST_NEIGHBOURS + i * 8));
	}

	@Override
	public int sizeof() 
	{
		return 	4
			+ 	4
			+	8 * m_neighbours.size();
	}

	@Override
	public boolean hasDynamicSize() {
		return true;
	}
	
	@Override
	public String toString()
	{
		return "SimpleVertex[m_id " + m_id + ", m_userData " + m_userData + ", numNeighbours " + m_neighbours.size() + "]";
	}
}

package de.uniduesseldorf.dxgraph;

import java.nio.ByteBuffer;
import java.util.Vector;

public class SimpleVertex implements DataStructure
{
	private static int IDX_USER_DATA = 0;
	private static int IDX_NUM_NEIGHBOURS = 4;
	private static int IDX_LIST_NEIGHBOURS = 8;
	
	public static int getSizeWithNeighbours(final int p_neighbourCount)
	{
		assert p_neighbourCount >= 0;
		
		return 	4
			+	4
			+	8 * p_neighbourCount;
	}
	
	public static int getUserData(final byte[] p_data)
	{
		return ByteArrayUtils.getInt(p_data, IDX_USER_DATA);
	}
	
	public static void setUserData(final byte[] p_data, final int p_userData)
	{
		ByteArrayUtils.setInt(p_data, IDX_USER_DATA, p_userData);
	}
	
	public static int getNumberOfNeighbours(final byte[] p_data)
	{
		return ByteArrayUtils.getInt(p_data, IDX_NUM_NEIGHBOURS);
	}
	
	public static void setNumberOfNeighbours(final byte[] p_data, final int p_numNeighbours)
	{
		ByteArrayUtils.setInt(p_data, IDX_NUM_NEIGHBOURS, p_numNeighbours);
	}
	
	public static long getNeighbour(final byte[] p_data, final int p_idx)
	{
		return ByteArrayUtils.getLong(p_data, IDX_LIST_NEIGHBOURS + p_idx * 8);
	}
	
	public static void setNeighbour(final byte[] p_data, final int p_idx, final long p_neighbour)
	{
		ByteArrayUtils.setLong(p_data, IDX_LIST_NEIGHBOURS + p_idx * 8, p_neighbour);
	}
	
	private long m_id;
	private int m_userData;
	private Vector<Long> m_neighbours = new Vector<Long>();
	
	public SimpleVertex(final long p_id)
	{
		m_id = p_id;
		m_userData = -1;
	}
	
	public long getID()
	{
		return m_id;
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
	public void serialize(ByteBuffer p_outputBuffer) 
	{
		p_outputBuffer.putInt(m_userData);
		p_outputBuffer.putInt(m_neighbours.size());
		for (int i = 0; i < m_neighbours.size(); i++)
			p_outputBuffer.putLong(m_neighbours.get(i));
	}

	@Override
	public void deserialize(ByteBuffer p_inputBuffer) {
		m_userData = p_inputBuffer.getInt();
		m_neighbours.setSize(p_inputBuffer.getInt());
		for (int i = 0; i < m_neighbours.size(); i++)
			m_neighbours.add(p_inputBuffer.getLong());
	}

	@Override
	public int sizeof() 
	{
		return 	4
			+ 	4
			+	8 * m_neighbours.size();
	}
}

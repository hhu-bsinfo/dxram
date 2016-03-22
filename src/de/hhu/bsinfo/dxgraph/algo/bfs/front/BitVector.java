package de.hhu.bsinfo.dxgraph.algo.bfs.front;

public class BitVector implements FrontierList
{
	private long[] m_vector = null;		
	
	private long m_itPos = 0;
	private long m_count = 0;
	
	public BitVector(final long p_vertexCount)
	{
		m_vector = new long[(int) ((p_vertexCount / 64L) + 1L)];
	}
	
	@Override
	public void pushBack(final long p_index)
	{
		long tmp = (1L << (p_index % 64L));
		int idx = (int) (p_index / 64L);
		if ((m_vector[idx] & tmp) == 0)
		{				
			m_count++;
			m_vector[idx] |= tmp;
		}
	}
	
	@Override
	public long size()
	{
		return m_count;
	}
	
	@Override
	public boolean isEmpty()
	{
		return m_count == 0;
	}
	
	@Override
	public void reset()
	{
		m_itPos = 0;
		m_count = 0;
		for (int i = 0; i < m_vector.length; i++) {
			m_vector[i] = 0;
		}
	}
	
	@Override
	public long popFront()
	{		
		while (m_count > 0)
		{
			if ((m_vector[(int) (m_itPos / 64L)] & (1L << (m_itPos % 64L))) != 0)
			{
				long tmp = m_itPos;
				m_itPos++;	
				m_count--;
				return tmp;
			}

			m_itPos++;
		}
		
		return -1;
	}
	
	@Override
	public String toString()
	{
		return "[m_count " + m_count + ", m_itPos " + m_itPos + "]"; 
	}
}

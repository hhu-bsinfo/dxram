package de.hhu.bsinfo.dxgraph.algo.bfs.front;

public class BitVector implements FrontierList
{
	private long[] m_vector = null;		
	
	private int m_itPos = 0;
	private long m_itBit = 0;
	
	private long m_count = 0;
	
	public BitVector(final long p_vertexCount)
	{
		m_vector = new long[(int) ((p_vertexCount / 64L) + 1L)];
	}
	
	@Override
	public void pushBack(final long p_index)
	{
		long tmp = (1L << (p_index % 64L));
		if ((m_vector[(int) (p_index / 64L)] & tmp) == 0)
		{				
			m_count++;
			m_vector[(int) (p_index / 64L)] |= tmp;
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
		m_itBit = 0;
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
			if (m_vector[m_itPos] != 0)
			{
				while (m_itBit < 64L)
				{
					if (((m_vector[m_itPos] >> m_itBit) & 1L) != 0)
					{
						m_count--;
						return m_itPos * 64L + m_itBit++;
					}
					
					m_itBit++;
				}
				
				m_itBit = 0;
			}
			
			m_itPos++;
		}
		
		return -1;
	}
}

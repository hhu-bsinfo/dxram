package de.hhu.bsinfo.dxgraph.algo.bfs.front;

public class BitVectorMultiLevel implements FrontierList
{
	private long[] m_vectorL0 = null;		
	private long[] m_vectorL1 = null;
	
	private long m_itPos = 0;
	
	private long m_count = 0;
	
	public BitVectorMultiLevel(final long p_vertexCount)
	{
		m_vectorL0 = new long[(int) ((p_vertexCount / 64L) + 1L)];
		m_vectorL1 = new long[(int) ((m_vectorL0.length / 64L) + 1)];
	}
	
	@Override
	public void pushBack(final long p_index)
	{
		long tmp = (1L << (p_index % 64L));
		int idx = (int) (p_index / 64L);
		if ((m_vectorL0[idx] & tmp) == 0)
		{				
			m_count++;
			m_vectorL0[idx] |= tmp;
			m_vectorL1[idx / 64] |= (1L << (idx % 64L));
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
		for (int i = 0; i < m_vectorL0.length; i++) {
			m_vectorL0[i] = 0;
		}
		for (int i = 0; i < m_vectorL1.length; i++) {
			m_vectorL1[i] = 0;
		}
	}
	
	@Override
	public long popFront()
	{
		while (m_count > 0)
		{
			if (m_vectorL1[(int) (m_itPos / 4096L)] != 0)
			{
				for (int idxL1 = 0; idxL1 < 64; idxL1++)
				{
					if ((m_vectorL1[(int) (m_itPos / 4096L)] & (1L << idxL1)) != 0)
					{
						for (int idxL0 = 0; idxL0 < 64; idxL0++)
						{
							if ((m_vectorL0[(int) (m_itPos / 64L)] & (1L << (m_itPos % 64L))) != 0)
							{
								long tmp = m_itPos;
								m_itPos++;	
								m_count--;
								return tmp;
							}

							m_itPos++;
						}
					}
					else
					{
						m_itPos += 64L;
					}
				}
			}
			else
			{
				m_itPos += 4096L;
			}
		}
		
		return -1;
	}
	
	@Override
	public String toString()
	{
		return "[m_count " + m_count + ", m_itPos " + m_itPos + "]"; 
	}
}
package de.hhu.bsinfo.dxgraph.algo.bfs.front;

public class BitVectorOptimized implements FrontierList
{
	private long[] m_vectorL0 = null;		
	private long[] m_vectorL1 = null;
	
	private int m_itVecBit = 0;
	private int m_itVecL0 = 0;
	private int m_itVecL1 = 0;
	
	private long m_count = 0;
	
	public BitVectorOptimized(final long p_vertexCount)
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
		m_itVecBit = 0;
		m_itVecL0 = 0;
		m_itVecL1 = 0;
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
			if (m_vectorL1[m_itVecL1] != 0)
			{
				while (m_itVecL0 < 64)
				{
					int idxL0 = m_itVecL1 * 64 + m_itVecL0;
					if (m_vectorL0[idxL0] != 0)
					{
						while (m_itVecBit < 64)
						{
							if (((m_vectorL0[idxL0] >> m_itVecBit) & 1L) != 0)
							{
								m_count--;
								return idxL0 * 64L + m_itVecBit++;
							}
							
							m_itVecBit++;
						}
						
						m_itVecBit = 0;
					}
					
					m_itVecL0++;
				}		
				
				m_itVecL0 = 0;
			}
			
			m_itVecL1++;
		}
		
		return -1;
	}
}
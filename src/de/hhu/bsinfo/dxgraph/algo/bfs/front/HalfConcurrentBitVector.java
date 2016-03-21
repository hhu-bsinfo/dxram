package de.hhu.bsinfo.dxgraph.algo.bfs.front;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class HalfConcurrentBitVector implements FrontierList
{
	private AtomicLongArray m_vector = null;		
	
	private long m_itPos = 0;
	
	private AtomicLong m_count = new AtomicLong(0);
	
	public HalfConcurrentBitVector(final long p_vertexCount)
	{
		m_vector = new AtomicLongArray((int) ((p_vertexCount / 64L) + 1L));
	}
	
	@Override
	public void pushBack(final long p_index)
	{
		long tmp = (1L << (p_index % 64L));
		int index = (int) (p_index / 64L);
		
		while (true)
		{
			long val = m_vector.get(index);
			if ((val & tmp) == 0)
			{				
				if (!m_vector.compareAndSet(index, val, val | tmp))
					continue;
				m_count.incrementAndGet();
			}
			
			break;
		}
	}
	
	@Override
	public long size()
	{
		return m_count.get();
	}
	
	@Override
	public boolean isEmpty()
	{
		return m_count.get() == 0;
	}
	
	@Override
	public void reset()
	{
		m_itPos = 0;
		m_count.set(0);
		for (int i = 0; i < m_vector.length(); i++) {
			m_vector.set(i, 0);
		}
	}
	
	@Override
	public long popFront()
	{
		while (m_count.get() > 0)
		{
			if ((m_vector.get((int) (m_itPos / 64L)) & (1L << m_itPos % 64L)) != 0)
			{
				long tmp = m_itPos;
				m_itPos++;	
				m_count.decrementAndGet();
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

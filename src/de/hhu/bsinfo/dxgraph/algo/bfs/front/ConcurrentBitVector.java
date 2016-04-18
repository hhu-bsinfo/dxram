package de.hhu.bsinfo.dxgraph.algo.bfs.front;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Thread safe, lock free implementation of a frontier listed based on
 * a bit vector.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 *
 */
public class ConcurrentBitVector implements FrontierList
{
	private AtomicLongArray m_vector = null;		
	
	private AtomicLong m_itPos = new AtomicLong(0);
	private AtomicLong m_count = new AtomicLong(0);
	
	/**
	 * Constructor
	 * @param p_maxElementCount Specify the maximum number of elements.
	 */
	public ConcurrentBitVector(final long p_maxElementCount)
	{
		m_vector = new AtomicLongArray((int) ((p_maxElementCount / 64L) + 1L));
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
	public boolean contains(long p_val) {
		long tmp = (1L << (p_val % 64L));
		int index = (int) (p_val / 64L);
		return ((m_vector.get(index) & tmp) != 0);
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
		m_itPos.set(0);
		m_count.set(0);
		for (int i = 0; i < m_vector.length(); i++) {
			m_vector.set(i, 0);
		}
	}
	
	@Override
	public long popFront()
	{
		while (true)
		{
			// this section keeps threads out
			// if the vector is already empty
			long count = m_count.get();
			if (count > 0) {
				if (!m_count.compareAndSet(count, count - 1)) {
					continue;
				}
			} else {				
				return -1;
			}
			
			while (true)
			{
				long itPos = m_itPos.get();
				
				if ((m_vector.get((int) (itPos / 64L)) & (1L << itPos % 64L)) != 0)
				{
					if (!m_itPos.compareAndSet(itPos, itPos + 1)) {
						continue;
					}
					
					return itPos;
				}
				
				if (!m_itPos.compareAndSet(itPos, itPos + 1)) {
					continue;
				}
			}
		}
	}
	
	@Override
	public String toString()
	{
		return "[m_count " + m_count + ", m_itPos " + m_itPos + "]"; 
	}
}

package de.hhu.bsinfo.dxgraph.algo.bfs.front;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class ConcurrentBitVector implements FrontierList
{
	private AtomicLongArray m_vector = null;		
	
	private AtomicLong m_itPos = new AtomicLong(0);
	private AtomicLong m_count = new AtomicLong(0);
	
//	public static void main(String[] args)
//	{
//		int size = 100000000;
//		ConcurrentBitVector vec = new ConcurrentBitVector(size);
//		
//		long expected = 0;
//		for (int i = 0; i < size; i++)
//		{
//			vec.pushBack(i);
//			expected += i;
//		}
//		System.out.println(expected);
//		
//		Bla[] threads = new Bla[12];
//		for (int i = 0; i < threads.length; i++) {
//			threads[i] = new Bla();
//			threads[i].m_bitVector = vec;
//		}
//		System.out.println("start");
//
//		long time = System.nanoTime();
//		for (int i = 0; i < threads.length; i++) {
//			threads[i].start();
//		}
//		
//		long val = 0;
//		for (int i = 0; i < threads.length; i++) {
//			try {
//				threads[i].join();
//				//System.out.println("thread " + i + ": " + threads[i].m_val);
//				//val += threads[i].m_val;
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//		System.out.println("time: " + (System.nanoTime() - time) / (1000 * 1000.0));
//		
//		System.out.println("finished: " + val);
//	}
//	
//	private static class Bla extends Thread
//	{
//		public ConcurrentBitVector m_bitVector = null;
//		public long m_val = 0;
//		
//		@Override
//		public void run()
//		{
//			while (!m_bitVector.isEmpty())
//			{
//				long tmp = m_bitVector.popFront();
//				if (tmp != -1)
//					m_val += tmp;
//				Thread.yield();
//			}
//		}
//	}
	
	public ConcurrentBitVector(final long p_vertexCount)
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


package de.hhu.bsinfo.dxgraph.algo.bfs.front;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread safe, lock free implementation of a frontier listed based on
 * a bit vector.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 */
public class ConcurrentBitVectorHybrid implements FrontierList {
	private long m_maxElementCount;
	private long m_offset;
	private AtomicLongArray m_vector;

	private AtomicLong m_count = new AtomicLong(0);
	private AtomicLong m_itPos = new AtomicLong(0);
	private AtomicLong m_posCount = new AtomicLong(0);
	private AtomicLong m_posCountInverse = new AtomicLong(0);

	/**
	 * Constructor
	 *
	 * @param p_maxElementCount Specify the maximum number of elements.
	 */
	public ConcurrentBitVectorHybrid(final long p_maxElementCount, final long p_offset) {
		m_maxElementCount = p_maxElementCount;
		m_offset = p_offset;
		m_vector = new AtomicLongArray((int) ((p_maxElementCount / 64L) + 1L));
		m_posCountInverse.set(m_maxElementCount);
	}

	public static void main(final String[] p_args) throws Exception {
		final int vecSize = 10000000;
		ConcurrentBitVectorHybrid vec = new ConcurrentBitVectorHybrid(vecSize, 0);

		Thread[] threads = new Thread[24];
		while (true) {
			System.out.println("--------------------------");
			System.out.println("Fill....");
			for (int i = 0; i < threads.length; i++) {
				threads[i] = new Thread() {
					@Override
					public void run() {
						Random rand = new Random();

						for (int i = 0; i < 100000; i++) {
							vec.pushBack(rand.nextInt(vecSize));
						}
					}
				};
				threads[i].start();
			}

			for (Thread thread : threads) {
				thread.join();
			}

			System.out.println("Total elements: " + vec.size());
			System.out.println("Empty...");

			AtomicLong sum = new AtomicLong(0);
			for (int i = 0; i < threads.length; i++) {
				threads[i] = new Thread() {
					private long m_count;

					@Override
					public void run() {
						while (true) {
							long elem = vec.popFront();
							if (elem == -1) {
								sum.addAndGet(m_count);
								break;
							}

							m_count++;
						}
					}
				};
				threads[i].start();
			}

			for (Thread thread : threads) {
				thread.join();
			}

			System.out.println("Empty elements " + vec.size() + ", total elements got " + sum.get());

			vec.reset();
		}
	}

	@Override
	public boolean pushBack(final long p_index) {
		long tmp = 1L << ((p_index - m_offset) % 64L);
		int index = (int) ((p_index - m_offset) / 64L);

		while (true) {
			long val = m_vector.get(index);
			if ((val & tmp) == 0) {
				if (!m_vector.compareAndSet(index, val, val | tmp)) {
					continue;
				}
				//m_count.incrementAndGet();
				System.out.println("push back " + m_count.incrementAndGet() + "/" + (p_index - m_offset));
				return true;
			}

			return false;
		}
	}

	@Override
	public boolean contains(final long p_val) {
		long tmp = 1L << ((p_val - m_offset) % 64L);
		int index = (int) ((p_val - m_offset) / 64L);
		return (m_vector.get(index) & tmp) != 0;
	}

	@Override
	public long capacity() {
		return m_maxElementCount;
	}

	@Override
	public long size() {
		return m_count.get();
	}

	@Override
	public boolean isEmpty() {
		return m_count.get() == 0;
	}

	@Override
	public void reset() {
		m_itPos.set(0);
		m_posCount.set(0);
		m_posCountInverse.set(m_maxElementCount);
		m_count.set(0);
		for (int i = 0; i < m_vector.length(); i++) {
			m_vector.set(i, 0);
		}
	}

	private ReentrantLock m_popFrontLock = new ReentrantLock(false);

	public void popFrontLock() {
		m_popFrontLock.lock();
	}

	@Override
	public long popFront() {
		if (m_posCount.decrementAndGet() < 0) {
			m_posCount.set(0);
			return -1;
		}

		long itPos = m_itPos.get();
		while (true) {
			if ((m_vector.get((int) (itPos / 64L)) & (1L << (itPos % 64L))) != 0) {
				m_itPos.set(itPos + 1);

				System.out.println("Pop front " + m_posCountInverse.get() + "/" + (itPos + m_offset));
				return itPos + m_offset;
			}

			itPos++;
		}
	}

	public void popFrontReset() {
		m_itPos.set(0);
		m_posCount.set(m_count.get());
		m_posCountInverse.set(m_maxElementCount - m_count.get());
	}

	// get the non set indices
	public long popFrontInverse() {
		if (m_posCountInverse.decrementAndGet() < 0) {
			m_posCountInverse.set(0);
			return -1;
		}

		long itPos = m_itPos.get();
		while (true) {
			if ((m_vector.get((int) (itPos / 64L)) & (1L << (itPos % 64L))) == 0) {
				m_itPos.set(itPos + 1);

				System.out.println("Pop front inverse " + m_posCountInverse.get() + "/" + (itPos + m_offset));
				return itPos + m_offset;
			}

			itPos++;
		}
	}

	public void popFrontUnlock() {
		m_popFrontLock.unlock();
	}

	@Override
	public String toString() {
		return "[m_count " + m_count + ", m_itPos " + m_itPos + "]";
	}
}

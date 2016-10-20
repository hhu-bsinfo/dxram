package de.hhu.bsinfo.dxgraph.algo.bfs.front;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BitVector implementation using a reentrant lock for synchronization
 */
public class BitVectorReentrantLock {
	private long m_maxElementCount;
	private long[] m_vector;

	private long m_itPos;
	private long m_count;

	private ReentrantLock m_lock = new ReentrantLock(false);

	/**
	 * Constructor
	 *
	 * @param p_maxElementCount Specify the maximum number of elements.
	 */
	public BitVectorReentrantLock(final long p_maxElementCount) {
		m_maxElementCount = p_maxElementCount;
		m_vector = new long[(int) ((p_maxElementCount / 64L) + 1L)];
	}

	public static void main(final String[] p_args) throws Exception {
		final int vecSize = 10000000;
		BitVectorReentrantLock vec = new BitVectorReentrantLock(vecSize);

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
							vec.lock();
							vec.pushBack(rand.nextInt(vecSize));
							vec.unlock();
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
							vec.lock();
							long elem = vec.popFront();
							if (elem == -1) {
								vec.unlock();
								sum.addAndGet(m_count);
								break;
							}
							vec.unlock();

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

	public void lock() {
		m_lock.lock();
	}

	public void unlock() {
		m_lock.unlock();
	}

	public boolean pushBack(final long p_index) {
		long tmp = 1L << (p_index % 64L);
		int index = (int) (p_index / 64L);

		long val = m_vector[index];
		if ((val & tmp) == 0) {
			m_vector[index] = val | tmp;
			m_count++;
			return true;
		}

		return false;
	}

	public boolean contains(final long p_val) {
		long tmp = 1L << (p_val % 64L);
		int index = (int) (p_val / 64L);
		return (m_vector[index] & tmp) != 0;
	}

	public long capacity() {
		return m_maxElementCount;
	}

	public long size() {
		return m_count;
	}

	public boolean isEmpty() {
		return m_count == 0;
	}

	public void reset() {
		m_itPos = 0;
		m_count = 0;
		for (int i = 0; i < m_vector.length; i++) {
			m_vector[i] = 0;
		}
	}

	public long popFront() {
		if (m_count == 0) {
			return -1;
		}

		while (true) {
			if ((m_vector[(int) (m_itPos / 64L)] & (1L << (m_itPos % 64L))) != 0) {
				m_count--;
				return m_itPos++;
			}

			m_itPos++;
		}
	}

	@Override
	public String toString() {
		return "[m_count " + m_count + ", m_itPos " + m_itPos + "]";
	}
}


package de.hhu.bsinfo.dxgraph.algo.bfs.front;

/**
 * Implementation of a frontier list, which is represented
 * as a bit vector indicating which values it contains.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 */
public class BitVector implements FrontierList {
	private long[] m_vector;

	private long m_itPos;
	private long m_count;

	/**
	 * Constructor
	 *
	 * @param p_maxElementCount Specify the maximum number of elements.
	 */
	public BitVector(final long p_maxElementCount) {
		m_vector = new long[(int) ((p_maxElementCount / 64L) + 1L)];
	}

	@Override
	public boolean pushBack(final long p_index) {
		long tmp = 1L << (p_index % 64L);
		int idx = (int) (p_index / 64L);
		if ((m_vector[idx] & tmp) == 0) {
			m_count++;
			m_vector[idx] |= tmp;
			return true;
		}

		return false;
	}

	@Override
	public boolean contains(final long p_val) {
		long tmp = 1L << (p_val % 64L);
		int idx = (int) (p_val / 64L);
		return (m_vector[idx] & tmp) != 0;
	}

	@Override
	public long size() {
		return m_count;
	}

	@Override
	public boolean isEmpty() {
		return m_count == 0;
	}

	@Override
	public void reset() {
		m_itPos = 0;
		m_count = 0;
		for (int i = 0; i < m_vector.length; i++) {
			m_vector[i] = 0;
		}
	}

	@Override
	public long popFront() {
		while (m_count > 0) {
			if ((m_vector[(int) (m_itPos / 64L)] & (1L << (m_itPos % 64L))) != 0) {
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
	public String toString() {
		return "[m_count " + m_count + ", m_itPos " + m_itPos + "]";
	}
}

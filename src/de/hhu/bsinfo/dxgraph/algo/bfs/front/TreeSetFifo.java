
package de.hhu.bsinfo.dxgraph.algo.bfs.front;

import java.util.TreeSet;

/**
 * Frontier implementation using Java's TreeSet.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 */
public class TreeSetFifo implements FrontierList {

	private long m_maxElementCount;
	private TreeSet<Long> m_tree = new TreeSet<>();

	public TreeSetFifo(final long p_maxElementCount) {
		m_maxElementCount = p_maxElementCount;
	}

	@Override
	public boolean pushBack(final long p_val) {
		return m_tree.add(p_val);
	}

	@Override
	public boolean contains(final long p_val) {
		return m_tree.contains(p_val);
	}

	@Override
	public long capacity() {
		return m_maxElementCount;
	}

	@Override
	public long size() {
		return m_tree.size();
	}

	@Override
	public boolean isEmpty() {
		return m_tree.isEmpty();
	}

	@Override
	public void reset() {
		m_tree.clear();
	}

	@Override
	public long popFront() {
		Long tmp = m_tree.pollFirst();
		if (tmp == null) {
			return -1;
		}
		return tmp;
	}

}

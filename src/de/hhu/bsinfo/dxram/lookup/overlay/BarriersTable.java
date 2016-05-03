package de.hhu.bsinfo.dxram.lookup.overlay;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by nothaas on 5/3/16.
 */
public class BarriersTable {
	public static final int MS_INVALID_BARRIER_ID = -1;

	private short m_nodeId;

	private short[][] m_barrierState;
	private ReentrantLock[] m_barrierLocks;
	private int m_allocatedBarriersCount;

	private ReentrantLock m_allocationLock;

	public BarriersTable(final int p_maxNumBarriers, final short p_nodeId) {
		m_barrierState = new short[p_maxNumBarriers][];
		m_barrierLocks = new ReentrantLock[p_maxNumBarriers];
		m_allocatedBarriersCount = 0;

		m_allocationLock = new ReentrantLock(false);

		m_nodeId = p_nodeId;
	}

	public int allocateBarrier(final int p_size) {
		if (p_size < 1) {
			return MS_INVALID_BARRIER_ID;
		}

		m_allocationLock.lock();

		// find next available barrier
		for (int i = 0; i < m_barrierState.length; i++) {
			if (m_barrierState[i] == null) {
				m_barrierState[i] = new short[p_size + 1];
				m_barrierLocks[i] = new ReentrantLock(false);
				m_barrierState[i][0] = 0;
				m_allocatedBarriersCount++;
				m_allocationLock.unlock();
				return i;
			}
		}

		m_allocationLock.unlock();
		return MS_INVALID_BARRIER_ID;
	}

	public int getMaxNumOfBarriers() {
		return m_barrierState.length;
	}

	public int getNumAllocatedBarriers() {
		return m_allocatedBarriersCount;
	}

	public boolean freeBarrier(final int p_barrierId) {
		if (p_barrierId >= m_barrierState.length) {
			return false;
		}

		if (m_allocationLock == null) {
			return false;
		}

		m_allocationLock.lock();

		if (m_barrierState[p_barrierId] == null) {
			m_allocationLock.unlock();
			return false;
		}

		m_barrierLocks[p_barrierId].lock();
		m_barrierState[p_barrierId] = null;
		m_barrierLocks[p_barrierId].unlock();

		m_barrierLocks[p_barrierId] = null;
		m_allocatedBarriersCount--;

		m_allocationLock.unlock();

		return true;
	}

	public int signOn(final int p_barrierId, final short p_nodeId) {
		if (p_barrierId >= m_barrierState.length || m_barrierState[p_barrierId] == null) {
			return -1;
		}

		m_barrierLocks[p_barrierId].lock();

		if (m_barrierState[p_barrierId][0] == m_barrierState[p_barrierId].length - 1) {
			m_barrierLocks[p_barrierId].unlock();
			return -1;
		}

		m_barrierState[p_barrierId][0]++;
		m_barrierState[p_barrierId][(int) (m_barrierState[p_barrierId][0] & 0xFFFF)] = p_nodeId;

		int ret = m_barrierState[p_barrierId][0];
		m_barrierLocks[p_barrierId].unlock();
		return ret;
	}

	public boolean reset(final int p_barrierId) {
		if (p_barrierId >= m_barrierState.length || m_barrierState[p_barrierId] == null) {
			return false;
		}

		m_barrierLocks[p_barrierId].lock();

		for (int i = 0; i < m_barrierState[p_barrierId].length; i++) {
			m_barrierState[p_barrierId][i] = 0;
		}

		m_barrierLocks[p_barrierId].unlock();
		return true;
	}

	// first item is the sign on count
	public short[] getSignedOnPeers(final int p_barrierId) {
		if (p_barrierId >= m_barrierState.length || m_barrierState[p_barrierId] == null) {
			return null;
		}

		return m_barrierState[p_barrierId];
	}
}

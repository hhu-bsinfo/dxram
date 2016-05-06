package de.hhu.bsinfo.dxram.lookup.overlay;

import de.hhu.bsinfo.menet.NodeID;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Table managing synchronization barriers on a superpeer.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 06.05.16
 */
class BarriersTable {
	private short m_nodeId;

	private long[][] m_barrierData;
	private short[][] m_barrierState;
	private ReentrantLock[] m_barrierLocks;
	private int m_allocatedBarriersCount;

	private ReentrantLock m_allocationLock;

	/**
	 * Constructor
	 *
	 * @param p_maxNumBarriers Max number of barriers allowed to be allocated.
	 * @param p_nodeId         Node id of the superpeer this class is running on.
	 */
	BarriersTable(final int p_maxNumBarriers, final short p_nodeId) {
		m_barrierData = new long[p_maxNumBarriers][];
		m_barrierState = new short[p_maxNumBarriers][];
		m_barrierLocks = new ReentrantLock[p_maxNumBarriers];
		m_allocatedBarriersCount = 0;

		m_allocationLock = new ReentrantLock(false);

		m_nodeId = p_nodeId;
	}

	/**
	 * Allocate a new barrier.
	 *
	 * @param p_size Size of the barrier, i.e. how many peers have to sign on for release.
	 * @return Barrier id on succuess, -1 on failure.
	 */
	int allocateBarrier(final int p_size) {
		if (p_size < 1) {
			return BarrierID.INVALID_ID;
		}

		m_allocationLock.lock();

		// find next available barrier
		for (int i = 0; i < m_barrierState.length; i++) {
			if (m_barrierState[i] == null) {
				m_barrierData[i] = new long[p_size];
				m_barrierState[i] = new short[p_size + 1];
				m_barrierLocks[i] = new ReentrantLock(false);
				m_barrierState[i][0] = 0;
				for (int j = 1; j < p_size; j++) {
					m_barrierState[i][j] = NodeID.INVALID_ID;
				}
				m_allocatedBarriersCount++;
				m_allocationLock.unlock();
				return BarrierID.createBarrierId(m_nodeId, i);
			}
		}

		m_allocationLock.unlock();
		return BarrierID.INVALID_ID;
	}

	/**
	 * Free a previously allocated barrier.
	 *
	 * @param p_barrierId Id of the barrier to free.
	 * @return True if successful, false on failure.
	 */
	boolean freeBarrier(final int p_barrierId) {
		if (p_barrierId == BarrierID.INVALID_ID) {
			return false;
		}

		short nodeId = BarrierID.getOwnerID(p_barrierId);
		int id = BarrierID.getBarrierID(p_barrierId);

		if (nodeId != m_nodeId) {
			return false;
		}

		if (id >= m_barrierState.length) {
			return false;
		}

		if (m_allocationLock == null) {
			return false;
		}

		m_allocationLock.lock();

		if (m_barrierState[id] == null) {
			m_allocationLock.unlock();
			return false;
		}

		m_barrierLocks[id].lock();
		m_barrierData[id] = null;
		m_barrierState[id] = null;
		m_barrierLocks[id].unlock();

		m_barrierLocks[id] = null;
		m_allocatedBarriersCount--;

		m_allocationLock.unlock();

		return true;
	}

	/**
	 * Change the size of a barrier after being created (i.e. you want to keep the barrier id)
	 *
	 * @param p_barrierId Id of the barrier to change the size of.
	 * @param p_newSize   The new size for the barrier.
	 * @return True if chaning size was sucessful, false otherwise.
	 */
	boolean changeBarrierSize(final int p_barrierId, final int p_newSize) {
		if (p_barrierId == BarrierID.INVALID_ID) {
			return false;
		}

		if (p_newSize < 1) {
			return false;
		}

		short nodeId = BarrierID.getOwnerID(p_barrierId);
		int id = BarrierID.getBarrierID(p_barrierId);

		if (nodeId != m_nodeId) {
			return false;
		}

		m_barrierLocks[id].lock();
		// cannot change size if barrier is currently in use
		if (m_barrierState[id][0] != 0) {
			m_barrierLocks[id].unlock();
			return false;
		}

		m_barrierData[id] = new long[p_newSize];
		m_barrierState[id] = new short[p_newSize + 1];
		m_barrierState[id][0] = 0;
		for (int i = 1; i < m_barrierState[id].length; i++) {
			m_barrierState[id][i] = NodeID.INVALID_ID;
		}

		m_barrierLocks[id].unlock();
		return false;
	}

	/**
	 * Sign on to a barrier using a barrier id.
	 *
	 * @param p_barrierId   Barrier id to sign on to.
	 * @param p_nodeId      Id of the peer node signing on
	 * @param p_barrierData Additional custom data to pass along to the barrier
	 * @return On success returns the number of peers left to sign on, -1 on failure
	 */
	int signOn(final int p_barrierId, final short p_nodeId, final long p_barrierData) {
		if (p_barrierId == BarrierID.INVALID_ID) {
			return -1;
		}

		short nodeId = BarrierID.getOwnerID(p_barrierId);
		int id = BarrierID.getBarrierID(p_barrierId);

		if (nodeId != m_nodeId) {
			return -1;
		}

		if (id >= m_barrierState.length || m_barrierState[id] == null) {
			return -1;
		}

		m_barrierLocks[id].lock();

		if (m_barrierState[id][0] == m_barrierState[id].length - 1) {
			m_barrierLocks[id].unlock();
			return -1;
		}

		m_barrierData[id][(int) (m_barrierState[id][0] & 0xFFFF)] = p_barrierData;
		m_barrierState[id][0]++;
		m_barrierState[id][(int) (m_barrierState[id][0] & 0xFFFF)] = p_nodeId;

		int ret = (m_barrierState[id].length - 1) - m_barrierState[id][0];
		m_barrierLocks[id].unlock();
		return ret;
	}

	/**
	 * Reset an existing barrier for reuse.
	 *
	 * @param p_barrierId Id of the barrier to reset.
	 * @return True if successful, false otherwise.
	 */
	boolean reset(final int p_barrierId) {
		if (p_barrierId == BarrierID.INVALID_ID) {
			return false;
		}

		short nodeId = BarrierID.getOwnerID(p_barrierId);
		int id = BarrierID.getBarrierID(p_barrierId);

		if (nodeId != m_nodeId) {
			return false;
		}

		if (id >= m_barrierState.length || m_barrierState[id] == null) {
			return false;
		}

		m_barrierLocks[id].lock();

		m_barrierState[id][0] = 0;
		for (int i = 1; i < m_barrierState[id].length; i++) {
			m_barrierData[id][i - 1] = 0;
			m_barrierState[id][i] = NodeID.INVALID_ID;
		}

		m_barrierLocks[id].unlock();
		return true;
	}

	/**
	 * Get the list of currently signed on peers from a barrier.
	 * The first item (index 0) is the sign on count.
	 *
	 * @param p_barrierId Id of the barrier to get.
	 * @return Array with node ids that already signed on. First index element is the count of signed on peers.
	 */
	short[] getSignedOnPeers(final int p_barrierId) {
		if (p_barrierId == BarrierID.INVALID_ID) {
			return null;
		}

		short nodeId = BarrierID.getOwnerID(p_barrierId);
		int id = BarrierID.getBarrierID(p_barrierId);

		if (nodeId != m_nodeId) {
			return null;
		}

		if (id >= m_barrierState.length || m_barrierState[id] == null) {
			return null;
		}

		return m_barrierState[id];
	}

	/**
	 * Get the custom data of a barrier that is passed along on barrier sign ons.
	 *
	 * @param p_barrierId Id of the barrier to get the custom data of.
	 * @return On success returns an array with the currently available custom data (sorted by order the peers logged in)
	 */
	long[] getBarrierCustomData(final int p_barrierId) {
		if (p_barrierId == BarrierID.INVALID_ID) {
			return null;
		}

		short nodeId = BarrierID.getOwnerID(p_barrierId);
		int id = BarrierID.getBarrierID(p_barrierId);

		if (nodeId != m_nodeId) {
			return null;
		}

		if (id >= m_barrierState.length || m_barrierState[id] == null) {
			return null;
		}

		return m_barrierData[id];
	}
}

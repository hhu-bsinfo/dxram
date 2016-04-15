
package de.hhu.bsinfo.dxram.lookup.overlay;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Helper methods for superpeer overlay
 * @author Kevin Beineke <kevin.beineke@hhu.de> 02.04.16
 */
public final class OverlayHelper {

	private static final short CLOSED_INTERVAL = 0;
	private static final short UPPER_CLOSED_INTERVAL = 1;

	/**
	 * Hidden constructor
	 */
	private OverlayHelper() {}

	/**
	 * Verifies if node is between start and end
	 * @param p_nodeID
	 *            NodeID to compare
	 * @param p_startID
	 *            first NodeID
	 * @param p_endID
	 *            last NodeID
	 * @param p_type
	 *            the type of the interval (open, half-closed, closed)
	 * @return true if p_key is between p_start and p_end (including p_end or not), false otherwise
	 */
	public static boolean isNodeInRange(final short p_nodeID, final short p_startID, final short p_endID, final short p_type) {
		boolean ret = false;

		if (CLOSED_INTERVAL == p_type) {
			if (p_startID < p_endID) {
				// Example: m = 8, start = 2, end = 6 -> true: 2, 3, 4, 5, 6; false: 0, 1, 7
				if (p_nodeID >= p_startID && p_nodeID <= p_endID) {
					ret = true;
				}
			} else {
				// Example: m = 8, start = 6, end = 2 -> true: 6, 7, 0, 1, 2; false: 3, 4, 5
				if (p_nodeID >= p_startID || p_nodeID <= p_endID) {
					ret = true;
				}
			}
		} else if (UPPER_CLOSED_INTERVAL == p_type) {
			if (p_startID < p_endID) {
				// Example: m = 8, start = 2, end = 6 -> true: 3, 4, 5, 6; false: 0, 1, 2, 7
				if (p_nodeID > p_startID && p_nodeID <= p_endID) {
					ret = true;
				}
			} else {
				// Example: m = 8, start = 6, end = 2 -> true: 7, 0, 1, 2; false: 3, 4, 5, 6
				if (p_nodeID > p_startID || p_nodeID <= p_endID) {
					ret = true;
				}
			}
		} else {
			if (p_startID < p_endID) {
				// Example: m = 8, start = 2, end = 6 -> true: 3, 4, 5; false: 0, 1, 2, 6, 7
				if (p_nodeID > p_startID && p_nodeID < p_endID) {
					ret = true;
				}
			} else {
				// Example: m = 8, start = 6, end = 2 -> true: 7, 0, 1; false: 2, 3, 4, 5, 6
				if (p_nodeID > p_startID || p_nodeID < p_endID) {
					ret = true;
				}
			}
		}

		return ret;
	}

	/**
	 * Checks if the superpeer overlay is stable by comparing the current number of superpeers with the initially
	 * expected
	 * @param p_initialNumberOfSuperpeers
	 *            number of superpeers in nodes configuration
	 * @param p_currentNumberOfSuperpeers
	 *            number of currently available superpeers
	 * @return whether the overlay is stable or not
	 */
	protected static boolean isOverlayStable(final int p_initialNumberOfSuperpeers, final int p_currentNumberOfSuperpeers) {
		return p_initialNumberOfSuperpeers == p_currentNumberOfSuperpeers;
	}

	/**
	 * Inserts the superpeer at given position in the superpeer array
	 * @param p_superpeer
	 *            NodeID of the new superpeer
	 * @param p_superpeers
	 *            all superpeers
	 * @note assumes m_overlayLock has been locked
	 */
	protected static void insertSuperpeer(final short p_superpeer, final ArrayList<Short> p_superpeers) {
		int index;

		assert p_superpeer != NodeID.INVALID_ID;

		index = Collections.binarySearch(p_superpeers, p_superpeer);
		if (0 > index) {
			p_superpeers.add(index * -1 - 1, p_superpeer);
		}
	}

	/**
	 * Removes superpeer
	 * @param p_superpeer
	 *            NodeID of the superpeer that has to be removed
	 * @param p_superpeers
	 *            all superpeers
	 * @return the index if p_superpeer was found and deleted, -1 otherwise
	 * @note assumes m_overlayLock has been locked
	 */
	protected static int removeSuperpeer(final short p_superpeer, final ArrayList<Short> p_superpeers) {
		int ret = -1;
		int index;

		index = Collections.binarySearch(p_superpeers, p_superpeer);
		if (0 <= index) {
			p_superpeers.remove(index);
			ret = index;
		}

		return ret;
	}

	/**
	 * Inserts the peer at given position in the peer array
	 * @param p_peer
	 *            NodeID of the new peer
	 * @param p_peers
	 *            all peers
	 * @note assumes m_overlayLock has been locked
	 */
	protected static void insertPeer(final short p_peer, final ArrayList<Short> p_peers) {
		int index;

		assert p_peer != NodeID.INVALID_ID;

		index = Collections.binarySearch(p_peers, p_peer);
		if (0 > index) {
			p_peers.add(index * -1 - 1, p_peer);
		}
	}

	/**
	 * Removes peer
	 * @param p_peer
	 *            NodeID of the peer that has to be removed
	 * @param p_peers
	 *            all peers
	 * @return true if p_peer was found and deleted, false otherwise
	 * @note assumes m_overlayLock has been locked
	 */
	protected static boolean removePeer(final short p_peer, final ArrayList<Short> p_peers) {
		boolean ret = false;
		int index;

		index = Collections.binarySearch(p_peers, p_peer);
		if (0 <= index) {
			p_peers.remove(index);
			ret = true;
		}

		return ret;
	}

	/**
	 * Determines the superpeers this superpeer stores backups for
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_predecessor
	 *            the predecessor superpeer
	 * @param p_superpeers
	 *            all superpeers
	 * @return the superpeers p_nodeID is responsible for (got backups for)
	 * @note assumes m_overlayLock has been locked
	 */
	protected static short[] getResponsibleArea(final short p_nodeID, final short p_predecessor, final ArrayList<Short> p_superpeers) {
		short[] responsibleArea;
		short size;
		short index;

		size = (short) p_superpeers.size();
		responsibleArea = new short[2];
		if (3 < size) {
			index = (short) Collections.binarySearch(p_superpeers, p_predecessor);
			if (3 <= index) {
				index -= 3;
			} else {
				index = (short) (size - (3 - index));
			}
			responsibleArea[0] = p_superpeers.get(index);
			responsibleArea[1] = p_nodeID;
		} else {
			responsibleArea[0] = p_nodeID;
			responsibleArea[1] = p_nodeID;
		}
		return responsibleArea;
	}

	/**
	 * Determines the responsible superpeer for given NodeID
	 * @param p_nodeID
	 *            NodeID from chunk whose location is searched
	 * @param p_superpeers
	 *            all superpeers
	 * @param p_overlayLock
	 *            the overlay lock
	 * @param p_logger
	 *            the logger component
	 * @return the responsible superpeer for given ChunkID
	 */
	protected static short getResponsibleSuperpeer(final short p_nodeID, final ArrayList<Short> p_superpeers, final ReentrantLock p_overlayLock,
			final LoggerComponent p_logger) {
		short responsibleSuperpeer = -1;
		int index;

		p_logger.trace(OverlayHelper.class, "Entering getResponsibleSuperpeer with: p_nodeID=" + NodeID.toHexString(p_nodeID));

		p_overlayLock.lock();
		if (!p_superpeers.isEmpty()) {
			index = Collections.binarySearch(p_superpeers, p_nodeID);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == p_superpeers.size()) {
					index = 0;
				}
			}
			responsibleSuperpeer = p_superpeers.get(index);
			p_overlayLock.unlock();
		} else {
			p_logger.warn(OverlayHelper.class, "do not know any other superpeer");
			p_overlayLock.unlock();
		}
		p_logger.trace(OverlayHelper.class, "Exiting getResponsibleSuperpeer");

		return responsibleSuperpeer;
	}

	/**
	 * Determines the backup superpeers for this superpeer
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_superpeers
	 *            all superpeers
	 * @return the three successing superpeers
	 * @note assumes m_overlayLock has been locked
	 */
	protected static short[] getBackupSuperpeers(final short p_nodeID, final ArrayList<Short> p_superpeers) {
		short[] superpeers;
		int size;
		int index;

		if (p_superpeers.isEmpty()) {
			// m_logger.warn(getClass()"no replication possible. Too less superpeers");
			superpeers = new short[] {-1};
		} else {
			size = Math.min(p_superpeers.size(), 3);
			if (3 > size) {
				// m_logger.warn(getClass()"replication incomplete. Too less superpeers");
			}
			superpeers = new short[size];

			index = Collections.binarySearch(p_superpeers, p_nodeID);
			if (0 > index) {
				index = index * -1 - 1;
			} else {
				index++;
			}
			for (int i = 0; i < size; i++) {
				if (index == p_superpeers.size()) {
					superpeers[i] = p_superpeers.get(0);
					index = 1;
				} else {
					superpeers[i] = p_superpeers.get(index);
					index++;
				}
			}
		}
		return superpeers;
	}
}

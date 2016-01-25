
package de.hhu.bsinfo.soh;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

import de.hhu.bsinfo.dxram.utils.locks.SpinLock;

/**
 * The arena manager tries to assign one segment to each thread.
 * This only needs to happen on a malloc call that all newly allocated
 * addresses for that thread are obtained from a single segment. However,
 * if the segment of a thread is close to be full, further malloc calls might
 * not be possible to obtain sufficient space requested. Thus the thread gets a
 * new segment assigned. Worst case would be stealing a segment from another thread.
 * From this point on, one segment might be accessed (free, read, write calls) by several
 * threads having memory blocks allocated. The latter is not handled here. The arena
 * manager takes care of assigning threads for malloc calls, only, i.e. synchronization
 * for free, read and write calls still needs to be handled.
 * @author Florian Klein 28.08.2014
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.15
 */
public final class ArenaManager {

	// Attributes
	private Map<Long, SmallObjectHeapSegment> m_arenas;
	private SmallObjectHeapSegment[] m_segments;

	private Lock m_segmentLock;

	// Constructors
	/**
	 * Creates an instance of ArenaManager
	 * @param p_segments Segments to manage.
	 */
	public ArenaManager(final SmallObjectHeapSegment[] p_segments) {
		assert p_segments != null;
		assert p_segments.length > 0;

		m_arenas = new HashMap<>();
		m_segments = p_segments;

		m_segmentLock = new SpinLock();
	}

	// Methods
	/**
	 * Enter a the arena on a malloc call. This assigns a new segment based on the current
	 * state of free segments or steal a segment from another thread, if available and not
	 * blocked.
	 * @param p_threadID ID of the thread entering.
	 * @param p_minSize Minimum size for a free block the malloc call needs.
	 * @return Segment assigned to this thread.
	 * @throws MemoryException If assigning Segment failed.
	 */
	public SmallObjectHeapSegment enterArenaOnMalloc(final long p_threadID, final int p_minSize)
			throws MemoryException {
		SmallObjectHeapSegment ret;

		ret = m_arenas.get(p_threadID);
		if (ret == null || !ret.tryLockManage()) {
			ret = assignNewSegmentOnMalloc(p_threadID, null, p_minSize);
		}

		return ret;
	}

	/**
	 * Leave the arena after a malloc call and unlock the segment.
	 * @param p_threadID Thread ID leaving the segment.
	 * @param p_segment Segment to leave.
	 */
	public void leaveArenaOnMalloc(final long p_threadID, final SmallObjectHeapSegment p_segment) {
		p_segment.unlockManage();
	}

	/**
	 * Assigns a new Segment to the Thread
	 * @param p_threadID
	 *            the ID of the Thread
	 * @param p_current
	 *            the current assigned Segment
	 * @param p_minSize
	 *            the minimum size of the new Segment
	 * @return the new assigned Segment
	 * @throws MemoryException
	 *             if no Segment could be assigned
	 */
	public SmallObjectHeapSegment assignNewSegmentOnMalloc(final long p_threadID,
			final SmallObjectHeapSegment p_current, final int p_minSize) throws MemoryException {
		SmallObjectHeapSegment ret = null;
		SmallObjectHeapSegment tempUnassigned;
		double fragmentationUnassigned;
		long freeUnassigned;
		SmallObjectHeapSegment tempAssigned;
		double fragmentationAssigned;
		long freeAssigned;
		double fragmentationTemp;
		long freeTemp;
		long previousThreadID;

		m_segmentLock.lock();

		if (p_current != null) {
			m_arenas.put(p_threadID, null);
			p_current.unassign();
			p_current.unlockManage();
		}

		tempUnassigned = null;
		fragmentationUnassigned = 1;
		freeUnassigned = 0;
		tempAssigned = null;
		fragmentationAssigned = 1;
		freeAssigned = 0;
		// we got trouble assigning segments in multithreaded scenarios
		// the try counter (default was 10) is way too low and causes exceptions
		// that the segment could not be assigned
		// however, 100 is still too low and causes the same trouble
		// all operations on memory have to terminate i.e.
		// we force wait here until a segment is available for stealing
		// old code:
		// for (int tries = 0; tries < 100 && tempUnassigned == null && tempAssigned == null; tries++) {
		while (tempUnassigned == null && tempAssigned == null) {
			for (int i = 0; i < m_segments.length; i++) {
				if (m_segments[i].getStatus().getFreeSpace() > p_minSize && m_segments[i].tryLockManage()) {
					if (m_segments[i].isAssigned()) {
						fragmentationTemp = m_segments[i].getFragmentation();
						freeTemp = m_segments[i].getStatus().getFreeSpace();

						if (fragmentationTemp < fragmentationAssigned
								|| fragmentationTemp == fragmentationAssigned && freeTemp > freeAssigned) {
							if (tempAssigned != null) {
								tempAssigned.unlockManage();
							}
							tempAssigned = m_segments[i];
							fragmentationAssigned = fragmentationTemp;
							freeAssigned = freeTemp;
						} else {
							m_segments[i].unlockManage();
						}
					} else {
						fragmentationTemp = m_segments[i].getFragmentation();
						freeTemp = m_segments[i].getStatus().getFreeSpace();

						if (fragmentationTemp < fragmentationUnassigned
								|| fragmentationTemp == fragmentationUnassigned && freeTemp > freeUnassigned) {
							if (tempUnassigned != null) {
								tempUnassigned.unlockManage();
							}
							tempUnassigned = m_segments[i];
							fragmentationUnassigned = fragmentationTemp;
							freeUnassigned = freeTemp;
						} else {
							m_segments[i].unlockManage();
						}
					}
				}
			}
		}

		if (tempUnassigned != null) {
			ret = tempUnassigned;
			ret.assign(p_threadID);
			if (tempAssigned != null) {
				tempAssigned.unlockManage();
			}
		} else if (tempAssigned != null) {
			ret = tempAssigned;
			previousThreadID = ret.assign(p_threadID);
			m_arenas.put(previousThreadID, null);
		} else {
			throw new MemoryException("Could not assign new segment");
		}
		m_arenas.put(p_threadID, ret);

		m_segmentLock.unlock();

		return ret;
	}

}

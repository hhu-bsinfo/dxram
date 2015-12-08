
package de.uniduesseldorf.dxram.core.chunk.storage;

import java.util.Map.Entry;

import de.uniduesseldorf.dxram.core.chunk.storage.HeapWalker.MemoryBlock;
import de.uniduesseldorf.dxram.core.chunk.storage.HeapWalker.Results;

import java.util.Vector;

/**
 * Integrity checker for the heap. This needs data from the
 * HeapWalker as input to execute a number of checks to verify
 * if the heap is in a healthy state.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.11.15
 */
public final class HeapIntegrityChecker {
	private static Vector<AbstractCheck> m_checks = new Vector<AbstractCheck>();

	/**
	 * Modules for the integrity checker.
	 */
	static {
		m_checks.add(new CheckHeapSizeBounds());
		m_checks.add(new CheckSegmentCount());
		m_checks.add(new CheckSegmentBounds());
		m_checks.add(new CheckSegmentCheckFreeBlockListBounds());
		m_checks.add(new CheckMemoryBlocksSegmentBounds());
	}

	/**
	 * Constructor
	 */
	private HeapIntegrityChecker() {}

	/**
	 * Execute the integrity check based on the provided results. The checks
	 * will abort as soon as one check fails.
	 * @param p_walkerResults
	 *            Results from the HeapWalker.
	 * @return 0 If all checks were successful or a negative number indicating which check failed.
	 */
	public static int check(final HeapWalker.Results p_walkerResults) {
		int ret = 0;

		for (int i = 0; i < m_checks.size(); i++) {
			if (!m_checks.get(i).check(p_walkerResults)) {
				ret = -m_checks.get(i).getID();
				break;
			}
		}

		return ret;
	}

	// -------------------------------------------------------------------------------

	/**
	 * Check the bounds of the memory area of the segments.
	 */
	private static class CheckMemoryBlocksSegmentBounds extends AbstractCheck {
		/**
		 * Constructor
		 */
		public CheckMemoryBlocksSegmentBounds() {
			super(5, "CheckMemoryBlocksSegmentBounds");
		}

		@Override
		protected boolean checkInner(final Results p_walkerResults) {
			for (HeapWalker.Segment segment : p_walkerResults.m_segments) {
				for (Entry<Long, MemoryBlock> entry : segment.m_memoryBlocks.entrySet()) {
					MemoryBlock block;
					block = entry.getValue();
					if (block.m_startAddress < segment.m_segmentBlock.m_startAddress
							|| block.m_endAddress > segment.m_segmentBlock.m_endAddress) {
						System.out.println("Found memory block which is not fully included in its segment.");
						System.out.println(segment);
						System.out.println(block);
					}
				}
			}

			return true;
		}
	}

	/**
	 * Check the bounds of the free block list of the segments.
	 */
	private static class CheckSegmentCheckFreeBlockListBounds extends AbstractCheck {
		/**
		 * Constructor
		 */
		public CheckSegmentCheckFreeBlockListBounds() {
			super(4, "CheckSegmentCheckFreeBlockListBounds");
		}

		@Override
		protected boolean checkInner(final Results p_walkerResults) {
			boolean ret = true;

			for (HeapWalker.Segment segment : p_walkerResults.m_segments) {
				if (segment.m_segmentBlock.m_startAddressFreeBlocksList < segment.m_segmentBlock.m_startAddress ||
						segment.m_segmentBlock.m_startAddressFreeBlocksList > segment.m_segmentBlock.m_endAddress) {
					System.out.println("FreeBlockList is not within segment bounds: " + segment);
					ret = false;
					break;
				}

				if (segment.m_segmentBlock.m_sizeFreeBlocksListArea != segment.m_segmentBlock.m_endAddress
						- segment.m_segmentBlock.m_startAddressFreeBlocksList) {
					System.out.println(
							"Size of FreeBlockList does not fit the assigned area in segment (check addresses): "
									+ segment);
					ret = false;
					break;
				}
			}

			return ret;
		}
	}

	/**
	 * Check the bounds of the segments.
	 */
	private static class CheckSegmentBounds extends AbstractCheck {
		/**
		 * Constructor
		 */
		public CheckSegmentBounds() {
			super(3, "CheckSegmentBounds");
		}

		@Override
		protected boolean checkInner(final Results p_walkerResults) {
			HeapWalker.Segment prevSegment = null;
			boolean ret = true;

			for (HeapWalker.Segment segment : p_walkerResults.m_segments) {
				if (prevSegment == null) {
					if (segment.m_segmentBlock.m_startAddress != 0) {
						System.out.println("Start address of first segment not 0: " + segment);
						ret = false;
						break;
					}

					prevSegment = segment;
				} else {
					if (prevSegment.m_segmentBlock.m_endAddress != segment.m_segmentBlock.m_startAddress) {
						System.out.println(
								"End address of previous block does not match the start address of next block (either overlapping or gap):");
						System.out.println("Previous: " + prevSegment);
						System.out.println("Next: " + segment);
						ret = false;
						break;
					}
				}
			}

			return ret;
		}
	}

	/**
	 * Check the number of segments.
	 */
	private static class CheckSegmentCount extends AbstractCheck {
		/**
		 * Constructor
		 */
		public CheckSegmentCount() {
			super(2, "SegmentCount");
		}

		@Override
		protected boolean checkInner(final Results p_walkerResults) {
			return p_walkerResults.m_heap.m_numSegments == p_walkerResults.m_segments.size();
		}
	}

	/**
	 * Check the bounds of the heap.
	 */
	private static class CheckHeapSizeBounds extends AbstractCheck {
		/**
		 * Constructor
		 */
		public CheckHeapSizeBounds() {
			super(1, "HeapSizeBounds");
		}

		@Override
		protected boolean checkInner(final Results p_walkerResults) {
			return p_walkerResults.m_heap.m_totalSize == p_walkerResults.m_heap.m_numSegments
					* p_walkerResults.m_heap.m_segmentSize;
		}
	}

	/**
	 * Base class for all checks executed by the integrity checker.
	 */
	private abstract static class AbstractCheck {
		private int m_id = -1;
		private String m_name = new String();

		/**
		 * Constructor
		 * @param p_id
		 *            ID of the check.
		 * @param p_name
		 *            Name of the check.
		 */
		public AbstractCheck(final int p_id, final String p_name) {
			m_id = p_id;
			m_name = p_name;
		}

		/**
		 * Get the ID of the check.
		 * @return ID.
		 */
		public int getID() {
			return m_id;
		}

		/**
		 * Get the name of the check.
		 * @return Name.
		 */
		@SuppressWarnings("unused")
		public String getName() {
			return m_name;
		}

		/**
		 * Execute the check.
		 * @param p_walkerResults
		 *            Input data for the check: Results of the HeapWalker.
		 * @return True if check successful, false if failed.
		 */
		public boolean check(final HeapWalker.Results p_walkerResults) {
			boolean ret = true;

			System.out.println("Executing " + this);
			if (checkInner(p_walkerResults)) {
				System.out.println(this + " successful.");
				ret = true;
			} else {
				System.out.println(this + " failed.");
				ret = false;
			}

			return ret;
		}

		/**
		 * Implement the actual check here.
		 * @param p_walkerResults
		 *            Input data from the HeapWalker.
		 * @return True if check successful, false if failed.
		 */
		protected abstract boolean checkInner(final HeapWalker.Results p_walkerResults);

		@Override
		public String toString() {
			return "Check: " + m_id + " | " + m_name;
		}
	}
}

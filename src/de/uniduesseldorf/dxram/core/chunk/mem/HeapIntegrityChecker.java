package de.uniduesseldorf.dxram.core.chunk.mem;

import java.util.Map.Entry;
import java.util.Vector;

import de.uniduesseldorf.dxram.core.chunk.mem.HeapWalker.MemoryBlock;
import de.uniduesseldorf.dxram.core.chunk.mem.HeapWalker.Results;

public class HeapIntegrityChecker 
{
	private static Vector<Check> m_checks = new Vector<Check>();
	
	static {
		m_checks.add(new CheckHeapSizeBounds());
		m_checks.add(new CheckSegmentCount());
		m_checks.add(new CheckSegmentBounds());
		m_checks.add(new CheckSegmentCheckFreeBlockListBounds());
		m_checks.add(new CheckMemoryBlocksSegmentBounds());
	}
	
	private HeapIntegrityChecker()
	{

	}
	
	public static int check(final HeapWalker.Results p_walkerResults)
	{		
		for (int i = 0; i < m_checks.size(); i++)
		{
			if (!m_checks.get(i).check(p_walkerResults))
				return -m_checks.get(i).getID();
		}
		
		return 0;
	}
	
	// -------------------------------------------------------------------------------
	
	private static class CheckMemoryBlocksSegmentBounds extends Check
	{
		public CheckMemoryBlocksSegmentBounds() {
			super(5, "CheckMemoryBlocksSegmentBounds");
		}

		@Override
		protected boolean checkInner(Results p_walkerResults) 
		{
			for (HeapWalker.Segment segment : p_walkerResults.m_segments)
			{
				for (Entry<Long, MemoryBlock> entry : segment.m_memoryBlocks.entrySet())
				{
					MemoryBlock block = entry.getValue();
					if (block.m_startAddress < segment.m_segmentBlock.m_startAddress ||
						block.m_endAddress > segment.m_segmentBlock.m_endAddress)
					{
						System.out.println("Found memory block which is not fully included in its segment.");
						System.out.println(segment);
						System.out.println(block);
					}
				}
			}
			
			return true;
		}
	}
	
	private static class CheckSegmentCheckFreeBlockListBounds extends Check
	{
		public CheckSegmentCheckFreeBlockListBounds() {
			super(4, "CheckSegmentCheckFreeBlockListBounds");
		}

		@Override
		protected boolean checkInner(Results p_walkerResults) 
		{
			HeapWalker.Segment prevSegment = null;
			
			for (HeapWalker.Segment segment : p_walkerResults.m_segments)
			{
				if (segment.m_segmentBlock.m_startAddressFreeBlocksList < segment.m_segmentBlock.m_startAddress ||
					segment.m_segmentBlock.m_startAddressFreeBlocksList > segment.m_segmentBlock.m_endAddress)
				{
					System.out.println("FreeBlockList is not within segment bounds: " + segment);
					return false;
				}
				
				if (segment.m_segmentBlock.m_sizeFreeBlocksListArea != segment.m_segmentBlock.m_endAddress - segment.m_segmentBlock.m_startAddressFreeBlocksList)
				{
					System.out.println("Size of FreeBlockList does not fit the assigned area in segment (check addresses): " + segment);
					return false;
				}
			}
			
			return true;
		}
	}
	
	private static class CheckSegmentBounds extends Check
	{
		public CheckSegmentBounds() {
			super(3, "CheckSegmentBounds");
		}

		@Override
		protected boolean checkInner(Results p_walkerResults) 
		{
			HeapWalker.Segment prevSegment = null;
			
			for (HeapWalker.Segment segment : p_walkerResults.m_segments)
			{
				if (prevSegment == null)
				{
					if (segment.m_segmentBlock.m_startAddress != 0)
					{
						System.out.println("Start address of first segment not 0: " + segment);
						return false;
					}
					
					prevSegment = segment;
				}
				else
				{
					if (prevSegment.m_segmentBlock.m_endAddress != segment.m_segmentBlock.m_startAddress)
					{
						System.out.println("End address of previous block does not match the start address of next block (either overlapping or gap):");
						System.out.println("Previous: " + prevSegment);
						System.out.println("Next: " + segment);
						return false;
					}
				}
			}
			
			return true;
		}
	}
	
	private static class CheckSegmentCount extends Check
	{
		public CheckSegmentCount() {
			super(2, "SegmentCount");
		}

		@Override
		protected boolean checkInner(Results p_walkerResults) {
			return p_walkerResults.m_heap.m_numSegments == p_walkerResults.m_segments.size();
		}
	}
	
	private static class CheckHeapSizeBounds extends Check
	{
		public CheckHeapSizeBounds() {
			super(1, "HeapSizeBounds");
		}

		@Override
		protected boolean checkInner(Results p_walkerResults) {
			return p_walkerResults.m_heap.m_totalSize == p_walkerResults.m_heap.m_numSegments * p_walkerResults.m_heap.m_segmentSize;
		}	
	}
	
	private static abstract class Check
	{
		private int m_id = -1;
		private String m_name = null;
		
		public Check(final int p_id, final String p_name)
		{
			m_id = p_id;
			m_name = p_name;
		}
		
		public int getID()
		{
			return m_id;
		}
		
		public String getName()
		{
			return m_name;
		}
		
		public boolean check(final HeapWalker.Results p_walkerResults)
		{
			System.out.println("Executing " + this);
			if (checkInner(p_walkerResults))
			{
				System.out.println(this + " successful.");
				return true;
			}
			else
			{
				System.out.println(this + " failed.");
				return true;
			}
		}
		
		protected abstract boolean checkInner(final HeapWalker.Results p_walkerResults);
		
		@Override
		public String toString()
		{
			return "Check: " + m_id + " | " + m_name;
		}
	}
}

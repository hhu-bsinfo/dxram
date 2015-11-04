package de.uniduesseldorf.dxram.core.chunk.storage.analyse;

import de.uniduesseldorf.dxram.core.chunk.mem.Segment;

public class RawMemoryWalker 
{
	public RawMemoryWalker()
	{
		
	}
	
	private class SegmentWalker
	{
		private Segment m_segment;
		
		
		
		public SegmentWalker(Segment segment)
		{
			m_segment = segment;
		}
		
		public void analyze()
		{
			m_segment.lockManage();
			
			clearResults();
			walkMemoryBlocks();
			walkMemoryFreeBlockList();
			
			m_segment.unlockManage();
		}
		
		private void clearResults()
		{
			
		}
		
		private void walkMemoryBlocks()
		{
			
		}
		
		private void walkMemoryFreeBlockList()
		{
			
		}
		
		public class MemoryBlock
		{
			public long m_startAddress;
			public long m_endAddress;
			
			public MemoryBlock()
			{
				
			}
		}
	}
}

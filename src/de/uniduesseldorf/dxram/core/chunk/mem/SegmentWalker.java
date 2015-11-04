package de.uniduesseldorf.dxram.core.chunk.mem;

public class SegmentWalker 
{
	private Segment m_segment;
		
	public SegmentWalker(Segment segment)
	{
		m_segment = segment;
	}
	
	public void walk()
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
		// get what we need from the segment
		Storage memory = m_segment.m_memory;
		long baseAddress = m_segment.m_base;
		long fullSize = m_segment.m_fullSize;
		
		// walk memory
		while (baseAddress < fullSize)
		{
			// TODO
		}
	}
		
	private void walkMemoryFreeBlockList()
	{
		
	}
		
	public class MemoryBlock
	{
		public long m_startAddress = -1;
		public long m_endAddress = -1;
		public long m_rawBlockSizeWithoutMarkers = -1;
		public byte m_markerByte = -1;
		public long m_blockContentSize = -1;
		public long m_nextFreeBlock = -1;
		public long m_prevFreeBlock = -1;
		
		public MemoryBlock()
		{
			
		}
	}

}

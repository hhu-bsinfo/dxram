package de.uniduesseldorf.dxram.core.chunk.mem;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

public class SegmentWalker 
{
	private Segment m_segment;
		
	private SegmentBlock m_segmentBlock = null;
	private HashMap<Long, MemoryBlock> m_memoryBlocks = new HashMap<Long, MemoryBlock>();
	
	public SegmentWalker(Segment segment)
	{
		m_segment = segment;
	}
	
	public void walk()
	{
		m_segment.lockManage();
		
		clearResults();
		parseSegment();
		walkMemoryBlocks();
		
		System.out.println(m_segmentBlock);
		Iterator<Entry<Long, MemoryBlock>> it = m_memoryBlocks.entrySet().iterator();
		while (it.hasNext())
		{
			Entry<Long, MemoryBlock> entry = it.next();
			
			System.out.println(entry.getValue());
		}
		
		walkMemoryFreeBlockList();
		
		m_segment.unlockManage();
	}
		
	private void clearResults()
	{
		m_segmentBlock = null;
		m_memoryBlocks.clear();
	}
	
	private void parseSegment()
	{
		m_segmentBlock = new SegmentBlock();
		m_segmentBlock.m_startAddress = m_segment.m_base;
		m_segmentBlock.m_endAddress = m_segment.m_base + m_segment.m_fullSize;
		m_segmentBlock.m_sizeMemoryBlockArea = m_segment.m_size;
		m_segmentBlock.m_startAddressFreeBlocksList = m_segment.m_base + m_segment.m_size;
		m_segmentBlock.m_sizeFreeBlocksListArea = m_segment.m_fullSize - m_segment.m_size;
	}
		
	private void walkMemoryBlocks()
	{
		// get what we need from the segment
		long baseAddress = m_segment.m_base;
		long blockAreaSize = m_segment.m_size;
		
		// walk memory block area
		while (baseAddress < blockAreaSize - 1)
		{
			MemoryBlock block = new MemoryBlock();
			
			try {
				block.m_startAddress = baseAddress;
				block.m_markerByte = m_segment.readRightPartOfMarker(baseAddress);
				
				switch (block.m_markerByte)
				{
					// free memory less than 12 bytes
					case 0:
					{
						int lengthFieldSize = 1;
						// size includes length field
						int sizeBlock = (int) m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE, lengthFieldSize);
		
						// + 2 marker bytes
						block.m_endAddress = baseAddress + sizeBlock + Segment.SIZE_MARKER_BYTE * 2;
						block.m_rawBlockSize = sizeBlock;
						block.m_customState = -1;
						block.m_prevFreeBlock = -1;
						block.m_nextFreeBlock = -1;
						block.m_blockPayloadSize = -1;
						
						m_memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += Segment.SIZE_MARKER_BYTE + lengthFieldSize;
						break;
					}
						
					// free block with X bytes length field and size >= 12 bytes
					case 1:
					case 2:
					case 3:
					case 4:
					case 5:
					{
						int lengthFieldSize = block.m_markerByte;
						long freeBlockSize = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE, lengthFieldSize);
						
						// + 2 marker bytes
						block.m_endAddress = baseAddress + freeBlockSize + Segment.SIZE_MARKER_BYTE * 2;
						block.m_rawBlockSize = freeBlockSize;
						block.m_customState = -1;
						block.m_prevFreeBlock = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE + lengthFieldSize, Segment.POINTER_SIZE);
						block.m_nextFreeBlock = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE + lengthFieldSize + Segment.POINTER_SIZE, Segment.POINTER_SIZE);
						block.m_blockPayloadSize = -1; // no payload
						
						m_memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += Segment.SIZE_MARKER_BYTE + freeBlockSize;
						
						break;
					}
						
					// malloc'd block, 1 byte length field
					case 6:
					case 9:
					case 12:
					{
						int lengthFieldSize = 1;
						long blockPayloadSize = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE, lengthFieldSize);
						
						// + 2 marker bytes
						block.m_endAddress = baseAddress + lengthFieldSize + blockPayloadSize + Segment.SIZE_MARKER_BYTE * 2;
						block.m_rawBlockSize = lengthFieldSize + blockPayloadSize;
						block.m_customState = m_segment.getCustomState(baseAddress + Segment.SIZE_MARKER_BYTE);
						block.m_prevFreeBlock = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE + lengthFieldSize, Segment.POINTER_SIZE);
						block.m_nextFreeBlock = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE + lengthFieldSize + Segment.POINTER_SIZE, Segment.POINTER_SIZE);
						block.m_blockPayloadSize = blockPayloadSize;
						
						m_memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += Segment.SIZE_MARKER_BYTE + lengthFieldSize + blockPayloadSize;
						
						break;
					}
					
					// malloc'd block, 2 byte length field
					case 7:
					case 10:
					case 13:
					{
						int lengthFieldSize = 2;
						long blockPayloadSize = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE, lengthFieldSize);
						
						// + 2 marker bytes
						block.m_endAddress = baseAddress + lengthFieldSize + blockPayloadSize + Segment.SIZE_MARKER_BYTE * 2;
						block.m_rawBlockSize = lengthFieldSize + blockPayloadSize;
						block.m_customState = m_segment.getCustomState(baseAddress + Segment.SIZE_MARKER_BYTE);
						block.m_prevFreeBlock = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE + lengthFieldSize, Segment.POINTER_SIZE);
						block.m_nextFreeBlock = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE + lengthFieldSize + Segment.POINTER_SIZE, Segment.POINTER_SIZE);
						block.m_blockPayloadSize = blockPayloadSize;
						
						m_memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += Segment.SIZE_MARKER_BYTE + lengthFieldSize + blockPayloadSize;
						
						break;
					}
					
					// malloc'd block, 3 byte length field
					case 8:
					case 11:
					case 14:
					{
						int lengthFieldSize = 3;
						long blockPayloadSize = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE, lengthFieldSize);
						
						// + 2 marker bytes
						block.m_endAddress = baseAddress + lengthFieldSize + blockPayloadSize + Segment.SIZE_MARKER_BYTE * 2;
						block.m_rawBlockSize = lengthFieldSize + blockPayloadSize;
						block.m_customState = m_segment.getCustomState(baseAddress + Segment.SIZE_MARKER_BYTE);
						block.m_prevFreeBlock = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE + lengthFieldSize, Segment.POINTER_SIZE);
						block.m_nextFreeBlock = m_segment.read(baseAddress + Segment.SIZE_MARKER_BYTE + lengthFieldSize + Segment.POINTER_SIZE, Segment.POINTER_SIZE);
						block.m_blockPayloadSize = blockPayloadSize;
						
						m_memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += Segment.SIZE_MARKER_BYTE + lengthFieldSize + blockPayloadSize;
						
						break;
					}
					
					// free memory 1 byte
					case 15:
					{
						block.m_endAddress = baseAddress + Segment.SIZE_MARKER_BYTE;
						block.m_rawBlockSize = 0;
						block.m_customState = -1;
						block.m_prevFreeBlock = -1;
						block.m_nextFreeBlock = -1;
						block.m_blockPayloadSize = 0;
						
						m_memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += Segment.SIZE_MARKER_BYTE;
						
						break;
					}
					
					default:
					{
						// TODO exception? log error?
					}
				}			
			} catch (MemoryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
		
	private void walkMemoryFreeBlockList()
	{
		
	}
	
	public class SegmentBlock
	{
		public long m_startAddress = -1;
		public long m_endAddress = -1;
		public long m_sizeMemoryBlockArea = -1;
		public long m_startAddressFreeBlocksList = -1;
		public long m_sizeFreeBlocksListArea = -1;
		
		public SegmentBlock()
		{
			
		}
		
		@Override
		public String toString()
		{
			return "SegmentBlock(m_startAddress " + m_startAddress +
					", m_endAddress " + m_endAddress + 
					", m_sizeMemoryBlockArea " + m_sizeMemoryBlockArea +
					", m_startAddressFreeBlocksList " + m_startAddressFreeBlocksList +
					", m_sizeFreeBlocksListArea " + m_sizeFreeBlocksListArea + ")";
		}
	}
		
	public class MemoryBlock
	{
		public long m_startAddress = -1;
		// end address excluding
		public long m_endAddress = -1;
		// markers don't count
		public long m_rawBlockSize = -1;
		public int m_markerByte = -1;
		public int m_customState = -1;
		public long m_blockPayloadSize = -1;
		public long m_nextFreeBlock = -1;
		public long m_prevFreeBlock = -1;
		
		public MemoryBlock()
		{
			
		}
		
		@Override
		public String toString()
		{
			return "MemoryBlock(m_startAddress " + m_startAddress +
					", m_endAddress " + m_endAddress +
					", m_rawBlockSize " + m_rawBlockSize +
					", m_markerByte " + m_markerByte +
					", m_customState " + m_customState +
					", m_blockPayloadSize " + m_blockPayloadSize + 
					", m_prevFreeBlock " + m_prevFreeBlock +
					", m_nextFreeBlock " + m_nextFreeBlock +
					 ")"; 
		}
	}

	public class FreeBlockList
	{
		public long m_freeBlockSize = -1; 
		public Vector<MemoryBlock> m_freeBlocks = new Vector<MemoryBlock>();
		
		public FreeBlockList()
		{
			
		}
	}
}

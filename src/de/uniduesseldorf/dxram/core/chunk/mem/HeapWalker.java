package de.uniduesseldorf.dxram.core.chunk.mem;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Vector;

import de.uniduesseldorf.dxram.core.exceptions.MemoryException;

public class HeapWalker 
{	
	private HeapWalker()
	{
		
	}
	
	public static Results walk(final SmallObjectHeap p_heap)
	{
		Results results = new Results();
		
		results.m_heap = new Heap();
		results.m_heap.m_numSegments = p_heap.m_segments.length;
		results.m_heap.m_segmentSize = p_heap.m_segmentSize;
		results.m_heap.m_totalSize = p_heap.m_memory.getSize();
		
		for (SmallObjectHeapSegment segment : p_heap.m_segments)
		{
			results.m_segments.add(walkSegment(segment));
		}
		
		return results;
	}

	// ------------------------------------------------------------------------------------------------------------
	
	private static Segment walkSegment(final SmallObjectHeapSegment p_segment)
	{
		assert p_segment != null;
		
		Segment resultsSegment = new Segment();
		
		p_segment.lockManage();
		
		resultsSegment.m_segmentBlock = parseSegment(p_segment);
		resultsSegment.m_memoryBlocks = walkMemoryBlocks(p_segment);	
		resultsSegment.m_freeBlockLists = walkMemoryFreeBlockList(p_segment);
		
		p_segment.unlockManage();
		
		return resultsSegment;
	}
	
	private static SegmentBlock parseSegment(final SmallObjectHeapSegment p_segment)
	{
		SegmentBlock segmentBlock = new SegmentBlock();
		
		segmentBlock.m_startAddress = p_segment.m_base;
		segmentBlock.m_endAddress = p_segment.m_base + p_segment.m_fullSize;
		segmentBlock.m_sizeMemoryBlockArea = p_segment.m_size;
		segmentBlock.m_startAddressFreeBlocksList = p_segment.m_base + p_segment.m_size;
		segmentBlock.m_sizeFreeBlocksListArea = p_segment.m_fullSize - p_segment.m_size;
		
		return segmentBlock;
	}
		
	private static HashMap<Long, MemoryBlock> walkMemoryBlocks(final SmallObjectHeapSegment p_segment)
	{
		HashMap<Long, MemoryBlock> memoryBlocks = new HashMap<Long, MemoryBlock>();
		
		// get what we need from the segment
		long baseAddress = p_segment.m_base;
		long blockAreaSize = p_segment.m_size;
		
		// walk memory block area
		while (baseAddress < blockAreaSize - 1)
		{
			MemoryBlock block = new MemoryBlock();
			
			try {
				block.m_startAddress = baseAddress;
				block.m_markerByte = p_segment.readRightPartOfMarker(baseAddress);
				
				switch (block.m_markerByte)
				{
					// free memory less than 12 bytes
					case 0:
					{
						int lengthFieldSize = 1;
						// size includes length field
						int sizeBlock = (int) p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE, lengthFieldSize);
		
						// + 2 marker bytes
						block.m_endAddress = baseAddress + sizeBlock + SmallObjectHeapSegment.SIZE_MARKER_BYTE * 2;
						block.m_rawBlockSize = sizeBlock;
						block.m_customState = -1;
						block.m_prevFreeBlock = -1;
						block.m_nextFreeBlock = -1;
						block.m_blockPayloadSize = -1;
						
						memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize;
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
						long freeBlockSize = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE, lengthFieldSize);
						
						// + 2 marker bytes
						block.m_endAddress = baseAddress + freeBlockSize + SmallObjectHeapSegment.SIZE_MARKER_BYTE * 2;
						block.m_rawBlockSize = freeBlockSize;
						block.m_customState = -1;
						block.m_prevFreeBlock = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize, SmallObjectHeapSegment.POINTER_SIZE);
						block.m_nextFreeBlock = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize + SmallObjectHeapSegment.POINTER_SIZE, SmallObjectHeapSegment.POINTER_SIZE);
						block.m_blockPayloadSize = -1; // no payload
						
						memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += SmallObjectHeapSegment.SIZE_MARKER_BYTE + freeBlockSize;
						
						break;
					}
						
					// malloc'd block, 1 byte length field
					case 6:
					case 9:
					case 12:
					{
						int lengthFieldSize = 1;
						long blockPayloadSize = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE, lengthFieldSize);
						
						// + 2 marker bytes
						block.m_endAddress = baseAddress + lengthFieldSize + blockPayloadSize + SmallObjectHeapSegment.SIZE_MARKER_BYTE * 2;
						block.m_rawBlockSize = lengthFieldSize + blockPayloadSize;
						block.m_customState = p_segment.getCustomState(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE);
						block.m_prevFreeBlock = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize, SmallObjectHeapSegment.POINTER_SIZE);
						block.m_nextFreeBlock = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize + SmallObjectHeapSegment.POINTER_SIZE, SmallObjectHeapSegment.POINTER_SIZE);
						block.m_blockPayloadSize = blockPayloadSize;
						
						memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize + blockPayloadSize;
						
						break;
					}
					
					// malloc'd block, 2 byte length field
					case 7:
					case 10:
					case 13:
					{
						int lengthFieldSize = 2;
						long blockPayloadSize = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE, lengthFieldSize);
						
						// + 2 marker bytes
						block.m_endAddress = baseAddress + lengthFieldSize + blockPayloadSize + SmallObjectHeapSegment.SIZE_MARKER_BYTE * 2;
						block.m_rawBlockSize = lengthFieldSize + blockPayloadSize;
						block.m_customState = p_segment.getCustomState(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE);
						block.m_prevFreeBlock = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize, SmallObjectHeapSegment.POINTER_SIZE);
						block.m_nextFreeBlock = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize + SmallObjectHeapSegment.POINTER_SIZE, SmallObjectHeapSegment.POINTER_SIZE);
						block.m_blockPayloadSize = blockPayloadSize;
						
						memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize + blockPayloadSize;
						
						break;
					}
					
					// malloc'd block, 3 byte length field
					case 8:
					case 11:
					case 14:
					{
						int lengthFieldSize = 3;
						long blockPayloadSize = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE, lengthFieldSize);
						
						// + 2 marker bytes
						block.m_endAddress = baseAddress + lengthFieldSize + blockPayloadSize + SmallObjectHeapSegment.SIZE_MARKER_BYTE * 2;
						block.m_rawBlockSize = lengthFieldSize + blockPayloadSize;
						block.m_customState = p_segment.getCustomState(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE);
						block.m_prevFreeBlock = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize, SmallObjectHeapSegment.POINTER_SIZE);
						block.m_nextFreeBlock = p_segment.read(baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize + SmallObjectHeapSegment.POINTER_SIZE, SmallObjectHeapSegment.POINTER_SIZE);
						block.m_blockPayloadSize = blockPayloadSize;
						
						memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += SmallObjectHeapSegment.SIZE_MARKER_BYTE + lengthFieldSize + blockPayloadSize;
						
						break;
					}
					
					// free memory 1 byte
					case 15:
					{
						block.m_endAddress = baseAddress + SmallObjectHeapSegment.SIZE_MARKER_BYTE;
						block.m_rawBlockSize = 0;
						block.m_customState = -1;
						block.m_prevFreeBlock = -1;
						block.m_nextFreeBlock = -1;
						block.m_blockPayloadSize = 0;
						
						memoryBlocks.put(block.m_startAddress, block);
					
						// proceed
						baseAddress += SmallObjectHeapSegment.SIZE_MARKER_BYTE;
						
						break;
					}
					
					default:
					{
						System.out.println("!!! Block with invalid marker detected: ptr " + 
								block.m_startAddress + ", marker " + block.m_markerByte);
						break;
					}
				}			
			} catch (MemoryException e) {
				e.printStackTrace();
			}
		}
		
		return memoryBlocks;
	}
		
	private static Vector<FreeBlockList> walkMemoryFreeBlockList(final SmallObjectHeapSegment p_segment)
	{
		Vector<FreeBlockList> freeBlocklist = new Vector<FreeBlockList>();
		
		// get what we need from the segment
		long baseAddress = p_segment.m_baseFreeBlockList;
		long freeBlockListAreaSize = p_segment.m_fullSize - p_segment.m_size;
		long freeBLockListEnd = baseAddress + freeBlockListAreaSize;
		long[] freeBlockListSizes = p_segment.m_freeBlockListSizes;
		
		try {	
			for (int i = 0; i < freeBlockListSizes.length; i++)
			{
				FreeBlockList list = new FreeBlockList();
				freeBlocklist.add(list);
				list.m_minFreeBlockSize = freeBlockListSizes[i];
				list.m_addressRoot = baseAddress;
				
				if (baseAddress <= freeBLockListEnd)
				{
					long ptr = p_segment.readPointer(baseAddress);
	
					if (ptr != 0)
					{
						FreeBlock block;
						
						do
						{
							int marker = p_segment.readRightPartOfMarker(ptr - 1);
							
							// verify marker byte of memory block first
							switch (marker)
							{
								case 1:
								case 2:
								case 3:
								case 4:
								case 5:
								{
									int lengthFieldSize = marker;
									long blockSize = p_segment.read(ptr, marker);
									
									// sanity check block size
									if (blockSize < 12)
									{
										System.out.println("!!! Block size < 12 detected: ptr " + 
												ptr + ", marker " + marker + ", blockSize " + blockSize);
										break;
									}
									
									long ptrPrev = p_segment.read(ptr + lengthFieldSize, SmallObjectHeapSegment.POINTER_SIZE);
									long ptrNext = p_segment.read(ptr + lengthFieldSize + SmallObjectHeapSegment.POINTER_SIZE, SmallObjectHeapSegment.POINTER_SIZE);
									
									block = new FreeBlock();
									// have block position before the marker byte for the walker
									block.m_blockAddress = ptr - 1; 
									if (ptrPrev == 0)
										block.m_prevBlockAddress = -1;
									else
										block.m_prevBlockAddress = ptrPrev;
									if (ptrNext == 0)
										block.m_nextBlockAddress = -1;
									else
										block.m_nextBlockAddress = ptrNext;
									
									list.m_blocks.add(block);
									
									ptr = ptrNext;
									
									break;
								}
								
								default:
								{
									System.out.println("!!! Block with invalid marker detected: ptr " + 
											ptr + ", marker " + marker);
									break;
								}
							}
						}
						while (ptr != 0);
					}
					
					baseAddress += SmallObjectHeapSegment.POINTER_SIZE;
				}
			}
		} catch (MemoryException e) {
			e.printStackTrace();
		}
		
		return freeBlocklist;
	}
	
	public static class Results
	{
		public Heap m_heap = null;
		public Vector<Segment> m_segments = new Vector<Segment>();
		
		public Results()
		{
			
		}
		
		public void clearResults()
		{
			m_heap = null;
			m_segments.clear();
		}
		
		@Override
		public String toString()
		{
			StringBuilder output = new StringBuilder();
			
			output.append("Results HeapWalk:");	
			
			output.append("\n");
			output.append(m_heap);
			
			Iterator<Segment> it = m_segments.iterator();
			while (it.hasNext())
			{
				Segment segment = it.next();
				
				output.append("\n");
				output.append(segment);
			}
			
			return output.toString();
		}
	}
	
	public static class Heap
	{
		public long m_segmentSize = -1;
		public long m_numSegments = -1;
		public long m_totalSize = -1;
		
		public Heap()
		{
			
		}
		
		@Override
		public String toString()
		{
			return "Heap(m_segmentSize " + m_segmentSize + 
					", m_numSegments " + m_numSegments + 
					", m_totalSize " + m_totalSize + ")";
		}
	}
	
	public static class Segment
	{
		public SegmentBlock m_segmentBlock = null;
		public HashMap<Long, MemoryBlock> m_memoryBlocks = new HashMap<Long, MemoryBlock>();
		public Vector<FreeBlockList> m_freeBlockLists = new Vector<FreeBlockList>();
		
		public Segment()
		{
			
		}

		@Override
		public String toString()
		{
			StringBuilder output = new StringBuilder();
			
			output.append("Segments:");	
			
			output.append("\n");
			output.append(m_segmentBlock);
			
			Iterator<Entry<Long, MemoryBlock>> it = m_memoryBlocks.entrySet().iterator();
			while (it.hasNext())
			{
				Entry<Long, MemoryBlock> entry = it.next();
				
				output.append("\n");
				output.append(entry.getValue());
			}
			
			Iterator<FreeBlockList> it2 = m_freeBlockLists.iterator();
			while (it2.hasNext())
			{
				FreeBlockList list = it2.next();
				
				output.append("\n");
				output.append(list);
			}
			
			return output.toString();
		}
	}
	
	public static class SegmentBlock
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
		
	public static class MemoryBlock
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

	public static class FreeBlock
	{
		public long m_blockAddress = -1;
		public long m_prevBlockAddress = -1;
		public long m_nextBlockAddress = -1;
		
		public FreeBlock()
		{
			
		}
		
		@Override
		public String toString()
		{
			return "FreeBlock(m_blockAddress " + m_blockAddress + 
					", m_prevBlockAddress " + m_prevBlockAddress +
					", m_nextBlockAddress " + m_nextBlockAddress + ")";
		}
	}
	
	public static class FreeBlockList
	{
		public long m_minFreeBlockSize = -1;
		public long m_addressRoot = -1;
		public Vector<FreeBlock> m_blocks = new Vector<FreeBlock>();
		
		public FreeBlockList()
		{
			
		}
		
		@Override
		public String toString()
		{
			String str = new String("FreeBlockList(m_minFreeBlockSize " + m_minFreeBlockSize + 
					", m_addressRoot " + m_addressRoot + ")");
			
			Iterator<FreeBlock> it = m_blocks.iterator();
			while (it.hasNext())
			{
				FreeBlock block = it.next();
				
				str += "\n" + block;
			}
				
			return str;
		}
	}
}

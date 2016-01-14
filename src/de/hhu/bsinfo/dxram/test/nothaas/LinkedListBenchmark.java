package de.hhu.bsinfo.dxram.test.nothaas;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.utils.Stopwatch;

public class LinkedListBenchmark 
{
	private DXRAM m_dxram = null;
	private ChunkService m_chunkService = null;
	private Stopwatch m_stopwatch = new Stopwatch();
	
	public LinkedListBenchmark()
	{
		m_dxram = new DXRAM();
		m_dxram.initialize("config/dxram.conf");
		m_chunkService = m_dxram.getService(ChunkService.class);
	}
	
	public void run(int numItems)
	{
		System.out.println("Creating linked list with " + numItems + " items.");
		m_stopwatch.start();
		long listHead = createLinkedList(numItems);
		m_stopwatch.printAndStop();
		System.out.println("Done creating linked list.");
		
		System.out.println("Walking linked list, head " + listHead);
		m_stopwatch.start();
		long itemsTouched = walkLinkedList(listHead);
		m_stopwatch.printAndStop();
		System.out.println("Walking linked list done, total elements touched: " + itemsTouched);
		
		System.out.println("Done");
	}
	
	public static void main(String[] args)
	{
		if (args.length < 1)
		{
			System.out.println("Usage: LinkedListBenchmark <#elements list>");
			return;
		}
		
		int numElementsList = Integer.parseInt(args[0]);
		LinkedListBenchmark benchmark = new LinkedListBenchmark();
		benchmark.run(numElementsList);
	}
	
	public long createLinkedList(int numItems)
	{	
		Chunk[] chunks = new Chunk[numItems];
		long[] chunkIDs = m_chunkService.create(8, numItems);
		Chunk head = null;
		Chunk previousChunk = null;
		
		for (int i = 0; i < chunkIDs.length; i++)
		{
			chunks[i] = new Chunk(chunkIDs[i], 8);
			if (previousChunk == null)
			{
				// init head
				head = chunks[i];
				previousChunk = head;
			} else {
				previousChunk.getData().putLong(chunks[i].getID());
				previousChunk = chunks[i];
			}
		}
		
		// mark end
		chunks[chunks.length - 1].getData().putLong(-1);
		
		if (m_chunkService.put(chunks) != chunks.length)
		{
			System.out.println("Putting linked list failed.");
			return -1;
		}
		
		return head.getID();
	}
	
	public long walkLinkedList(long headChunkID)
	{	
		long counter = 0;
		Chunk chunk = new Chunk(headChunkID, 8);
		if (m_chunkService.get(chunk) != 1)
		{
			System.out.println("Getting head chunk if linked list failed.");
			return 0;
		}
		counter++;
		
		while (chunk != null)
		{
			long nextChunkID = chunk.getData().getLong();
			if (nextChunkID == -1)
				break;
			// reuse same chunk to avoid allocations
			chunk.setID(nextChunkID);
			m_chunkService.get(chunk);
			counter++;
		}
		
		return counter;
	}
}

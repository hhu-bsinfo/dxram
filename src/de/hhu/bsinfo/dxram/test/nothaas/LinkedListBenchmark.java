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
	}
	
	public static void main(String[] args)
	{
		if (args.length < 1)
		{
			System.out.println("Usage: LinkedListBenchmark <#elements list>");
			return;
		}
		
		long numElementsList = Long.parseLong(args[0]);
		

		
		System.out.println("Creating linked list with " + numElementsList + " items.");
		Stopwatch.start();
		long listHead = createLinkedList(numElementsList);
		Stopwatch.printAndStop();
		System.out.println("Done creating linked list.");
		
		System.out.println("Walking linked list, head " + listHead);
		Stopwatch.start();
		long itemsTouched = walkLinkedList(listHead);
		Stopwatch.printAndStop();
		System.out.println("Walking linked list done, total elements touched: " + itemsTouched);
		
		System.out.println("Done");
	}
	
	public Chunk createLinkedList(int numItems)
	{	
		Chunk[] chunks = new Chunk[numItems];
		long[] chunkIDs = m_chunkService.create(8, numItems);
		
		for ()
		
		for (long i = 0; i < numItems; i++)
		{
			Chunk chunk = Core.createNewChunk(8);
			
			// have previous chunk point to next/this one
			previousChunk.getData().putLong(chunk.getChunkID());
			Core.put(previousChunk);
			previousChunk = chunk;
		}
		
		previousChunk.getData().putLong(-1);
		Core.put(previousChunk);
		
		return head;
	}
	
	public long walkLinkedList(long headChunkID) throws DXRAMException
	{	
		long counter = 0;
		Chunk chunk = Core.get(headChunkID);
		
		while (chunk != null)
		{
			long nextChunkID = chunk.getData().getLong();
			if (nextChunkID == -1)
				break;
			chunk = Core.get(nextChunkID);
			counter++;
		}
		
		return counter;
	}
}

package de.uniduesseldorf.dxram.test.nothaas;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.utils.Stopwatch;

public class LinkedListBenchmark 
{
	public static void main(String[] args) throws DXRAMException
	{
		if (args.length < 1)
		{
			System.out.println("Usage: LinkedListBenchmark <#elements list>");
			return;
		}
		
		long numElementsList = Long.parseLong(args[0]);
		
		System.out.println("Starting Peer...");
		Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
				NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
		
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
	
	public static long createLinkedList(long numItems) throws DXRAMException
	{	
		// initial chunk
		long head = -1;
		Chunk previousChunk = Core.createNewChunk(8);
		head = previousChunk.getChunkID();
		
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
	
	public static long walkLinkedList(long headChunkID) throws DXRAMException
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

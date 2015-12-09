package de.uniduesseldorf.dxram.test.nothaas;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.nodeconfig.NodeID;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.utils.NameServiceStringConverter;

import de.uniduesseldorf.soh.SmallObjectHeap;
import de.uniduesseldorf.utils.Tools;
import de.uniduesseldorf.utils.config.ConfigurationHandler;

public class BFSPeer 
{
	public static void main(String[] p_arguments) throws DXRAMException
	{
		System.out.println("Starting BFS peer...");
		try {
			Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
					NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
		} catch (final DXRAMException e1) {
			e1.printStackTrace();
		}

//		System.out.println("BFS Peer started, ID: " + Core.getNodeID());
//
//		Chunk chunk = Core.createNewChunk(70);
//		for (int i = 0; i < 70; i++)
//			chunk.getData().array()[i] = (byte) 0xAA;
//		Chunk chunk2 = Core.createNewChunk(100);
//		for (int i = 0; i < 100; i++)
//			chunk2.getData().array()[i] = (byte) 0xBB;
//		for (int i = 0; i < 348; i++)
//		{
//			Core.put(chunk);
//			Core.put(chunk2);
//		}
//		Core.remove(chunk.getChunkID());
//		Core.remove(chunk2.getChunkID());
//		
//		System.out.println("done");
		
//		Chunk[] chunks = new Chunk[4000];
//		for (int i = 0; i < chunks.length; i++)
//		{
//			chunks[i] = Core.createNewChunk(68, "bla" + i);
//		}
//		
//		System.out.println("next");
//		
//		for (int i = 0; i < chunks.length; i++)
//		{
//			for (int j = 0; j < 100; j++)
//				Core.put(chunks[i]);
//		}
//		
//		System.out.println("next");
//		
//		for (int i = 0; i < chunks.length; i++)
//		{
//			Core.remove(chunks[i].getChunkID());
//		}
		
		System.out.println("done");
		
//		BFSPeer bfsPeer = new BFSPeer();
//		bfsPeer.start();
//		
//		System.out.println("Waiting...");
//		while (true)
//		{
//			try {
//				Thread.sleep(100000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
	}
	
	public BFSPeer()
	{
		
	}
	
	public void start()
	{
		try {
			createGraphFromEdgeListKernel("edge_list");
		} catch (IOException | DXRAMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// kernel 1
	// TODO time?
	private void createGraphFromEdgeListKernel(String filename) throws IOException, DXRAMException
	{
		final int EDGE_SIZE = 8 * 2;
		final int VERTEX_SIZE = 8;
		
		System.out.println("Creating graph from edge list " + filename);
		
		ByteBuffer buffer = ByteBuffer.allocate(EDGE_SIZE * 100);
		InputStream inFile = new FileInputStream(new File(filename));
		
		int totalBytesRead = 0;
		int bytesRead = -1;
		while (true)
		{			
			buffer.position(0);
			bytesRead = inFile.read(buffer.array());
			
			if (bytesRead == -1)
				break;
			
			totalBytesRead += bytesRead;
			
			if (bytesRead % EDGE_SIZE != 0)
			{
				// TODO exception/error
			}
			
			while (buffer.position() < bytesRead)
			{				
				System.out.println(totalBytesRead);
				System.out.println(buffer.position() + "/" + bytesRead);
				
				long nodeFrom = buffer.getLong();
				long nodeTo = buffer.getLong();
				
				// TODO nodeFrom as chunkID?
				Chunk chunkFrom = Core.get(Long.toString(nodeFrom));
				if (chunkFrom == null)
					chunkFrom = Core.createNewChunk(4 + VERTEX_SIZE * 8, Long.toString(nodeFrom)); // default alloc up to 8 vertices? XXX
				
				int nedges = chunkFrom.getData().getInt(0);
				// check if there is space for another edge
				if ((nedges + 1) * VERTEX_SIZE + 4 >= chunkFrom.getData().capacity())
				{
					// realloc
					// TODO go for size or speed i.e. block alloc or single item alloc?
					Chunk newChunk = Core.createNewChunk(chunkFrom.getData().capacity() + VERTEX_SIZE * 8, Long.toString(nodeFrom)); // grow another 8 vertices
					newChunk.getData().put(chunkFrom.getData());
					
					// remove old one
					Core.remove(chunkFrom.getChunkID());
						
					chunkFrom = newChunk;
				}	
					
				chunkFrom.getData().putInt(0, nedges + 1);
					
				// create new node if needed
				// TODO nodeTo as chunkID?
				Chunk chunkTo = Core.get(Long.toString(nodeTo));
				if (chunkTo == null)
					chunkTo = Core.createNewChunk(4 + VERTEX_SIZE * 8, Long.toString(nodeTo)); // XXX block alloc default?
					
				// add new node to the end, skip other nodes
				// TODO check if duped edge?
				chunkFrom.getData().putLong(4 + VERTEX_SIZE * nedges, chunkTo.getChunkID());
				
				Core.put(chunkFrom);
				Core.put(chunkTo);
			}
		}
		
		System.out.println("Creating graph from edge list done");
		
		inFile.close();
	}
	
	// kernel 2
	private void bfsKernel(String filename) throws IOException
	{
		final int VERTEX_SIZE = 8;
	
		long[] searchKeys = new long[64];
		
		{
			ByteBuffer buffer = ByteBuffer.allocate(VERTEX_SIZE * 64); // 64 search keys
			RandomAccessFile inFile = new RandomAccessFile(new File(filename), "r");
		
			for (int i = 0; i < searchKeys.length; i++)
			{
				searchKeys[i] = inFile.readLong();
			}
			
			inFile.close();
		}
		
		for (int i = 0; i < searchKeys.length; i++)
		{
			
		}
		
	}
	
	private void bfs(long startVertex)
	{
		
	}
}

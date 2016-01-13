package de.hhu.bsinfo.dxram.test.nothaas;

import java.io.IOException;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;

public class Test 
{
	public static void main(String[] args)
	{
		DXRAM dxram = new DXRAM();
		
		dxram.initialize("config/dxram.conf");
		
		ChunkService service = dxram.getService(ChunkService.class);
		
		int[] sizes = new int[] {100, 32, 432, 8};
		long[] chunkIDs = service.create(sizes);
		Chunk[] chunks = new Chunk[chunkIDs.length];
		Chunk[] chunksCopy = new Chunk[chunkIDs.length];
		for (int i = 0; i < chunkIDs.length; i++) {
			chunks[i] = new Chunk(chunkIDs[i], sizes[i]);
			chunksCopy[i] = new Chunk(chunkIDs[i], sizes[i]);
		}
		
		System.out.println("Chunks: ");
		for (int i = 0; i < chunkIDs.length; i++)
		{
			System.out.println(chunks[i]);
		}
		
		for (Chunk chunk : chunks)
		{
			chunk.getData().putLong(chunk.getID());
		}
	
		service.put(chunks);
		
		service.get(chunksCopy);
		
		System.out.println("Data got: ");
		for (Chunk chunk : chunksCopy)
		{
			System.out.println(Long.toHexString(chunk.getData().getLong()));
		}
		
		int removeCount = service.remove(chunks);
		System.out.println("Removed: " + removeCount);
		
//		chunkIDs = service.create((short) -27647, new int[] {155, 543, 99, 65, 233});
//		
//		for (int i = 0; i < chunkIDs.length; i++)
//		{
//			System.out.println(chunkIDs[i]);
//		}
		
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

package de.hhu.bsinfo.dxram.test.nothaas;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;

public class SimpleLocalChunkServiceTest 
{
	public DXRAM m_dxram;
	public ChunkService m_chunkService;

	public SimpleLocalChunkServiceTest()
	{
		DXRAM m_dxram = new DXRAM();
		m_dxram.initialize("config/dxram.conf");
		m_chunkService = m_dxram.getService(ChunkService.class);
	}
	
	public void run()
	{
		int[] sizes = new int[] {100, 32, 432, 8};
		long[] chunkIDs = m_chunkService.create(sizes);
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
		
//		for (Chunk chunk : chunks)
//		{
//			chunk.getData().putLong(chunk.getID());
//		}
//	
//		m_chunkService.put(chunks);
//		
//		m_chunkService.get(chunksCopy);
//		
//		System.out.println("Data got: ");
//		for (Chunk chunk : chunksCopy)
//		{
//			System.out.println(Long.toHexString(chunk.getData().getLong()));
//		}
		
		int removeCount = m_chunkService.remove(chunks);
		System.out.println("Removed: " + removeCount);
	}
	
	public static void main(String[] args)
	{
		SimpleLocalChunkServiceTest test = new SimpleLocalChunkServiceTest();
		test.run();
	}
}

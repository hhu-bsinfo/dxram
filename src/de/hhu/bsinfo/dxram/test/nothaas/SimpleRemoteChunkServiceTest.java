package de.hhu.bsinfo.dxram.test.nothaas;

import java.io.IOException;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;

public class SimpleRemoteChunkServiceTest 
{
	public DXRAM m_dxram;
	public ChunkService m_chunkService;

	public SimpleRemoteChunkServiceTest()
	{
		DXRAM m_dxram = new DXRAM();
		m_dxram.initialize("config/dxram.conf");
		m_chunkService = m_dxram.getService(ChunkService.class);
	}
	
	public void run(final short p_remotePeerID)
	{
		int[] sizes = new int[] {155, 543, 99, 65, 233};
		long[] chunkIDs = m_chunkService.create(p_remotePeerID, sizes);
		if (chunkIDs == null)
		{
			System.out.println("Creating chunks failed.");
			return;
		}
		Chunk[] chunks = new Chunk[chunkIDs.length];
		Chunk[] chunksCopy = new Chunk[chunkIDs.length];
		for (int i = 0; i < chunkIDs.length; i++) {
			chunks[i] = new Chunk(chunkIDs[i], sizes[i]);
			chunksCopy[i] = new Chunk(chunkIDs[i], sizes[i]);
		}
		
		for (int i = 0; i < chunkIDs.length; i++)
		{
			System.out.println(chunkIDs[i]);
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
		
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args)
	{
		if (args.length < 1)
		{
			System.out.println("Usage: SimpleRemoteChunkServiceTest <remote peer ID>");
			return;
		}
		
		short remotePeerID = Short.parseShort(args[0]);
		
		SimpleRemoteChunkServiceTest test = new SimpleRemoteChunkServiceTest();
		test.run(remotePeerID);
	}
}

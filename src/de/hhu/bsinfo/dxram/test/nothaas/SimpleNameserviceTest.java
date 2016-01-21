package de.hhu.bsinfo.dxram.test.nothaas;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;

public class SimpleNameserviceTest {
	public DXRAM m_dxram;
	public ChunkService m_chunkService;
	public NameserviceService m_nameserviceService;

	public SimpleNameserviceTest()
	{
		DXRAM m_dxram = new DXRAM();
		m_dxram.initialize("config/dxram.conf");
		m_chunkService = m_dxram.getService(ChunkService.class);
		m_nameserviceService = m_dxram.getService(NameserviceService.class);
	}
	
	public void run()
	{
		final int size = 76;
		final int chunkCount = 100;
		Chunk[] chunks = new Chunk[chunkCount];
		int[] sizes = new int[chunkCount];
		for (int i = 0; i < sizes.length; i++) {
			sizes[i] = size;
		}
		
		System.out.println("Creating chunks...");
		long[] chunkIDs = m_chunkService.create(sizes);
		
		for (int i = 0; i < chunks.length; i++) {
			chunks[i] = new Chunk(chunkIDs[i], sizes[i]);
		}
		
		Map<Long, String> verificationMap = new HashMap<Long, String>();
		
		System.out.println("Registering " + chunkCount + " chunks...");
		for (int i = 0; i < chunks.length; i++) {
			String name = new String("Entry_" + i);
			m_nameserviceService.register(chunks[i], name);
			verificationMap.put(chunks[i].getID(), name);
		}
		
		System.out.println("Getting chunk IDs from name service...");
		for (Entry<Long, String> entry : verificationMap.entrySet())
		{
			long chunkID = m_nameserviceService.getChunkID(entry.getValue());
			if (chunkID != entry.getKey().longValue())
			{
				System.out.println("ChunkID from name service (" + Long.toHexString(entry.getKey()) + 
						") not matching original chunk ID (" + chunkID + "), for name " + entry.getValue() + "."); 
			}
		}
		
		System.out.println("Removing chunks from name service...");
		for (Chunk chunk : chunks)
		{
			m_nameserviceService.remove(chunk);
		}
		
		System.out.println("Done.");
	}
	
	public static void main(String[] args)
	{
		SimpleNameserviceTest test = new SimpleNameserviceTest();
		test.run();
	}
}

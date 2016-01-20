package de.hhu.bsinfo.dxram.test.nothaas;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;

// before running this as a peer, start a superpeer and an additional storage peer
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
		System.out.println("Creating remote chunks...");
		long[] chunkIDs = m_chunkService.create(p_remotePeerID, sizes);
		if (chunkIDs == null) {
			System.out.println("Creating remote chunks failed.");
			return;
		}
		Chunk[] chunks = new Chunk[chunkIDs.length];
		Chunk[] chunksCopy = new Chunk[chunkIDs.length];
		for (int i = 0; i < chunkIDs.length; i++) {
			chunks[i] = new Chunk(chunkIDs[i], sizes[i]);
			chunksCopy[i] = new Chunk(chunkIDs[i], sizes[i]);
		}
		
		System.out.println("Remote chunks created: ");
		for (int i = 0; i < chunkIDs.length; i++) {
			System.out.println(chunkIDs[i]);
		}
		
		System.out.println("Setting chunk payload...");
		for (Chunk chunk : chunks) {
			System.out.println(Long.toHexString(chunk.getID()) + ": " + Long.toHexString(chunk.getID()));
			chunk.getData().putLong(chunk.getID());
		}
	
		System.out.println("Putting chunks...");
		int ret = m_chunkService.put(chunks);
		System.out.println("Putting chunks results: " + ret);
		
		System.out.println("Getting chunks...");
		ret = m_chunkService.get(chunksCopy);
		System.out.println("Getting chunks restults: " + ret);
		
		System.out.println("Data got: ");
		for (Chunk chunk : chunksCopy) {
			System.out.println(Long.toHexString(chunk.getData().getLong()));
		}
		
		System.out.println("Removing chunks...");
		int removeCount = m_chunkService.remove(chunks);
		System.out.println("Removed chunks: " + removeCount);
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

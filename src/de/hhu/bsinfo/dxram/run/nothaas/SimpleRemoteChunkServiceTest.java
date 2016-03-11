package de.hhu.bsinfo.dxram.run.nothaas;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.Main;

// before running this as a peer, start a superpeer and an additional storage peer
public class SimpleRemoteChunkServiceTest extends Main
{
	public static final Argument ARG_REMOTE_PEER_ID = new Argument("remotePeerID", "-15999", true, "NodeID of the remote peer to create chunks on");
	
	private DXRAM m_dxram;
	private ChunkService m_chunkService;

	public static void main(final String[] args) {
		Main main = new SimpleRemoteChunkServiceTest();
		main.run(args);
	}
	
	public SimpleRemoteChunkServiceTest()
	{
		super("Test creating chunks on a remote peer");
		
		m_dxram = new DXRAM();
		m_dxram.initialize("config/dxram.conf");
		m_chunkService = m_dxram.getService(ChunkService.class);
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(ARG_REMOTE_PEER_ID);
	}

	@Override
	protected int main(ArgumentList p_arguments) {
		final short remotePeerID = (short) (p_arguments.getArgument(ARG_REMOTE_PEER_ID).getValue(Short.class) & 0xFFFF);
		
		int[] sizes = new int[] {155, 543, 99, 65, 233};
		System.out.println("Creating remote chunks...");
		long[] chunkIDs = m_chunkService.create(remotePeerID, sizes);
		if (chunkIDs == null) {
			System.out.println("Creating remote chunks failed.");
			return -1;
		}
		Chunk[] chunks = new Chunk[chunkIDs.length];
		Chunk[] chunksCopy = new Chunk[chunkIDs.length];
		for (int i = 0; i < chunkIDs.length; i++) {
			chunks[i] = new Chunk(chunkIDs[i], sizes[i]);
			chunksCopy[i] = new Chunk(chunkIDs[i], sizes[i]);
		}
		
		System.out.println("Remote chunks created: ");
		for (int i = 0; i < chunkIDs.length; i++) {
			System.out.println(Long.toHexString(chunkIDs[i]));
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
		return 0;
	}
}

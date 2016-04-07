package de.hhu.bsinfo.dxram.run.nothaas;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.run.DXRAMMain;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.Main;

/**
 * Small test/benchmark to measure execution time of the core methods
 * of ChunkService using the built in StatisticsService.
 * Run this as a peer, start one superpeer.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 *
 */
public class ChunkServiceBenchmarkPeer extends DXRAMMain {
	
	private static final Argument ARG_NUM_CHUNKS = new Argument("numChunks", "1000", true, "Total number of chunks involved in the test");
	private static final Argument ARG_BATCHES = new Argument("batches", "10", true, "Split the total number of chunks into a number of batches for get and put operations");
	private static final Argument ARG_CHUNK_SIZE = new Argument("chunkSize", "128", true, "Size of a chunk in bytes");
	
	/**
	 * Java main entry point.
	 * @param args Main arguments.
	 */
	public static void main(final String[] args) {
		Main main = new ChunkServiceBenchmarkPeer();
		main.run(args);
	}
	
	/**
	 * Constructor
	 */
	protected ChunkServiceBenchmarkPeer() {

	}

	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		super.registerDefaultProgramArguments(p_arguments);
		p_arguments.setArgument(ARG_NUM_CHUNKS);
		p_arguments.setArgument(ARG_BATCHES);
		p_arguments.setArgument(ARG_CHUNK_SIZE);
	}

	@Override
	protected int mainApplication(ArgumentList p_arguments) 
	{
		int numChunks = p_arguments.getArgument(ARG_NUM_CHUNKS).getValue(Integer.class);
		int numBatches = p_arguments.getArgument(ARG_BATCHES).getValue(Integer.class);
		int chunkSize = p_arguments.getArgument(ARG_CHUNK_SIZE).getValue(Integer.class);
		
		ChunkService chunkService = getService(ChunkService.class);
		StatisticsService statisticsService = getService(StatisticsService.class);
		

		Chunk[] chunks = new Chunk[numChunks];
		for (int i = 0; i < chunks.length; i++) {
			chunks[i] = new Chunk(chunkSize);
		}
		
		if (chunkService.create(chunks) != chunks.length)
		{
			System.out.println("ERROR: Creating chunks failed.");
			return -1;
		}

		int batchSize = numChunks / numBatches;
		for (int i = 0; i < numBatches; i++)
		{
			if (chunkService.put(ChunkLockOperation.NO_LOCK_OPERATION, chunks, batchSize * i, batchSize) != batchSize)
			{
				System.out.println("ERROR: Putting chunks failed.");
				return -2;
			}
			
			if (chunkService.get(chunks, batchSize * i, batchSize) != batchSize)
			{
				System.out.println("ERROR: Getting chunks failed.");
				return -3;
			}
		}
		
		if (chunkService.remove(chunks) != chunks.length)
		{
			System.out.println("ERROR: Removing chunks failed.");
			return -4;
		}

		statisticsService.printStatistics();
	
		return 0;
	}
}

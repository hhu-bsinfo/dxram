package de.hhu.bsinfo.dxram.run.nothaas;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.run.DXRAMMain;
import de.hhu.bsinfo.dxram.stats.StatisticsService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.Main;

public class ChunkServiceBenchmarkPeer extends DXRAMMain {
	
	public static void main(final String[] args) {
		Main main = new ChunkServiceBenchmarkPeer();
		main.run(args);
	}
	
	protected ChunkServiceBenchmarkPeer() {
		//super("Benchmarks the ChunkService's core methods.");
	}

	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected int mainApplication(ArgumentList p_arguments) 
	{
		ChunkService chunkService = getService(ChunkService.class);
		StatisticsService statisticsService = getService(StatisticsService.class);
		
		// TODO main argument setable
		Chunk[] chunks = new Chunk[1000];
		for (int i = 0; i < chunks.length; i++) {
			chunks[i] = new Chunk(128);
		}
		
		// TODO also have setable batch count for one call i.e. 100 chunks -> batch of 10
		
		if (chunkService.create(chunks) != chunks.length)
		{
			System.out.println("ERROR: Creating chunks failed.");
			return -1;
		}
		
		if (chunkService.put(chunks) != chunks.length)
		{
			System.out.println("ERROR: Putting chunks failed.");
			return -2;
		}
		
		if (chunkService.get(chunks) != chunks.length)
		{
			System.out.println("ERROR: Getting chunks failed.");
			return -3;
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

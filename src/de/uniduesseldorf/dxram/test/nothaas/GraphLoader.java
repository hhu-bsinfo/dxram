package de.uniduesseldorf.dxram.test.nothaas;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.storage.CIDTable;
import de.uniduesseldorf.dxram.core.chunk.storage.RawMemory;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.utils.Pair;

public class GraphLoader implements Runnable
{
	public static void main(String[] args) throws DXRAMException
	{
		if (args.length < 1)
		{
			System.out.println("Usage: GraphLoader <edge list files> ...");
			return;
		}
		
		System.out.println("Starting Peer...");
		Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
				NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
		
		Vector<Thread> loaderThreads = new Vector<Thread>();
		
		RawMemory rawMemory = new RawMemory();
		rawMemory.initialize(1024 * 1024 * 32); // XXX 32 mb enough?
		CIDTable nodeMappingTable = new CIDTable();
		nodeMappingTable.initialize(rawMemory);
		
		// start a new thread for every edge list file
		for (int i = 0; i < args.length; i++)
		{
			System.out.println("Starting loader instance for '" + args[i] + "'.");
			GraphImporter importer = createInstance();
			if (!importer.setEdgeInputFile(new File(args[i])))
			{
				System.out.println("Opening input file '" + args[i] + "' failed.");
				continue;
			}
			
			importer.setMappingTable(nodeMappingTable);
			
			GraphLoader loader = new GraphLoader(importer);
			loader.run();
			//Thread thread = new Thread(loader);
			//thread.start();
			//loaderThreads.add(thread);
		}
		
		System.out.println("All loader instances started.");
//		
//		for (Thread thread : loaderThreads)
//		{
//			try {
//				thread.join();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
		
		System.out.println("All threads fininshed.");
		
		Core.close();
		System.out.println("Done");
	}
	
	public static GraphImporter createInstance(/** TODO parameter to select **/)
	{
		// TODO hardcoded for now...
		return new GraphImporterMappingTable();
	}
	
	// ------------------------------------------------------------------------------------------------
	
	private GraphImporter m_importer;
	
	public GraphLoader(GraphImporter importer)
	{
		m_importer = importer;
	}
	
	@Override
	public void run() 
	{
		try 
		{
			while (true)
			{
				List<Pair<Long, Long>> edges = m_importer.readEdges(100);
				if (edges.isEmpty())
					break; // nothing left to read
				
				Iterator<Pair<Long, Long>> it = edges.iterator();
				while (it.hasNext())
				{
					long chunkIDFrom = Chunk.INVALID_CHUNKID;
					long chunkIDTo = Chunk.INVALID_CHUNKID;
					
					Pair<Long, Long> pair = it.next();
					
					// target node
					chunkIDTo = m_importer.getChunkIDForNode(pair.second());
					// only check if exists and put back new chunk
					if (chunkIDTo == Chunk.INVALID_CHUNKID)
					{
						Chunk chunkTo = Core.createNewChunk(Integer.BYTES);
						chunkTo.getData().putInt(0, 0); // 0 edges
						Core.put(chunkTo);
						chunkIDTo = chunkTo.getChunkID();
						m_importer.setChunkIDForNode(pair.second(), chunkIDTo);
					}
					
					// source node
					chunkIDFrom = m_importer.getChunkIDForNode(pair.first());
					Chunk chunkFrom = null;
					if (chunkIDFrom == Chunk.INVALID_CHUNKID)
					{
						chunkFrom = Core.createNewChunk(Integer.BYTES);
						chunkFrom.getData().putInt(0, 0); // 0 edges
						chunkIDFrom = chunkFrom.getChunkID();
						m_importer.setChunkIDForNode(pair.first(), chunkIDFrom);
					}
					else
						chunkFrom = Core.get(chunkIDFrom);
					
					// add target node/edge
					{						
						// realloc bigger chunk
						Core.remove(chunkIDFrom);
						Chunk reallocedChunk = Core.createNewChunk(chunkFrom.getData().capacity() + Long.BYTES);
						reallocedChunk.getData().put(chunkFrom.getData());
						reallocedChunk.getData().putLong(chunkFrom.getData().capacity(), chunkIDTo);
						// increase outgoing edges count
						int outgoingEdges = reallocedChunk.getData().getInt(0);
						reallocedChunk.getData().putInt(0, outgoingEdges + 1);
						
						chunkFrom = reallocedChunk;
					}
					
					Core.put(chunkFrom);
				}
			}
		} catch (DXRAMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

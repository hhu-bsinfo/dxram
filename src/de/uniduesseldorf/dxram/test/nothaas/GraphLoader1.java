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
import de.uniduesseldorf.dxram.core.chunk.storage.PagingTable;
import de.uniduesseldorf.dxram.core.chunk.storage.RawMemory;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.utils.Pair;

public class GraphLoader1 implements Runnable
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
			
			GraphLoader1 loader = new GraphLoader1(importer);
			Thread thread = new Thread(loader);
			thread.start();
			loaderThreads.add(thread);
		}
		
		System.out.println("All loader instances started.");
		
		for (Thread thread : loaderThreads)
		{
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
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
	
	public GraphLoader1(GraphImporter importer)
	{
		m_importer = importer;
	}
	

}

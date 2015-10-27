package de.uniduesseldorf.dxram.test.nothaas.glp;

import java.io.File;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

public class GraphLoaderTool 
{
	public static void main(String[] args) throws DXRAMException
	{
		if (args.length < 1)
		{
			System.out.println("Usage: GraphLoaderTool <edge list files> ...");
			return;
		}
		
		System.out.println("Starting Peer...");
		Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
				NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
		
		// TODO hardcoded thread count
		final int threadCount = 4;
		GraphLoader loader = new GraphLoader(threadCount);
		loader.setNodeMapping(new NodeMappingPagingInMemory());
		
		// add same amount of importers as threads by default
		for (int i = 0; i < threadCount; i++)
		{
			loader.addImporter(new GraphImporterSimple());
		}

		for (int i = 0; i < args.length; i++)
		{
			System.out.println("Adding edge list file '" + args[i] + "'.");
			
			loader.addReader(new GraphReaderFile(new File(args[i])));
		}
		
		System.out.println("Execute loading....");
		if (!loader.execute())
		{
			System.out.println("Executing failed.");
		}

		System.out.println("All fininshed.");
		
		Core.close();
		System.out.println("Done");
	}
}

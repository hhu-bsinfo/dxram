package de.uniduesseldorf.dxram.test.nothaas.glp;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Vector;

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
			System.out.println("Usage: GraphLoaderTool --entryNodes <entry nodes list> <edge list files> ...");
			return;
		}
		
		System.out.println("Starting Peer...");
		Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
				NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
		
		// TODO hardcoded thread count
		final int threadCount = 8;
		GraphLoader loader = new GraphLoader(threadCount);
		loader.setNodeMapping(new NodeMappingPagingInMemory());
		
		// add same amount of importers as threads by default
		for (int i = 0; i < threadCount; i++)
		{
			loader.addImporter(new GraphImporterSimple());
		}

		String entryNodes = null;
		int i = 0;
		if (args.length > 2 && args[0].equals("--entryNodes"))
		{
			entryNodes = args[1];
			i = 2;
		}
		
		while (i < args.length)
		{
			System.out.println("Adding edge list file '" + args[i] + "'.");
			
			loader.addReader(new GraphReaderFile(new File(args[i])));
			i++;
		}
		
		System.out.println("Execute loading....");
		if (!loader.execute())
		{
			System.out.println("Executing failed.");
		}

		System.out.println("All fininshed.");
		
		if (entryNodes != null)
		{
			Vector<Long> entryNodesConverted = new Vector<Long>();
			RandomAccessFile entryNodesFile = null;
			try {
				entryNodesFile = new RandomAccessFile(new File(entryNodes), "r");
				while (entryNodesFile.getFilePointer() < entryNodesFile.length())
					entryNodesConverted.add(loader.getNodeMapping().getChunkIDForNodeID(entryNodesFile.readLong()));
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			System.out.println("EntryNodes: ");
			for (Long node : entryNodesConverted)
				System.out.println(node);
		}
		
		//Core.close();
		System.out.println("Done");
	}
}

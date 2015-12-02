package de.uniduesseldorf.dxgraph.load.old;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Vector;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.utils.Pair;

import de.uniduesseldorf.dxgraph.load.GraphEdgeReaderFile;
import de.uniduesseldorf.dxgraph.load.NodeMappingHashMap;

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
		final int threadCount = 1;
		GraphLoader loader = new GraphLoader(threadCount);
		//loader.setNodeMapping(new NodeMappingPagingInMemory());
		loader.setNodeMapping(new NodeMappingHashMap());
		
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
			
			loader.addReader(new GraphEdgeReaderFile(new File(args[i])));
			i++;
		}
		
		System.out.println("Execute loading....");
		if (!loader.execute())
		{
			System.out.println("Executing failed.");
		}

		System.out.println("All fininshed.");
		
		System.out.println("Mapping table entries: " + loader.getNodeMapping().getNumMappingEntries());
		
		// dump entries
		{
			try {
				RandomAccessFile mappingsDump = new RandomAccessFile(new File("nodeMappings"), "rw");

				Iterator<Pair<Long, Long>> it = loader.getNodeMapping().getIterator();
				while (it.hasNext())
				{
					Pair<Long, Long> elem = it.next();
					mappingsDump.writeLong(elem.first());
					mappingsDump.writeLong(elem.second());
				}
				
				mappingsDump.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if (entryNodes != null)
		{
			Vector<Long> entryNodesConverted = new Vector<Long>();
			RandomAccessFile entryNodesFile = null;
			try {
				entryNodesFile = new RandomAccessFile(new File(entryNodes), "r");
				System.out.println("EntryNodes: ");
				while (entryNodesFile.getFilePointer() < entryNodesFile.length())
				{
					Long node = entryNodesFile.readLong();
					Long chunk = loader.getNodeMapping().getChunkIDForNodeID(node);
					entryNodesConverted.add(chunk);
					System.out.println(node + " -> " + chunk);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		//Core.close();
		System.out.println("Done");
	}
}

package de.hhu.bsinfo.dxgraph.load;

import de.hhu.bsinfo.dxcompute.Task;
import de.hhu.bsinfo.dxcompute.logger.LOG_LEVEL;
import de.hhu.bsinfo.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class TaskLoadGraphEdgeList extends Task
{
	public TaskLoadGraphEdgeList()
	{
		super("LoadGraphEdgeList");
	}
	
	@Override
	protected Object execute(final Object p_arg) 
	{
		if (!(p_arg instanceof Pair<?, ?>))
		{
			log(LOG_LEVEL.LL_ERROR, "Parameter type for task invalid.");
			setExitCode(-1);
			return null;
		}
		
		@SuppressWarnings("unchecked")
		Pair<String, String> fileNames = (Pair<String, String>) p_arg;
		
		NodeMapping nodeMapping = new NodeMappingHashMap();
		
		File file = new File(fileNames.first());
		if (!file.exists())
		{
			log(LOG_LEVEL.LL_ERROR, "Edge list file " + file + " does not exist.");
			setExitCode(-2);
			return null;
		}
			
		GraphEdgeReader edgeReader;
		try {
			edgeReader = new GraphEdgeReaderFile(file);
		} catch (IOException e) {
			log(LOG_LEVEL.LL_ERROR, "Opening file " + file + " for graph edge reader failed.");
			setExitCode(-3);
			return null;
		}
		
		// TODO configurable?
		for (int i = 0; i < 8; i++)
		{
			getTaskDelegate().submitJob(new JobLoadEdges(edgeReader, nodeMapping));
		}
		
		getTaskDelegate().waitForSubmittedJobsToFinish();
		
		return readEntryVerticesFile(fileNames.second(), nodeMapping);
	}
	
	private long[] readEntryVerticesFile(final String p_filename, final NodeMapping p_nodeMapping)
	{
		File file = new File(p_filename);
		if (!file.exists())
		{
			log(LOG_LEVEL.LL_ERROR, "Edge list entry vertices file " + file + " does not exist.");
			setExitCode(-4);
			return null;
		}
		
		int totalNumberOfVertices = 0;
		ByteBuffer buffer = null;
		try {
			RandomAccessFile raFile = new RandomAccessFile(file, "r");
			totalNumberOfVertices = (int) (raFile.length() / 8);
			buffer = ByteBuffer.allocate((int) (totalNumberOfVertices * 8));
			
			int readBytes = raFile.read(buffer.array());
			raFile.close();
			if (readBytes == -1)
			{
				log(LOG_LEVEL.LL_ERROR, "Reading file " + file + " for graph edge reader failed.");
				setExitCode(-5);
				return null;
			}
		} catch (IOException e) {
			log(LOG_LEVEL.LL_ERROR, "Opening file " + file + " for graph edge reader failed.");
			setExitCode(-6);
			return null;
		}
		
		buffer.position(0);
		int nonInvalidNodeCount = 0;
		long[] vertices = new long[totalNumberOfVertices];
		for (int i = 0; i < totalNumberOfVertices; i++)
		{
			vertices[i] = p_nodeMapping.getChunkIDForNodeID(buffer.getLong());
			if (vertices[i] != -1)
				nonInvalidNodeCount++;
		}
		
		if (nonInvalidNodeCount != vertices.length)
		{
			log(LOG_LEVEL.LL_WARNING, "Entry node set contains vertices, that are not found: "
					+ (vertices.length - nonInvalidNodeCount) + "/" + vertices.length 
					+ " vertices not found in loaded graph.");
		}
		
		// filter invalid nodes
		long[] nonInvalidVertices = new long[nonInvalidNodeCount];
		int counter = 0;
		for (long vertex : vertices)
		{
			if (vertex != -1)
			{
				nonInvalidVertices[counter] = vertex;
				counter++;
			}
		}
		return nonInvalidVertices;
	}

}

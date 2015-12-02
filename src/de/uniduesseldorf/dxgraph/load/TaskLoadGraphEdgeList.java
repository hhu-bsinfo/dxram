package de.uniduesseldorf.dxgraph.load;

import de.uniduesseldorf.dxcompute.Task;
import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;

import java.io.File;

import de.uniduesseldorf.dxram.utils.Pair;

public class TaskLoadGraphEdgeList extends Task
{
	public TaskLoadGraphEdgeList()
	{
		
	}
	
	@Override
	protected Object execute(final Object p_arg) 
	{
		Pair<String, String> fileNames = (Pair<String, String>) p_arg;
		String edgeListFileName = fileNames.first();
		String entryNodesFileName = fileNames.second();
		
		NodeMapping nodeMapping = new NodeMappingHashMap();
		
		File file = new File(fileNames.first());
		if (!file.exists())
		{
			getTaskInterface().log(LOG_LEVEL.LL_ERROR, "Edge list file " + file + " does not exist.");
			return null;
		}
			
		GraphEdgeReader edgeReader = new GraphEdgeReaderFile(file);
		
		// TODO configurable?
		for (int i = 0; i < 8; i++)
		{
			getTaskInterface().submitJob(new JobLoadEdges(edgeReader, nodeMapping));
		}
		
		// TODO wait for all jobs to complete
		
		return null;
	}

}

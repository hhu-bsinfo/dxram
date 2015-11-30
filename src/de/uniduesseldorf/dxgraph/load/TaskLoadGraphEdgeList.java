package de.uniduesseldorf.dxgraph.load;

import de.uniduesseldorf.dxcompute.Task;
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
		
		
		
		return null;
	}

}

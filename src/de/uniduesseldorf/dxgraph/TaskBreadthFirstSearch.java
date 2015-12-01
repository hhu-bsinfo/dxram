package de.uniduesseldorf.dxgraph;

import de.uniduesseldorf.dxcompute.Task;

public class TaskBreadthFirstSearch extends Task 
{
	public TaskBreadthFirstSearch()
	{
		
	}

	@Override
	public Object execute(Object p_arg) 
	{
		long[] entryPoints;
		SimpleVertex[] entryVertices;
		int resultVertCount;
		
		entryPoints = (long[]) p_arg;
		entryVertices = new SimpleVertex[entryPoints.length];
		
		for (int i = 0; i < entryPoints.length; i++)
		{
			entryVertices[i] = new SimpleVertex(entryPoints[i]);	
		}
		
		resultVertCount = getStorageInterface().get(entryVertices);
		if (resultVertCount != entryVertices.length)
		{
			// TODO error
			return null;
		}
		
		for (SimpleVertex vertex : entryVertices)
		{
			getTaskInterface().submitJob(new JobBreadthFirstSearchNaive(vertex, -1));
		}
		
		// TODO wait until no more jobs are queued and all queued jobs are done processing
		
		return null;
	}
}

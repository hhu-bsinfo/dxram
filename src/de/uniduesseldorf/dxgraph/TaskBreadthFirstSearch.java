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
		
		entryPoints = (long[]) p_arg;
		
		for (long entryPoint : entryPoints)
		{
			getTaskInterface().submitJob(new JobBreadthFirstSearchNaive(entryPoint, -1));
		}
		
		// TODO wait until no more jobs are queued and all queued jobs are done processing
		
		return null;
	}
}

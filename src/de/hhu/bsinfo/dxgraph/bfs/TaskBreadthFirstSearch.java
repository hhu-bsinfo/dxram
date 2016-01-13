package de.hhu.bsinfo.dxgraph.bfs;

import de.hhu.bsinfo.dxcompute.Task;
import de.hhu.bsinfo.dxcompute.logger.LOG_LEVEL;
import de.hhu.bsinfo.dxgraph.data.SimpleVertex;

public class TaskBreadthFirstSearch extends Task 
{
	public TaskBreadthFirstSearch()
	{
		super("BreadthFirstSearch");
	}

	@Override
	public Object execute(final Object p_arg) 
	{
		long[] entryPoints;
		SimpleVertex[] entryVertices;
		int resultVertCount;
		
		if (!(p_arg instanceof long[]))
		{
			log(LOG_LEVEL.LL_ERROR, "Invalid type parameters entry points.");
			setExitCode(-1);
			return null;
		}
			
		entryPoints = (long[]) p_arg;
		entryVertices = new SimpleVertex[entryPoints.length];
		
		for (int i = 0; i < entryPoints.length; i++)
		{
			entryVertices[i] = new SimpleVertex(entryPoints[i]);	
		}
		
		resultVertCount = getStorageDelegate().get(entryVertices);
		if (resultVertCount != entryVertices.length)
		{
			log(LOG_LEVEL.LL_WARNING, "Missing vertices in loaded graph, entry vertices count " 
					+ entryVertices.length + " found vertices in graph " + resultVertCount);
		}
		
		for (SimpleVertex vertex : entryVertices)
		{
			getTaskDelegate().submitJob(new JobBreadthFirstSearchNaive(vertex, -1));
		}
		
		getTaskDelegate().waitForSubmittedJobsToFinish();
		
		return null;
	}
}

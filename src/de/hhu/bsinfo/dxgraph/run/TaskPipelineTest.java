package de.hhu.bsinfo.dxgraph.run;

import de.hhu.bsinfo.dxgraph.GraphTaskPipeline;
import de.hhu.bsinfo.dxgraph.algo.GraphAlgorithm;
import de.hhu.bsinfo.dxgraph.algo.GraphAlgorithmBFS;
import de.hhu.bsinfo.dxgraph.load.GraphLoader;
import de.hhu.bsinfo.dxgraph.load.GraphLoaderOrderedEdgeList;

public class TaskPipelineTest extends GraphTaskPipeline {

	public TaskPipelineTest()
	{
		
	}
	
	@Override
	public boolean setup() 
	{
		GraphLoader loader = new GraphLoaderOrderedEdgeList();
		loader.setPath("vneighbourlist.txt");
		pushTask(loader);
		GraphAlgorithm algorithm = new GraphAlgorithmBFS();
		algorithm.setEntryNodes(0xc181000000000001L);
		pushTask(algorithm);
		
		return true;
	}
}

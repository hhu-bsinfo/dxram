package de.hhu.bsinfo.dxgraph.pipe;

import de.hhu.bsinfo.dxgraph.GraphTaskPipeline;
import de.hhu.bsinfo.dxgraph.algo.GraphAlgorithm;
import de.hhu.bsinfo.dxgraph.algo.GraphAlgorithmBFS;
import de.hhu.bsinfo.dxgraph.load.GraphLoader;
import de.hhu.bsinfo.dxgraph.load.oel.GraphLoaderOrderedEdgeListLocal;
import de.hhu.bsinfo.dxram.data.ChunkID;

public class TestPipeline1 extends GraphTaskPipeline {

	@Override
	public boolean setup() 
	{
		recordTaskStatistics(true);
		
		pushTask(new GraphLoaderOrderedEdgeListLocal("graph", 1, 100));
		pushTask(new GraphAlgorithmBFS(1, ChunkID.getChunkID(m_bootService.getNodeID(), 1)));
		
		return true;
	}
}

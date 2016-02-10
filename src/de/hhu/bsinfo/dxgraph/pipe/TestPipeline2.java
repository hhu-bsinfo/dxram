package de.hhu.bsinfo.dxgraph.pipe;

import de.hhu.bsinfo.dxgraph.GraphTaskPipeline;
import de.hhu.bsinfo.dxgraph.algo.GraphAlgorithm;
import de.hhu.bsinfo.dxgraph.algo.GraphAlgorithmBFS;
import de.hhu.bsinfo.dxgraph.load.oel.GraphLoaderOrderedEdgeListMultiNode;
import de.hhu.bsinfo.dxram.data.ChunkID;

public abstract class TestPipeline2 extends GraphTaskPipeline {

	public abstract boolean isMaster();
	
	@Override
	public boolean setup() {
		GraphLoaderOrderedEdgeListMultiNode loader = new GraphLoaderOrderedEdgeListMultiNode();
		loader.setPath("graph");
		loader.setNumNodes(2);
		loader.setMasterLoader(isMaster());
		pushTask(loader);
		
		GraphAlgorithm algorithm = new GraphAlgorithmBFS();
		algorithm.setEntryNodes(ChunkID.getChunkID(m_bootService.getNodeID(), 1));
		pushTask(algorithm);
		
		return true;
	}

	public static class Master extends TestPipeline2 
	{
		@Override
		public boolean isMaster() {
			return true;
		}
	}
	
	public static class Slave extends TestPipeline2 
	{
		@Override
		public boolean isMaster() {
			return false;
		}
	}
}

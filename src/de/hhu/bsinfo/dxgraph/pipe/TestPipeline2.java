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
		if (isMaster()) {
			loader.setPath("graph/master");
		} else {
			loader.setPath("graph/slave");
		}
		loader.setNumNodes(2);
		loader.setMasterLoader(isMaster());
		pushTask(loader);
		
		// TODO different entry nodes for master/slaves? -> use node ID for that?
		// only have master run the computation for now
		// TODO have GraphAlgorithmBFS which runs job remotely if the vertex is not on the current node
		if (isMaster()) {
			GraphAlgorithm algorithm = new GraphAlgorithmBFS();
			algorithm.setEntryNodes(ChunkID.getChunkID(m_bootService.getNodeID(), 1));
			pushTask(algorithm);
		}
		
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

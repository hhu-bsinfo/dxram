package de.hhu.bsinfo.dxgraph.pipe;

import de.hhu.bsinfo.dxgraph.GraphTaskPipeline;
import de.hhu.bsinfo.dxgraph.algo.GraphAlgorithmBFS;
import de.hhu.bsinfo.dxgraph.coord.SyncBarrierMaster;
import de.hhu.bsinfo.dxgraph.coord.SyncBarrierSlave;
import de.hhu.bsinfo.dxgraph.load.oel.GraphLoaderOrderedEdgeListMultiNode;
import de.hhu.bsinfo.dxram.data.ChunkID;

public abstract class TestPipeline2 extends GraphTaskPipeline {

	public abstract boolean isMaster();
	
	@Override
	public boolean setup() {
		final int numNodes = 2;
		final int numSlaves = numNodes - 1;
		
		recordTaskStatistics(true);
		
		if (isMaster()) {
			pushTask(new GraphLoaderOrderedEdgeListMultiNode("graph/master", numNodes, 100, true));
		} else {
			pushTask(new GraphLoaderOrderedEdgeListMultiNode("graph/slave", numNodes, 100, false));
		}		
		
		if (isMaster()) {
			pushTask(new SyncBarrierMaster(numSlaves, 3000));
		} else {
			pushTask(new SyncBarrierSlave());
		}
		
		// TODO different entry nodes for master/slaves? -> use node ID for that?
		// only have master run the computation for now
		// TODO have GraphAlgorithmBFS which runs job remotely if the vertex is not on the current node
		if (isMaster()) {
			pushTask(new GraphAlgorithmBFS(1, ChunkID.getChunkID(m_bootService.getNodeID(), 1)));
		}
		
		if (isMaster()) {
			pushTask(new SyncBarrierMaster(numSlaves, 3000));
		} else {
			pushTask(new SyncBarrierSlave());
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

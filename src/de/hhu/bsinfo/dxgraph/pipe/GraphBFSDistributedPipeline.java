package de.hhu.bsinfo.dxgraph.pipe;

import de.hhu.bsinfo.dxcompute.Pipeline;
import de.hhu.bsinfo.dxcompute.coord.SyncBarrierMaster;
import de.hhu.bsinfo.dxcompute.coord.SyncBarrierSlave;
import de.hhu.bsinfo.dxgraph.algo.GraphAlgorithmBFS;
import de.hhu.bsinfo.dxgraph.load.oel.GraphLoaderOrderedEdgeListMultiNode;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;

public abstract class GraphBFSDistributedPipeline extends Pipeline {

	protected static final Pair<String, String> ARG_GRAPH_LOAD_DATA_PATH = new Pair<String, String>("graphLoadDataPath", "."); 
	protected static final Pair<String, Integer> ARG_NODE_COUNT = new Pair<String, Integer>("nodeCount", 1); 
	protected static final Pair<String, Integer> ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE = new Pair<String, Integer>("graphLoadVertexBatchSize", 100);
	
	protected static final Pair<String, Integer> ARG_GRAPH_BFS_NODE_COUNT_PER_JOB = new Pair<String, Integer>("graphBfsNodeCountPerJob", 100); 
	protected static final Pair<String, Long> ARG_GRAPH_BFS_ENTRY_NODE = new Pair<String, Long>("graphBfsEntryNode", 0L); 
	
	@Override
	public boolean setup(final ArgumentList p_arguments) {		
		recordTaskStatistics(true);
		
		final String graphLoadDataPath = p_arguments.getArgument(ARG_GRAPH_LOAD_DATA_PATH);
		final int nodeCount = p_arguments.getArgument(ARG_NODE_COUNT);
		final int graphLoadVertexBatchSize = p_arguments.getArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE);
		final int graphBfsNodeCountPerJob = p_arguments.getArgument(ARG_GRAPH_BFS_NODE_COUNT_PER_JOB);
		final long graphBfsEntryNode = p_arguments.getArgument(ARG_GRAPH_BFS_ENTRY_NODE);
		
		final int numSlaves = nodeCount - 1;
		
		if (isMaster()) {
			pushTask(new GraphLoaderOrderedEdgeListMultiNode(graphLoadDataPath, nodeCount, graphLoadVertexBatchSize, true));
		} else {
			pushTask(new GraphLoaderOrderedEdgeListMultiNode(graphLoadDataPath, nodeCount, graphLoadVertexBatchSize, false));
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
			pushTask(new GraphAlgorithmBFS(graphBfsNodeCountPerJob, graphBfsEntryNode));
		}
		
		if (isMaster()) {
			pushTask(new SyncBarrierMaster(numSlaves, 3000));
		} else {
			pushTask(new SyncBarrierSlave());
		}
		
		return true;
	}
	
	protected GraphBFSDistributedPipeline(final String p_graphLoadDataPath, final int p_nodeCount, 
			final int p_graphLoadVertexBatchSize, final int p_graphBfsNodeCountPerJob, final long p_graphBfsEntryNode)
	{
		m_arguments.setArgument(ARG_GRAPH_LOAD_DATA_PATH, p_graphLoadDataPath);
		m_arguments.setArgument(ARG_NODE_COUNT, p_nodeCount);
		m_arguments.setArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE, p_graphLoadVertexBatchSize);
		
		m_arguments.setArgument(ARG_GRAPH_BFS_NODE_COUNT_PER_JOB, p_graphBfsNodeCountPerJob);
		m_arguments.setArgument(ARG_GRAPH_BFS_ENTRY_NODE, p_graphBfsEntryNode);
	}
	
	protected abstract boolean isMaster();

	public static class Master extends GraphBFSDistributedPipeline 
	{
		public Master(String p_graphLoadDataPath, int p_nodeCount, int p_graphLoadVertexBatchSize,
				int p_graphBfsNodeCountPerJob, long p_graphBfsEntryNode) {
			super(p_graphLoadDataPath, p_nodeCount, p_graphLoadVertexBatchSize, p_graphBfsNodeCountPerJob, p_graphBfsEntryNode);
		}

		@Override
		public boolean isMaster() {
			return true;
		}
	}
	
	public static class Slave extends GraphBFSDistributedPipeline 
	{		
		public Slave(String p_graphLoadDataPath, int p_nodeCount, int p_graphLoadVertexBatchSize,
				int p_graphBfsNodeCountPerJob, long p_graphBfsEntryNode) {
			super(p_graphLoadDataPath, p_nodeCount, p_graphLoadVertexBatchSize, p_graphBfsNodeCountPerJob, p_graphBfsEntryNode);
		}

		@Override
		public boolean isMaster() {
			return false;
		}
	}
}

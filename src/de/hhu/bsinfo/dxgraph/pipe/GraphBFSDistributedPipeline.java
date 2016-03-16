package de.hhu.bsinfo.dxgraph.pipe;

import de.hhu.bsinfo.dxcompute.Pipeline;
import de.hhu.bsinfo.dxcompute.coord.SyncBarrierMaster;
import de.hhu.bsinfo.dxcompute.coord.SyncBarrierSlave;
import de.hhu.bsinfo.dxcompute.stats.PrintMemoryStatusToConsoleTask;
import de.hhu.bsinfo.dxcompute.stats.PrintStatisticsToConsoleTask;
import de.hhu.bsinfo.dxgraph.load.oel.GraphLoaderOrderedEdgeListMultiNode;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public abstract class GraphBFSDistributedPipeline extends Pipeline {

	protected static final Argument ARG_GRAPH_LOAD_DATA_PATH = new Argument("graphLoadDataPath", ".", true, "Path containing graph data files to laod"); 
	protected static final Argument ARG_NODE_COUNT = new Argument("nodeCount", "2", true, "Total number of nodes involved (master and slaves)"); 
	protected static final Argument ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE = new Argument("graphLoadVertexBatchSize", "100", true, "Batch size for loading vertices from the file");
	
	protected static final Argument ARG_GRAPH_BFS_NODE_COUNT_PER_JOB = new Argument("graphBfsNodeCountPerJob", "100", true, "Number of nodes to process within a single BFS job"); 
	protected static final Argument ARG_GRAPH_BFS_ENTRY_NODE_LOCAL = new Argument("graphBfsEntryNodeLocal", null, true, "Local ID of entry node for BFS algorithm"); 
	protected static final Argument ARG_GRAPH_BFS_COUNT_VERTS_SINGLE_THREAD = new Argument("graphBfsCountVertsSingleThread", "false", true, "Instead of running the normal BFS algorithm, run a single threaded version which counts the vertices (used to determine size of connected graph)"); 
	
	@Override
	public boolean setup(final ArgumentList p_arguments) {		
		recordTaskStatistics(true);
		
		final String graphLoadDataPath = p_arguments.getArgument(ARG_GRAPH_LOAD_DATA_PATH).getValue(String.class);
		final int nodeCount = p_arguments.getArgument(ARG_NODE_COUNT).getValue(Integer.class);
		final int graphLoadVertexBatchSize = p_arguments.getArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE).getValue(Integer.class);
		final int graphBfsNodeCountPerJob = p_arguments.getArgument(ARG_GRAPH_BFS_NODE_COUNT_PER_JOB).getValue(Integer.class);
		final Long graphBfsEntryNodeLocal = p_arguments.getArgument(ARG_GRAPH_BFS_ENTRY_NODE_LOCAL).getValue(Long.class);
		final boolean graphBfsCountVertsSingleThread = p_arguments.getArgument(ARG_GRAPH_BFS_COUNT_VERTS_SINGLE_THREAD).getValue(Boolean.class);
		
		final int numSlaves = nodeCount - 1;
		
		if (isMaster()) {
			pushTask(new GraphLoaderOrderedEdgeListMultiNode(graphLoadDataPath, nodeCount, graphLoadVertexBatchSize, true));
		} else {
			pushTask(new GraphLoaderOrderedEdgeListMultiNode(graphLoadDataPath, nodeCount, graphLoadVertexBatchSize, false));
		}		
		
		pushTask(new PrintMemoryStatusToConsoleTask());
		pushTask(new PrintStatisticsToConsoleTask());
		
		if (isMaster()) {
			pushTask(new SyncBarrierMaster(numSlaves, 3000, 1));
		} else {
			pushTask(new SyncBarrierSlave(1));
		}
		
		// TODO fix this later
		
		if (!graphBfsCountVertsSingleThread)
		{
//			// run algorithm on both master and slave(s) if entry node provided
//			if (graphBfsEntryNodeLocal != null) {
//				pushTask(new GraphAlgorithmBFS3(graphBfsNodeCountPerJob, ChunkID.getChunkID(m_bootService.getNodeID(), graphBfsEntryNodeLocal)));
//			}
		}
		else
		{
//			if (isMaster()) {
//				if (graphBfsEntryNodeLocal != null) {
//					pushTask(new GraphAlgorithmBFSSingleThread(graphBfsNodeCountPerJob, ChunkID.getChunkID(m_bootService.getNodeID(), graphBfsEntryNodeLocal)));
//				}	
//			}
		}
		
		if (isMaster()) {
			pushTask(new SyncBarrierMaster(numSlaves, 3000, 2));
		} else {
			pushTask(new SyncBarrierSlave(2));
		}
		
		pushTask(new PrintStatisticsToConsoleTask());
		
		return true;
	}
	
	protected GraphBFSDistributedPipeline(final ArgumentList p_arguments)
	{
		super(p_arguments);
	}
	
	protected GraphBFSDistributedPipeline(final String p_graphLoadDataPath, final int p_nodeCount, 
			final int p_graphLoadVertexBatchSize, final int p_graphBfsNodeCountPerJob, final long p_graphBfsEntryNodeLocal)
	{
		m_arguments.setArgument(ARG_GRAPH_LOAD_DATA_PATH, p_graphLoadDataPath);
		m_arguments.setArgument(ARG_NODE_COUNT, p_nodeCount);
		m_arguments.setArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE, p_graphLoadVertexBatchSize);
		
		m_arguments.setArgument(ARG_GRAPH_BFS_NODE_COUNT_PER_JOB, p_graphBfsNodeCountPerJob);
		m_arguments.setArgument(ARG_GRAPH_BFS_ENTRY_NODE_LOCAL, p_graphBfsEntryNodeLocal);
	}
	
	protected abstract boolean isMaster();

	public static class Master extends GraphBFSDistributedPipeline 
	{
		public Master(final ArgumentList p_arguments)
		{
			super(p_arguments);
		}
		
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
		public Slave(final ArgumentList p_arguments)
		{
			super(p_arguments);
		}
		
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

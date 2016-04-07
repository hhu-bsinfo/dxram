package de.hhu.bsinfo.dxgraph.pipe;


import de.hhu.bsinfo.dxcompute.Pipeline;
import de.hhu.bsinfo.dxcompute.coord.SyncBarrierMasterTask;
import de.hhu.bsinfo.dxcompute.coord.SyncBarrierSlaveTask;
import de.hhu.bsinfo.dxcompute.stats.PrintMemoryStatusToConsoleTask;
import de.hhu.bsinfo.dxcompute.stats.PrintStatisticsToConsoleTask;
import de.hhu.bsinfo.dxgraph.algo.bfs.GraphAlgorithmBFSDistMultiThreaded;
import de.hhu.bsinfo.dxgraph.load.GraphLoader;
import de.hhu.bsinfo.dxgraph.load.oel.GraphLoaderOrderedEdgeListMultiNode;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public abstract class GraphBFSDistributedPipeline extends Pipeline {

	protected static final Argument ARG_GRAPH_LOAD_DATA_PATH = new Argument("graphLoadDataPath", ".", true, "Path containing graph data files to laod"); 
	protected static final Argument ARG_NODE_COUNT = new Argument("nodeCount", "2", true, "Total number of nodes involved (master and slaves)"); 
	
	protected static final Argument ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE = new Argument("graphLoadVertexBatchSize", "100", true, "Batch size for loading vertices from the file");
	
	protected static final Argument ARG_GRAPH_BFS_THREAD_COUNT = new Argument("graphBfsThreadCount", "1", true, "Total number of threads for BFS per node"); 
	protected static final Argument ARG_GRAPH_BFS_VERTEX_BATCH_SIZE_THREAD = new Argument("graphBfsVertexBatchSizeThread", "100", true, "Number of vertices per thread to be batch processed"); 
	protected static final Argument ARG_GRAPH_BFS_MSG_VERTEX_BATCH_SIZE = new Argument("graphBfsMsgVertexBatchSize", "100", true, "Batch size for sending vertices over the net");
	
	protected static final Argument ARG_GRAPH_BFS_ENTRY_NODE_LOCAL = new Argument("graphBfsEntryNodeLocal", null, true, "Local ID of entry node for BFS algorithm"); 
	
	@Override
	public boolean setup(final ArgumentList p_arguments) {		
		recordTaskStatistics(true);
		
		final String graphLoadDataPath = p_arguments.getArgument(ARG_GRAPH_LOAD_DATA_PATH).getValue(String.class);
		final int nodeCount = p_arguments.getArgument(ARG_NODE_COUNT).getValue(Integer.class);
		
		final int graphLoadVertexBatchSize = p_arguments.getArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE).getValue(Integer.class);
		
		final int graphBfsThreadCount = p_arguments.getArgument(ARG_GRAPH_BFS_THREAD_COUNT).getValue(Integer.class);
		final int graphBfsVertexBatchSizeThread = p_arguments.getArgument(ARG_GRAPH_BFS_VERTEX_BATCH_SIZE_THREAD).getValue(Integer.class);
		final int graphBfsMsgVertexBatchSize = p_arguments.getArgument(ARG_GRAPH_BFS_MSG_VERTEX_BATCH_SIZE).getValue(Integer.class);
		
		final Long graphBfsEntryNodeLocal = p_arguments.getArgument(ARG_GRAPH_BFS_ENTRY_NODE_LOCAL).getValue(Long.class);
		
	
		final int numSlaves = nodeCount - 1;

		// ----------------------------------

		GraphLoader loader = new GraphLoaderOrderedEdgeListMultiNode(graphLoadDataPath, nodeCount, graphLoadVertexBatchSize, isMaster());
		pushTask(loader);
		pushTask(new PrintMemoryStatusToConsoleTask());
		pushTask(new PrintStatisticsToConsoleTask());
		
		// ----------------------------------
		
		int numSlavesBfs = numSlaves;
		if (!isMaster()) {
			numSlavesBfs = 0;
		}
		
		if (graphBfsEntryNodeLocal != null) {
			pushTask(new GraphAlgorithmBFSDistMultiThreaded(
					graphBfsVertexBatchSizeThread, 
					graphBfsMsgVertexBatchSize, 
					numSlavesBfs, 
					1000, 
					2, 
					graphBfsThreadCount, 
					loader, 
					ChunkID.getChunkID(m_bootService.getNodeID(), graphBfsEntryNodeLocal)));
		} else {
			pushTask(new GraphAlgorithmBFSDistMultiThreaded(
					graphBfsVertexBatchSizeThread, 
					graphBfsMsgVertexBatchSize, 
					numSlavesBfs, 
					1000, 
					2, 
					graphBfsThreadCount, 
					loader));
		}
		
		// ----------------------------------
		
		pushTask(new PrintStatisticsToConsoleTask());
		
		return true;
	}
	
	protected GraphBFSDistributedPipeline(final ArgumentList p_arguments)
	{
		super(p_arguments);
	}
	
	protected GraphBFSDistributedPipeline(
			final String p_graphLoadDataPath, 
			final int p_nodeCount, 
			final int p_graphLoadVertexBatchSize,
			final int p_bfsThreadCount,
			final int p_bfsVertexBatchSizePerThread,
			final int p_bfsMsgVertexBatchSize,
			final String p_bfsFrontier,
			final long p_bfsEntryNodeLocalId)
	{
		m_arguments.setArgument(ARG_GRAPH_LOAD_DATA_PATH, p_graphLoadDataPath);
		m_arguments.setArgument(ARG_NODE_COUNT, p_nodeCount);
		
		m_arguments.setArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE, p_graphLoadVertexBatchSize);
		
		m_arguments.setArgument(ARG_GRAPH_BFS_THREAD_COUNT, p_bfsThreadCount);
		m_arguments.setArgument(ARG_GRAPH_BFS_VERTEX_BATCH_SIZE_THREAD, p_bfsVertexBatchSizePerThread);
		m_arguments.setArgument(ARG_GRAPH_BFS_MSG_VERTEX_BATCH_SIZE, p_bfsMsgVertexBatchSize);
		
		m_arguments.setArgument(ARG_GRAPH_BFS_ENTRY_NODE_LOCAL, p_bfsEntryNodeLocalId);		
	}
	
	protected abstract boolean isMaster();

	public static class Master extends GraphBFSDistributedPipeline 
	{
		public Master(final ArgumentList p_arguments)
		{
			super(p_arguments);
		}
		
		public Master(
				final String p_graphLoadDataPath, 
				final int p_nodeCount, 
				final int p_graphLoadVertexBatchSize,
				final int p_bfsThreadCount,
				final int p_bfsVertexBatchSizePerThread,
				final int p_bfsMsgVertexBatchSize,
				final String p_bfsFrontier,
				final long p_bfsEntryNodeLocalId) {
			super(p_graphLoadDataPath, p_nodeCount, p_graphLoadVertexBatchSize, p_bfsThreadCount, 
					p_bfsVertexBatchSizePerThread, p_bfsMsgVertexBatchSize, p_bfsFrontier, p_bfsEntryNodeLocalId);
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
		
		public Slave(
				final String p_graphLoadDataPath, 
				final int p_nodeCount, 
				final int p_graphLoadVertexBatchSize,
				final int p_bfsThreadCount,
				final int p_bfsVertexBatchSizePerThread,
				final int p_bfsMsgVertexBatchSize,
				final String p_bfsFrontier,
				final long p_bfsEntryNodeLocalId) {
			super(p_graphLoadDataPath, p_nodeCount, p_graphLoadVertexBatchSize, p_bfsThreadCount, 
					p_bfsVertexBatchSizePerThread, p_bfsMsgVertexBatchSize, p_bfsFrontier, p_bfsEntryNodeLocalId);
		}

		@Override
		public boolean isMaster() {
			return false;
		}
	}
}

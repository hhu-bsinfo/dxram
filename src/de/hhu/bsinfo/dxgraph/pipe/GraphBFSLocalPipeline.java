package de.hhu.bsinfo.dxgraph.pipe;

import de.hhu.bsinfo.dxcompute.Pipeline;
import de.hhu.bsinfo.dxcompute.stats.PrintMemoryStatusToConsoleTask;
import de.hhu.bsinfo.dxcompute.stats.PrintStatisticsToConsoleTask;
import de.hhu.bsinfo.dxgraph.algo.GraphAlgorithmBFS;
import de.hhu.bsinfo.dxgraph.algo.GraphAlgorithmBFS3;
import de.hhu.bsinfo.dxgraph.load.oel.GraphLoaderOrderedEdgeListLocal;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public class GraphBFSLocalPipeline extends Pipeline {

	protected static final Argument ARG_GRAPH_LOAD_DATA_PATH = new Argument("graphLoadDataPath", ".", true, "Path containing graph data files to laod"); 
	protected static final Argument ARG_NODE_COUNT = new Argument("nodeCount", 1, true, "Total number of nodes involved (master and slaves)"); 
	protected static final Argument ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE = new Argument("graphLoadVertexBatchSize", 100, true, "Batch size for loading vertices from the file");
	
	protected static final Argument ARG_GRAPH_BFS_NODE_COUNT_PER_JOB = new Argument("graphBfsNodeCountPerJob", 100, true, "Number of nodes to process within a single BFS job"); 
	protected static final Argument ARG_GRAPH_BFS_ENTRY_NODE_LOCAL = new Argument("graphBfsEntryNodeLocal", null, false, "Local ID of entry node for BFS algorithm"); 
	
	public GraphBFSLocalPipeline(final ArgumentList p_arguments)
	{
		super(p_arguments);
	}
	
	public GraphBFSLocalPipeline(final String p_graphLoadDataPath, final int p_nodeCount, 
			final int p_graphLoadVertexBatchSize, final int p_graphBfsNodeCountPerJob, final long p_graphBfsEntryNodeLocal)
	{
		m_arguments.setArgument(ARG_GRAPH_LOAD_DATA_PATH, p_graphLoadDataPath);
		m_arguments.setArgument(ARG_NODE_COUNT, p_nodeCount);
		m_arguments.setArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE, p_graphLoadVertexBatchSize);
		
		m_arguments.setArgument(ARG_GRAPH_BFS_NODE_COUNT_PER_JOB, p_graphBfsNodeCountPerJob);
		m_arguments.setArgument(ARG_GRAPH_BFS_ENTRY_NODE_LOCAL, p_graphBfsEntryNodeLocal);
	}

	@Override
	public boolean setup(final ArgumentList p_arguments) 
	{
		recordTaskStatistics(true);
	
		final String graphLoadDataPath = p_arguments.getArgument(ARG_GRAPH_LOAD_DATA_PATH).getValue(String.class);
		final int nodeCount = p_arguments.getArgument(ARG_NODE_COUNT).getValue(Integer.class);
		final int graphLoadVertexBatchSize = p_arguments.getArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE).getValue(Integer.class);
		final int graphBfsNodeCountPerJob = p_arguments.getArgument(ARG_GRAPH_BFS_NODE_COUNT_PER_JOB).getValue(Integer.class);
		final long graphBfsEntryNodeLocal = p_arguments.getArgument(ARG_GRAPH_BFS_ENTRY_NODE_LOCAL).getValue(Long.class);
		
		pushTask(new GraphLoaderOrderedEdgeListLocal(graphLoadDataPath, nodeCount, graphLoadVertexBatchSize));
		pushTask(new PrintMemoryStatusToConsoleTask());
		pushTask(new GraphAlgorithmBFS3(graphBfsNodeCountPerJob, ChunkID.getChunkID(m_bootService.getNodeID(), graphBfsEntryNodeLocal)));
		pushTask(new PrintStatisticsToConsoleTask());
		
		return true;
	}
}

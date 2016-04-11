package de.hhu.bsinfo.dxgraph.pipe;

import de.hhu.bsinfo.dxcompute.old.Pipeline;
import de.hhu.bsinfo.dxcompute.stats.PrintMemoryStatusToConsoleTask;
import de.hhu.bsinfo.dxcompute.stats.PrintStatisticsToConsoleTask;
import de.hhu.bsinfo.dxgraph.algo.bfs.GraphAlgorithmBFSLocalMultiThreaded;
import de.hhu.bsinfo.dxgraph.algo.bfs.GraphAlgorithmBFSLocalMultiThreaded2;
import de.hhu.bsinfo.dxgraph.algo.bfs.GraphAlgorithmBFSLocalMultiThreaded3;
import de.hhu.bsinfo.dxgraph.algo.bfs.GraphAlgorithmBFSLocalSingleThreaded;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.FrontierList;
import de.hhu.bsinfo.dxgraph.load.GraphLoader;
import de.hhu.bsinfo.dxgraph.load.oel.GraphLoaderOrderedEdgeListLocal;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public class GraphBFSLocalPipeline extends Pipeline {

	protected static final Argument ARG_GRAPH_LOAD_DATA_PATH = new Argument("graphLoadDataPath", ".", true, "Path containing graph data files to laod"); 
	protected static final Argument ARG_GRAPH_BFS_THREAD_COUNT = new Argument("bfsThreadCount", "1", true, "Total number of threads for BFS"); 
	protected static final Argument ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE = new Argument("graphLoadVertexBatchSize", "100", true, "Batch size for loading vertices from the file");
	protected static final Argument ARG_GRAPH_BFS_FRONTIER = new Argument("graphLoadBfsFrontier", "BitVector", true, "Data structure used for the frontiers (available on single thread only)");
	protected static final Argument ARG_GRAPH_BFS_VERTEX_BATCH_SIZE_THREAD = new Argument("graphBfsVertexBatchSizeThread", "100", true, "Number of vertices per thread to be batch processed"); 
	protected static final Argument ARG_GRAPH_BFS_ENTRY_NODE_LOCAL = new Argument("graphBfsEntryNodeLocal", null, true, "Local ID of entry node for BFS algorithm"); 
	
	public GraphBFSLocalPipeline(final ArgumentList p_arguments)
	{
		super(p_arguments);
	}
	
	public GraphBFSLocalPipeline(final String p_graphLoadDataPath, final int p_threadCount, 
			final int p_graphLoadVertexBatchSize, final Class<? extends FrontierList> p_frontierClass, int p_graphBfsVertexBatchSizeThread, final long p_graphBfsEntryNodeLocal)
	{
		m_arguments.setArgument(ARG_GRAPH_LOAD_DATA_PATH, p_graphLoadDataPath);
		m_arguments.setArgument(ARG_GRAPH_BFS_THREAD_COUNT, p_threadCount);
		m_arguments.setArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE, p_graphLoadVertexBatchSize);
		m_arguments.setArgument(ARG_GRAPH_BFS_FRONTIER, p_frontierClass.getName());
		m_arguments.setArgument(ARG_GRAPH_BFS_VERTEX_BATCH_SIZE_THREAD, p_graphBfsVertexBatchSizeThread);
		m_arguments.setArgument(ARG_GRAPH_BFS_ENTRY_NODE_LOCAL, p_graphBfsEntryNodeLocal);
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean setup(final ArgumentList p_arguments) 
	{
		recordTaskStatistics(true);
	
		final String graphLoadDataPath = p_arguments.getArgument(ARG_GRAPH_LOAD_DATA_PATH).getValue(String.class);
		final int threadCount = p_arguments.getArgument(ARG_GRAPH_BFS_THREAD_COUNT).getValue(Integer.class);
		final int graphLoadVertexBatchSize = p_arguments.getArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE).getValue(Integer.class);
		final String graphBfsFrontier = p_arguments.getArgument(ARG_GRAPH_BFS_FRONTIER).getValue(String.class);
		final int graphBfsVertexBatchCount = p_arguments.getArgument(ARG_GRAPH_BFS_VERTEX_BATCH_SIZE_THREAD).getValue(Integer.class);
		Long graphBfsEntryNodeLocal = p_arguments.getArgument(ARG_GRAPH_BFS_ENTRY_NODE_LOCAL).getValue(Long.class);
		
		Class<? extends FrontierList> clazz = null;
		try {
			if (graphBfsFrontier != null) {
				clazz = (Class<? extends FrontierList>) Class.forName("de.hhu.bsinfo.dxgraph.algo.bfs.front." + graphBfsFrontier);
			}
		} catch (ClassNotFoundException e) {
			m_loggerService.error(getClass(), "Cannot find class of frontier: " + graphBfsFrontier);
			return false;
		}
		
		GraphLoader loader = new GraphLoaderOrderedEdgeListLocal(graphLoadDataPath, 1, graphLoadVertexBatchSize);
		pushTask(loader);
		pushTask(new PrintMemoryStatusToConsoleTask());
		if (threadCount > 1) {
			if (graphBfsEntryNodeLocal != null) {
				pushTask(new GraphAlgorithmBFSLocalMultiThreaded3(graphBfsVertexBatchCount, threadCount, loader, ChunkID.getChunkID(m_bootService.getNodeID(), graphBfsEntryNodeLocal)));
			} else {
				pushTask(new GraphAlgorithmBFSLocalMultiThreaded3(graphBfsVertexBatchCount, threadCount, loader));
			}
			
		} else {
			if (graphBfsEntryNodeLocal != null) {
				pushTask(new GraphAlgorithmBFSLocalSingleThreaded(graphBfsVertexBatchCount, clazz, loader, ChunkID.getChunkID(m_bootService.getNodeID(), graphBfsEntryNodeLocal)));
			} else {
				pushTask(new GraphAlgorithmBFSLocalSingleThreaded(graphBfsVertexBatchCount, clazz, loader));
			}
		}
		
		pushTask(new PrintStatisticsToConsoleTask());
		
		return true;
	}
}

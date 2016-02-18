package de.hhu.bsinfo.dxgraph.pipe;

import de.hhu.bsinfo.dxcompute.Pipeline;
import de.hhu.bsinfo.dxgraph.algo.GraphAlgorithmBFS;
import de.hhu.bsinfo.dxgraph.load.oel.GraphLoaderOrderedEdgeListLocal;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class GraphBFSLocalPipeline extends Pipeline {

	private static final Pair<String, String> ARG_GRAPH_LOAD_DATA_PATH = new Pair<String, String>("graphLoadDataPath", "."); 
	private static final Pair<String, Integer> ARG_NODE_COUNT = new Pair<String, Integer>("nodeCount", 1); 
	private static final Pair<String, Integer> ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE = new Pair<String, Integer>("graphLoadVertexBatchSize", 100); 
	
	private static final Pair<String, Integer> ARG_GRAPH_BFS_NODE_COUNT_PER_JOB = new Pair<String, Integer>("graphBfsNodeCountPerJob", 100); 
	private static final Pair<String, Long> ARG_GRAPH_BFS_ENTRY_NODE = new Pair<String, Long>("graphBfsEntryNode", 0L); 
	
	public GraphBFSLocalPipeline(final String p_graphLoadDataPath, final int p_nodeCount, 
			final int p_graphLoadVertexBatchSize, final int p_graphBfsNodeCountPerJob, final long p_graphBfsEntryNode)
	{
		m_arguments.setArgument(ARG_GRAPH_LOAD_DATA_PATH, p_graphLoadDataPath);
		m_arguments.setArgument(ARG_NODE_COUNT, p_nodeCount);
		m_arguments.setArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE, p_graphLoadVertexBatchSize);
		
		m_arguments.setArgument(ARG_GRAPH_BFS_NODE_COUNT_PER_JOB, p_graphBfsNodeCountPerJob);
		m_arguments.setArgument(ARG_GRAPH_BFS_ENTRY_NODE, p_graphBfsEntryNode);
	}

	@Override
	public boolean setup(final ArgumentList p_arguments) 
	{
		recordTaskStatistics(true);
	
		final String graphLoadDataPath = p_arguments.getArgument(ARG_GRAPH_LOAD_DATA_PATH);
		final int nodeCount = p_arguments.getArgument(ARG_NODE_COUNT);
		final int graphLoadVertexBatchSize = p_arguments.getArgument(ARG_GRAPH_LOAD_VERTEX_BATCH_SIZE);
		final int graphBfsNodeCountPerJob = p_arguments.getArgument(ARG_GRAPH_BFS_NODE_COUNT_PER_JOB);
		final long graphBfsEntryNode = p_arguments.getArgument(ARG_GRAPH_BFS_ENTRY_NODE);
		
		pushTask(new GraphLoaderOrderedEdgeListLocal(graphLoadDataPath, nodeCount, graphLoadVertexBatchSize));
		pushTask(new GraphAlgorithmBFS(graphBfsNodeCountPerJob, graphBfsEntryNode));
		
		return true;
	}
}

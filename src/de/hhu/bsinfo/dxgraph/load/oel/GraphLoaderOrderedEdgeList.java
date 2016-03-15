package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxgraph.load.GraphLoader;
import de.hhu.bsinfo.dxgraph.load.RebaseVertexID;
import de.hhu.bsinfo.utils.Pair;

// this class can handle split files for multiple nodes, but
// will only load them on a single/the current node
public abstract class GraphLoaderOrderedEdgeList extends GraphLoader {

	protected int m_vertexBatchSize = 100;
	
	private long m_totalVerticesLoaded = 0;
	private long m_totalEdgesLoaded = 0;
	private long[] m_roots = null;
	
	public GraphLoaderOrderedEdgeList(final String p_path, final int p_numNodes, final int p_vertexBatchSize)
	{
		super(p_path, p_numNodes);
		m_vertexBatchSize = p_vertexBatchSize;
	}
	
	@Override
	public long[] getRoots() {
		return m_roots;
	}
	
	@Override
	public long getTotalVertexCount() {
		return m_totalVerticesLoaded;
	}

	@Override
	public long getTotalEdgeCount() {
		return m_totalEdgesLoaded;
	}

	// returns edge list sorted by nodeIdx and localIdx
	protected Pair<List<OrderedEdgeList>, OrderedEdgeListRoots> setupEdgeLists(final String p_path) {
		Pair<List<OrderedEdgeList>, OrderedEdgeListRoots> lists = 
				new	Pair<List<OrderedEdgeList>, OrderedEdgeListRoots>(new ArrayList<OrderedEdgeList>(), null);
		
		// check if directory
		File tmpFile = new File(p_path);
		if (!tmpFile.exists())
		{
			m_loggerService.error(getClass(), "Cannot setup edge lists, path does not exist: " + p_path);
			return lists;
		}
		
		if (!tmpFile.isDirectory())
		{
			m_loggerService.error(getClass(), "Cannot setup edge lists, path is not a directory: " + p_path);
			return lists;
		}
		
		// iterate files in dir, filter by pattern
		File[] files = tmpFile.listFiles(new FilenameFilter(){
			@Override
			public boolean accept(File dir, String name) 
			{
				String[] tokens = name.split("\\.");
				
				// looking for format xxx.oel or xxx.oel.<nidx>
				if (tokens.length > 1) {
					if (tokens[1].equals("oel") || tokens[1].equals("boel") || tokens[1].equals("roel")) {
						return true;
					}
				} 
				
				return false;
			}
		});
		
		// add filtered files
		for (File file : files) {
			String[] tokens = file.getName().split("\\.");
			
			// looking for format xxx.oel or xxx.oel.<nidx>
			if (tokens.length > 1) {
				if (tokens[1].equals("oel"))
				{
					lists.m_first.add(new OrderedEdgeListTextFileThreadBuffering(file.getAbsolutePath(), m_vertexBatchSize * 1000));
				}
				else if (tokens[1].equals("boel"))
				{
					lists.m_first.add(new OrderedEdgeListBinaryFileThreadBuffering(file.getAbsolutePath(), m_vertexBatchSize * 1000));
				}
				else if (tokens[1].equals("roel"))
				{
					lists.m_second = new OrderedEdgeListRootsTextFile(file.getAbsolutePath());
				}
			} 
		}
		
		// make sure our list is sorted by nodeIdx/localIdx
		lists.m_first.sort(new Comparator<OrderedEdgeList>(){
			@Override
			public int compare(final OrderedEdgeList p_lhs, final OrderedEdgeList p_rhs) {
				if (p_lhs.getNodeIndex() < p_rhs.getNodeIndex()) {
					return -1;
				} else if (p_lhs.getNodeIndex() > p_rhs.getNodeIndex()) {
					return 1;
				} else {
					return 0;
				}
		}});
			
		return lists;
	}
	
	protected boolean load(final OrderedEdgeList p_orderedEdgeList, final RebaseVertexID p_rebase)
	{
		Vertex2[] vertexBuffer = new Vertex2[m_vertexBatchSize];
		int readCount = 0;
		boolean loop = true;
		
		long verticesProcessed = 0;
		float previousProgress = 0.0f;
		
		m_totalVerticesLoaded = p_orderedEdgeList.getTotalVertexCount();
		
		m_loggerService.info(getClass(), "Loading started, vertex count: " + m_totalVerticesLoaded);
		
		while (loop)
		{
			readCount = 0;
			while (readCount < vertexBuffer.length)
			{
				Vertex2 vertex = p_orderedEdgeList.readVertex();
				if (vertex == null) {
					break;
				}
				
				// re-basing of neighbors needed for multiple files
				// offset tells us how much to add
				// also add current node ID
				long[] neighbours = vertex.getNeighbours();
				p_rebase.rebase(neighbours);
			
				vertexBuffer[readCount] = vertex;
				readCount++;
				m_totalEdgesLoaded += neighbours.length;
			}
			
			// create an array which is filled without null padding at the end
			// if necessary 
			if (readCount != vertexBuffer.length) {
				Vertex2[] tmp = new Vertex2[readCount];
				for (int i = 0; i < readCount; i++) {
					tmp[i] = vertexBuffer[i];
				}
				
				vertexBuffer = tmp;
				loop = false;
			}
			
			int count = m_chunkService.create(vertexBuffer);
			if (count != vertexBuffer.length)
			{
				m_loggerService.error(getClass(), "Creating chunks for vertices failed: " + count + " != " + vertexBuffer.length);
				return false;
			}
			
			count = m_chunkService.put(vertexBuffer);
			if (m_chunkService.put(vertexBuffer) != vertexBuffer.length)
			{
				m_loggerService.error(getClass(), "Putting vertex data for chunks failed: " + count + " != " + vertexBuffer.length);
				return false;
			}
			
			verticesProcessed += readCount;
			
			float curProgress = ((float) verticesProcessed) / m_totalVerticesLoaded;
			if (curProgress - previousProgress > 0.01)
			{
				previousProgress = curProgress;
				m_loggerService.info(getClass(), "Loading progress: " + (int)(curProgress * 100) + "%");
			}
		}
		
		m_loggerService.info(getClass(), "Loading done, vertex count: " + m_totalVerticesLoaded);
		
		return true;
	}
	
	protected boolean loadRoots(final OrderedEdgeListRoots p_orderedEdgeListRoots, final RebaseVertexID p_rebase)
	{
		ArrayList<Long> tmp = new ArrayList<Long>();
		m_loggerService.info(getClass(), "Loading roots started");
		
		while (true)
		{
			long root = p_orderedEdgeListRoots.getRoot();
			if (root == -1)
				break;
			
			tmp.add(p_rebase.rebase(root));
		}
		
		m_roots = new long[tmp.size()];
		for (int i = 0; i < tmp.size(); i++) {
			m_roots[i] = tmp.get(i);
		}
		
		m_loggerService.info(getClass(), "Loading roots done, count: " + m_roots.length);
		return true;
	}
}

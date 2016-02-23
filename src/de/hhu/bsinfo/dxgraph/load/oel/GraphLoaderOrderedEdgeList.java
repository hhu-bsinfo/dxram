package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxgraph.load.GraphLoader;
import de.hhu.bsinfo.dxram.data.ChunkID;

// this class can handle split files for multiple nodes, but
// will only load them on a single/the current node
public abstract class GraphLoaderOrderedEdgeList extends GraphLoader {

	protected int m_vertexBatchSize = 100;
	
	public GraphLoaderOrderedEdgeList(final String p_path, final int p_numNodes, final int p_vertexBatchSize)
	{
		super(p_path, p_numNodes);
		m_vertexBatchSize = p_vertexBatchSize;
	}

	// returns edge list sorted by nodeIdx and localIdx
	protected List<OrderedEdgeList> setupEdgeLists(final String p_path) {
		List<OrderedEdgeList> list = new ArrayList<OrderedEdgeList>();
		
		// check if directory
		File tmpFile = new File(p_path);
		if (!tmpFile.exists())
		{
			m_loggerService.error(getClass(), "Cannot setup edge lists, path does not exist: " + p_path);
			return list;
		}
		
		if (!tmpFile.isDirectory())
		{
			m_loggerService.error(getClass(), "Cannot setup edge lists, path is not a directory: " + p_path);
			return list;
		}
		
		// iterate files in dir, filter by pattern
		File[] files = tmpFile.listFiles(new FilenameFilter(){
			@Override
			public boolean accept(File dir, String name) 
			{
				String[] tokens = name.split("\\.");
				
				// looking for format xxx.oel or xxx.oel.<nidx>
				if (tokens.length > 1) {
					if (tokens[1].equals("oel")) {
						return true;
					}
				} 
				
				return false;
			}
		});
		
		// add filtered files
		for (File file : files) {
			list.add(new OrderedEdgeListFile(file.getAbsolutePath()));
		}
		
		// make sure our list is sorted by nodeIdx/localIdx
		list.sort(new Comparator<OrderedEdgeList>(){
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
			
		return list;
	}
	
	protected boolean load(final OrderedEdgeList p_orderedEdgeList, final long p_vertexIDOffset)
	{
		// loading all data of a graph on a single node
		// -> we do not have to re-base the neighbor lists of each vertex
		
		Vertex[] vertexBuffer = new Vertex[m_vertexBatchSize];
		int readCount = 0;
		boolean loop = true;
		long totalVertexCount = p_orderedEdgeList.getTotalVertexCount();
		long verticesProcessed = 0;
		float previousProgress = 0.0f;
		while (loop)
		{
			readCount = 0;
			while (readCount < vertexBuffer.length)
			{
				Vertex vertex = p_orderedEdgeList.readVertex();
				if (vertex == null) {
					break;
				}
				
				// re-basing of neighbors needed for multiple files
				// offset tells us how much to add
				// also add current node ID
				for (int i = 0; i < vertex.getNeighbours().size(); i++) {
					vertex.getNeighbours().set(i, ChunkID.getChunkID(m_bootService.getNodeID(), p_vertexIDOffset + vertex.getNeighbours().get(i)));
				}
			
				vertexBuffer[readCount] = vertex;
				readCount++;
			}
			
			// create an array which is filled without null padding at the end
			// if necessary 
			if (readCount != vertexBuffer.length) {
				Vertex[] tmp = new Vertex[readCount];
				for (int i = 0; i < readCount; i++) {
					tmp[i] = vertexBuffer[i];
				}
				
				vertexBuffer = tmp;
				loop = false;
			}
			
			if (m_chunkService.create(vertexBuffer) != vertexBuffer.length)
			{
				m_loggerService.error(getClass(), "Creating chunks for vertices failed.");
				return false;
			}
			
			if (m_chunkService.put(vertexBuffer) != vertexBuffer.length)
			{
				m_loggerService.error(getClass(), "Putting vertex data for chunks failed.");
				return false;
			}
			
			verticesProcessed += readCount;
			
			float curProgress = ((float) verticesProcessed) / totalVertexCount;
			if (curProgress - previousProgress > 0.01)
			{
				previousProgress = curProgress;
				m_loggerService.info(getClass(), "Loading progress: " + (int)(curProgress * 100) + "%");
			}
			
//			for (Vertex v : vertexBuffer)
//			{
//				System.out.println(v);
//			}
		}
		
		return true;
	}
}

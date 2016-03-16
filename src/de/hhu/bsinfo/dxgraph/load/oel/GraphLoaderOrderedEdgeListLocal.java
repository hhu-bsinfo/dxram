package de.hhu.bsinfo.dxgraph.load.oel;

import java.util.List;

import de.hhu.bsinfo.dxgraph.load.RebaseVertexIDLocal;
import de.hhu.bsinfo.utils.Pair;

public class GraphLoaderOrderedEdgeListLocal extends GraphLoaderOrderedEdgeList {

	public GraphLoaderOrderedEdgeListLocal(final String p_path, final int p_numNodes, final int p_vertexBatchSize)
	{
		super(p_path, p_numNodes, p_vertexBatchSize);
	}
	
	@Override
	public boolean load(final String p_path, final int p_numNodes) 
	{		
		Pair<List<OrderedEdgeList>, OrderedEdgeListRoots> edgeLists = setupEdgeLists(p_path);
		
		// we have to assume that the data order matches
		// the nodeIdx/localIdx sorting
		
		// add offset with each file we processed so we can concat multiple files
		long vertexIDOffset = 0;
		boolean somethingLoaded = false;
		for (OrderedEdgeList edgeList : edgeLists.first()) {
			somethingLoaded = true;
			m_loggerService.info(getClass(), "Loading from edge list " + edgeList);
			if (!load(edgeList, new RebaseVertexIDLocal(m_bootService.getNodeID(), vertexIDOffset)))
				return false;
			vertexIDOffset += edgeList.getTotalVertexCount();
		}
		
		if (!loadRoots(edgeLists.second(), new RebaseVertexIDLocal(m_bootService.getNodeID(), 0))) {
			m_loggerService.warn(getClass(), "Loading roots failed.");
		}
		
		if (!somethingLoaded) {
			m_loggerService.warn(getClass(), "There were no ordered edge lists to load.");
		}
	
		return true;
	}
}

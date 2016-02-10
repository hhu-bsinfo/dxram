package de.hhu.bsinfo.dxgraph.load.oel;

import java.util.List;

public class GraphLoaderOrderedEdgeListLocal extends GraphLoaderOrderedEdgeList {

	@Override
	public boolean load(final String p_path, final int p_numNodes) 
	{		
		List<OrderedEdgeList> edgeLists = setupEdgeLists(p_path);
		
		// we have to assume that the data order matches
		// the nodeIdx/localIdx sorting
		// => we can iterate the list and start separate jobs
		// for every list
		
		for (OrderedEdgeList edgeList : edgeLists) {	
			load(edgeList);
		}
		
		m_jobService.waitForLocalJobsToFinish();
	
		return true;
	}
}

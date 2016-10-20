
package de.hhu.bsinfo.dxgraph;

import de.hhu.bsinfo.dxcompute.ms.TaskPayloadManager;
import de.hhu.bsinfo.dxgraph.algo.bfs.GraphAlgorithmBFSTaskPayload;
import de.hhu.bsinfo.dxgraph.load.GraphLoadBFSRootListTaskPayload;
import de.hhu.bsinfo.dxgraph.load.GraphLoadOrderedEdgeListTaskPayload;
import de.hhu.bsinfo.dxgraph.load.GraphLoadPartitionIndexTaskPayload;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceManager;

/**
 * Main service for dxgraph.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class GraphService extends AbstractDXRAMService {

	public GraphService() {
		super("graph");
	}

	@Override
	protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {

		TaskPayloadManager.registerTaskPayloadClass(GraphTaskPayloads.TYPE,
				GraphTaskPayloads.SUBTYPE_GRAPH_LOAD_PART_INDEX, GraphLoadPartitionIndexTaskPayload.class);
		TaskPayloadManager.registerTaskPayloadClass(GraphTaskPayloads.TYPE,
				GraphTaskPayloads.SUBTYPE_GRAPH_LOAD_OEL, GraphLoadOrderedEdgeListTaskPayload.class);
		TaskPayloadManager.registerTaskPayloadClass(GraphTaskPayloads.TYPE,
				GraphTaskPayloads.SUBTYPE_GRAPH_LOAD_BFS_ROOTS, GraphLoadBFSRootListTaskPayload.class);
		TaskPayloadManager.registerTaskPayloadClass(GraphTaskPayloads.TYPE,
				GraphTaskPayloads.SUBTYPE_GRAPH_ALGO_BFS, GraphAlgorithmBFSTaskPayload.class);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		return true;
	}
}

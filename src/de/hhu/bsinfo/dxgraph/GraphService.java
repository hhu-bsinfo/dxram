
package de.hhu.bsinfo.dxgraph;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxgraph.load.GraphLoadBFSRootListTaskPayload;
import de.hhu.bsinfo.dxgraph.load.GraphLoadOrderedEdgeListTaskPayload;
import de.hhu.bsinfo.dxgraph.load.GraphLoadPartitionIndexTaskPayload;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;

/**
 * Main service for dxgraph.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class GraphService extends AbstractDXRAMService {

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {

		AbstractTaskPayload.registerTaskPayloadClass(GraphTaskPayloads.TYPE,
				GraphTaskPayloads.SUBTYPE_GRAPH_LOAD_PART_INDEX, GraphLoadPartitionIndexTaskPayload.class);
		AbstractTaskPayload.registerTaskPayloadClass(GraphTaskPayloads.TYPE,
				GraphTaskPayloads.SUBTYPE_GRAPH_LOAD_OEL, GraphLoadOrderedEdgeListTaskPayload.class);
		AbstractTaskPayload.registerTaskPayloadClass(GraphTaskPayloads.TYPE,
				GraphTaskPayloads.SUBTYPE_GRAPH_LOAD_BFS_ROOTS, GraphLoadBFSRootListTaskPayload.class);

		return true;
	}

	@Override
	protected boolean shutdownService() {

		return true;
	}

}

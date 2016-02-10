package de.hhu.bsinfo.dxgraph.pipe;

import de.hhu.bsinfo.dxgraph.GraphTaskPipeline;

public class NullPipeline extends GraphTaskPipeline {

	@Override
	public boolean setup() {
		return true;
	}
}

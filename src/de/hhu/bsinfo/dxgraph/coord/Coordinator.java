package de.hhu.bsinfo.dxgraph.coord;

import de.hhu.bsinfo.dxgraph.GraphTask;

public abstract class Coordinator extends GraphTask {

	@Override
	public boolean execute() {
		setup();
		return coordinate();
	}
	
	protected abstract boolean setup();
	
	protected abstract boolean coordinate();
	
}

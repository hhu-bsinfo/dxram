package de.hhu.bsinfo.dxcompute.coord;

import de.hhu.bsinfo.dxcompute.Task;

/**
 * Special kind of task, which provides coordination/synchronization
 * functionality for a pipeline.
 * 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public abstract class Coordinator extends Task {

	@Override
	public boolean execute() {
		setup();
		return coordinate();
	}
	
	/**
	 * Do some setup/init in here before coordination starts.
	 * @return True if setup was successful, false otherwise.
	 */
	protected abstract boolean setup();
	
	/**
	 * Execute coordination task.
	 * @return True if successful, false otherwise.
	 */
	protected abstract boolean coordinate();
	
}

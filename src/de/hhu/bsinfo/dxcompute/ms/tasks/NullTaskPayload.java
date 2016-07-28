
package de.hhu.bsinfo.dxcompute.ms.tasks;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

/**
 * Null task payload for testing.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class NullTaskPayload extends AbstractTaskPayload {

	/**
	 * Constructor
	 */
	public NullTaskPayload() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_NULL_TASK);
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		return 0;
	}

	@Override
	public void handleSignal(final Signal p_signal) {
		// ignore signals
	}
}

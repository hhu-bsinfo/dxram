
package de.hhu.bsinfo.dxcompute.ms.tasks;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Print information about the current slave to the console.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class SlavePrintInfoTaskPayload extends AbstractTaskPayload {
	/**
	 * Constructor
	 */
	public SlavePrintInfoTaskPayload() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_SLAVE_PRINT_INFO_TASK);
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {

		System.out.println("Task " + getClass().getSimpleName() + ": ");
		System.out.println("OwnSlaveId: " + getSlaveId());
		System.out.println("List of slaves in current compute group " + getComputeGroupId() + ": ");
		short[] slaves = getSlaveNodeIds();
		for (int i = 0; i < slaves.length; i++) {
			System.out.println(i + ": " + NodeID.toHexString(slaves[i]));
		}

		return 0;
	}

	@Override
	public void handleSignal(final Signal p_signal) {
		// ignore signals
	}
}

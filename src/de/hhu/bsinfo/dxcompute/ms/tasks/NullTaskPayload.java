
package de.hhu.bsinfo.dxcompute.ms.tasks;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

public class NullTaskPayload extends AbstractTaskPayload {

	public NullTaskPayload() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_NULL_TASK);
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		return 0;
	}

}

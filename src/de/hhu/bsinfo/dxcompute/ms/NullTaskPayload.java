
package de.hhu.bsinfo.dxcompute.ms;

import de.hhu.bsinfo.dxram.DXRAM;

public class NullTaskPayload extends AbstractTaskPayload {

	static {
		registerTaskClass(0, NullTaskPayload.class);
	}

	public NullTaskPayload() {}

	@Override
	public int execute(final DXRAM p_dxram) {
		return 0;
	}

}

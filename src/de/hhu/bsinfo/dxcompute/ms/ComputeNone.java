
package de.hhu.bsinfo.dxcompute.ms;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

public class ComputeNone extends ComputeMSBase {

	public ComputeNone(final int p_computeGroupId, final DXRAMServiceAccessor p_serviceAccessor,
			final NetworkComponent p_network,
			final LoggerComponent p_logger, final NameserviceComponent p_nameservice,
			final AbstractBootComponent p_boot) {
		super(ComputeRole.NONE, p_computeGroupId, p_serviceAccessor, p_network, p_logger, p_nameservice, p_boot);
	}

	@Override
	public void run() {

	}

	@Override
	public void shutdown() {

	}
}

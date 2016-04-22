
package de.hhu.bsinfo.dxcompute.ms;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * None/Null implementation of the compute base. This node does not participate in any
 * master slave computing groups.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class ComputeNone extends AbstractComputeMSBase {

	/**
	 * Constructor
	 * @param p_serviceAccessor
	 *            Service accessor for tasks.
	 * @param p_network
	 *            NetworkComponent
	 * @param p_logger
	 *            LoggerComponent
	 * @param p_nameservice
	 *            NameserviceComponent
	 * @param p_boot
	 *            BootComponent
	 */
	public ComputeNone(final DXRAMServiceAccessor p_serviceAccessor,
			final NetworkComponent p_network,
			final LoggerComponent p_logger, final NameserviceComponent p_nameservice,
			final AbstractBootComponent p_boot) {
		super(ComputeRole.NONE, -1, 0, p_serviceAccessor, p_network, p_logger, p_nameservice, p_boot);
	}

	@Override
	public void run() {

	}

	@Override
	public void shutdown() {

	}
}

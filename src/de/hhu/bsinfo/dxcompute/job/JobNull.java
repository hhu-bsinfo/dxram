
package de.hhu.bsinfo.dxcompute.job;

import de.hhu.bsinfo.dxram.logger.LoggerService;

/**
 * Example for a job implementation.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class JobNull extends AbstractJob {

	public static final short MS_TYPE_ID = 0;
	static {
		registerType(MS_TYPE_ID, JobNull.class);
	}

	/**
	 * Constructor
	 */
	public JobNull() {
		super(null);
	}

	@Override
	public short getTypeID() {
		return MS_TYPE_ID;
	}

	@Override
	protected void execute(final short p_nodeID, final long[] p_chunkIDs) {
		LoggerService logger = getService(LoggerService.class);

		// #if LOGGER >= DEBUG
		// // logger.debug(getClass(), "I am null job.");
		// #endif /* LOGGER >= DEBUG */
	}
}

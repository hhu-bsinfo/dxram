
package de.hhu.bsinfo.dxcompute.ms.tasks;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Wait for specified amount of time.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class WaitTaskPayload extends AbstractTaskPayload {

	private static final Argument MS_ARG_TIME =
			new Argument("timeMs", null, false, "Amount of time to wait in ms.");

	private int m_waitMs;

	/**
	 * Constructor
	 */
	public WaitTaskPayload() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_WAIT_TASK);
	}

	/**
	 * Set the time to wait in ms.
	 * @param p_timeMs
	 *            Time to wait in ms.
	 */
	public void setWaitTimeMs(final int p_timeMs) {
		m_waitMs = p_timeMs;
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		try {
			Thread.sleep(m_waitMs);
		} catch (final InterruptedException e) {
			return -1;
		}

		return 0;
	}

	@Override
	public void terminalCommandRegisterArguments(final ArgumentList p_argumentList) {
		p_argumentList.setArgument(MS_ARG_TIME);
	}

	@Override
	public void terminalCommandCallbackForArguments(final ArgumentList p_argumentList) {
		m_waitMs = p_argumentList.getArgumentValue(MS_ARG_TIME, Integer.class);
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		int size = super.exportObject(p_exporter, p_size);

		p_exporter.writeInt(m_waitMs);

		return size + Integer.BYTES;
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {
		int size = super.importObject(p_importer, p_size);

		m_waitMs = p_importer.readInt();

		return size + Integer.BYTES;
	}

	@Override
	public int sizeofObject() {
		return super.sizeofObject() + Integer.BYTES;
	}
}

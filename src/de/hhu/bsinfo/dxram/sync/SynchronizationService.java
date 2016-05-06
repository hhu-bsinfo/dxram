package de.hhu.bsinfo.dxram.sync;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.sync.tcmds.TcmdBarrierAlloc;
import de.hhu.bsinfo.dxram.sync.tcmds.TcmdBarrierFree;
import de.hhu.bsinfo.dxram.sync.tcmds.TcmdBarrierSignOn;
import de.hhu.bsinfo.dxram.sync.tcmds.TcmdBarrierStatus;
import de.hhu.bsinfo.dxram.term.TerminalComponent;

/**
 * Service providing mechanisms for synchronizing.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 06.05.16
 */
public class SynchronizationService extends AbstractDXRAMService {

	private LookupComponent m_lookup;
	private TerminalComponent m_terminal;

	/**
	 * Allocate a barrier for synchronizing multiple peers.
	 *
	 * @param p_size Size of the barrier, i.e. number of peers that have to sign on until the barrier gets released.
	 * @return Barrier identifier on success, -1 on failure.
	 */
	public int barrierAllocate(final int p_size) {
		return m_lookup.barrierAllocate(p_size);
	}

	/**
	 * Free an allocated barrier.
	 *
	 * @param p_barrierId Barrier to free.
	 * @return True if successful, false otherwise.
	 */
	public boolean barrierFree(final int p_barrierId) {
		return m_lookup.barrierFree(p_barrierId);
	}

	/**
	 * Sign on to a barrier, wait for other peers to sign on as well and for the barrier to get released.
	 *
	 * @param p_barrierId Id of the barrier to sign on to.
	 * @return True if sign on and barrier release was successful, false on error.
	 */
	public boolean barrierSignOn(final int p_barrierId) {
		return m_lookup.barrierSignOn(p_barrierId);
	}

	/**
	 * Get the status of a specific barrier.
	 *
	 * @param p_barrierId Id of the barrier.
	 * @return Array of currently signed on peers with the first index being the number of signed on peers.
	 */
	public short[] barrierGetStatus(final int p_barrierId) {
		return m_lookup.barrierGetStatus(p_barrierId);
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_lookup = getComponent(LookupComponent.class);
		m_terminal = getComponent(TerminalComponent.class);

		m_terminal.registerCommand(new TcmdBarrierAlloc());
		m_terminal.registerCommand(new TcmdBarrierFree());
		m_terminal.registerCommand(new TcmdBarrierSignOn());
		m_terminal.registerCommand(new TcmdBarrierStatus());

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_lookup = null;

		return true;
	}
}

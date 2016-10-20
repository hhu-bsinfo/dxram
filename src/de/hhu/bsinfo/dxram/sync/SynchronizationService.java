
package de.hhu.bsinfo.dxram.sync;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceManager;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.utils.Pair;

/**
 * Service providing mechanisms for synchronizing.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 06.05.16
 */
public class SynchronizationService extends AbstractDXRAMService {

	// dependent components
	private LookupComponent m_lookup;

	/**
	 * Constructor
	 */
	public SynchronizationService() {
		super("sync");
	}

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
	 * Alter the size of an existing barrier (i.e. you want to keep the barrier id but with a different size).
	 *
	 * @param p_barrierId Id of an allocated barrier to change the size of.
	 * @param p_newSize   New size for the barrier.
	 * @return True if changing size was successful, false otherwise.
	 */
	public boolean barrierChangeSize(final int p_barrierId, final int p_newSize) {
		return m_lookup.barrierChangeSize(p_barrierId, p_newSize);
	}

	/**
	 * Sign on to a barrier and wait for it getting released (number of peers, barrier size, have signed on).
	 *
	 * @param p_barrierId  Id of the barrier to sign on to.
	 * @param p_customData Custom data to pass along with the sign on
	 * @return A pair consisting of the list of signed on peers and their custom data passed along
	 * with the sign ons, null on error
	 */
	public Pair<short[], long[]> barrierSignOn(final int p_barrierId, final long p_customData) {
		return m_lookup.barrierSignOn(p_barrierId, p_customData);
	}

	/**
	 * Get the status of a specific barrier.
	 *
	 * @param p_barrierId Id of the barrier.
	 * @return Array of currently signed on peers with the first index being the number of signed on peers or null on error.
	 */
	public short[] barrierGetStatus(final int p_barrierId) {
		return m_lookup.barrierGetStatus(p_barrierId);
	}

	@Override
	protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		m_lookup = getComponent(LookupComponent.class);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_lookup = null;

		return true;
	}
}

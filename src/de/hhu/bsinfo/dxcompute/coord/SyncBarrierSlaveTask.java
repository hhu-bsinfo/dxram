package de.hhu.bsinfo.dxcompute.coord;

/**
 * Counterpart for the SyncBarrierMaster, this is used on a slave node to sync
 * multiple slaves to a single master.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public class SyncBarrierSlaveTask extends Coordinator {
	
	private int m_barrierIdentifer = -1;
	
	private SyncBarrierSlave m_syncBarrier = null;
	
	/**
	 * Constructor
	 * @param p_barrierIdentifier Token to identify this barrier (if using multiple barriers), which is used as a sync token.
	 */
	public SyncBarrierSlaveTask(final int p_barrierIdentifier) {
		m_barrierIdentifer = p_barrierIdentifier;
	}
	
	@Override
	protected boolean setup() {
		m_syncBarrier = new SyncBarrierSlave(m_networkService, m_loggerService);
		return true;
	}

	@Override
	protected boolean coordinate() {
		return m_syncBarrier.execute(m_barrierIdentifer, -1);
	}
}

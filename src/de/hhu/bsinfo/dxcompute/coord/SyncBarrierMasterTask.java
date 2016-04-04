package de.hhu.bsinfo.dxcompute.coord;

/**
 * Implementation for a sync barrier for multiple nodes with one master 
 * and multiple slave nodes. The Master sends a broadcast message 
 * periodically to catch slaves waiting at the barrier. When the master
 * got enough slaves at the barrier, it sends a message to release them.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public class SyncBarrierMasterTask extends Coordinator {

	private int m_numSlaves = 1;
	private int m_broadcastIntervalMs = 2000;	
	private int m_barrierIdentifer = -1;
	
	private SyncBarrierMaster m_syncBarrier = null;
	
	/**
	 * Constructor
	 * @param p_numSlaves Num of slaves to expect for synchronization.
	 * @param p_broadcastIntervalMs Interval in ms to broadcast a message message to catch slaves waiting for the barrier.
	 * @param p_barrierIdentifier Token to identify this barrier (if using multiple barriers), which is used as a sync token.
	 */
	public SyncBarrierMasterTask(final int p_numSlaves, final int p_broadcastIntervalMs, final int p_barrierIdentifier) {
		m_numSlaves = p_numSlaves;
		m_broadcastIntervalMs = p_broadcastIntervalMs;
		m_barrierIdentifer = p_barrierIdentifier;
	}

	@Override
	protected boolean setup() {
		m_syncBarrier = new SyncBarrierMaster(m_numSlaves, m_broadcastIntervalMs, m_networkService, m_bootService, m_loggerService);
		return true;
	}

	@Override
	protected boolean coordinate() {
		return m_syncBarrier.execute(m_barrierIdentifer, -1);
	}
}

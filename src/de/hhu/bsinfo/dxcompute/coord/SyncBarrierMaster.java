
package de.hhu.bsinfo.dxcompute.coord;

import java.util.ArrayList;

import de.hhu.bsinfo.dxcompute.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxcompute.coord.messages.MasterSyncBarrierReleaseMessage;
import de.hhu.bsinfo.dxcompute.coord.messages.SlaveSyncBarrierSignOnMessage;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.Pair;

/**
 * Implementation for a sync barrier for multiple nodes with one master
 * and multiple slave nodes. The Master sends a broadcast message
 * periodically to catch slaves waiting at the barrier. When the master
 * got enough slaves at the barrier, it sends a message to release them.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 */
// TODO redoc
public class SyncBarrierMaster implements MessageReceiver {

	private int m_barrierIdentifer = -1;
	private ArrayList<Pair<Short, Long>> m_slavesSynced = new ArrayList<Pair<Short, Long>>();

	private NetworkService m_networkService = null;
	private BootService m_bootService = null;
	private LoggerService m_loggerService = null;

	/**
	 * Constructor
	 * @param p_numSlaves
	 *            Num of slaves to expect for synchronization.
	 * @param p_broadcastIntervalMs
	 *            Interval in ms to broadcast a message message to catch slaves waiting for the barrier.
	 * @param p_barrierIdentifier
	 *            Token to identify this barrier (if using multiple barriers), which is used as a sync token.
	 */
	public SyncBarrierMaster(final NetworkService p_networkService,
			final BootService p_bootService, final LoggerService p_loggerService) {
		m_networkService = p_networkService;
		m_bootService = p_bootService;
		m_loggerService = p_loggerService;

		m_networkService.registerMessageType(CoordinatorMessages.TYPE,
				CoordinatorMessages.SUBTYPE_SLAVE_SYNC_BARRIER_SIGN_ON, SlaveSyncBarrierSignOnMessage.class);
		m_networkService.registerMessageType(CoordinatorMessages.TYPE,
				CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_RELEASE, MasterSyncBarrierReleaseMessage.class);
	}

	public boolean execute(final int p_barrierCount, final int p_barrierIdentifier, final long p_data) {

		m_barrierIdentifer = p_barrierIdentifier;
		m_slavesSynced.clear();

		m_networkService.registerReceiver(SlaveSyncBarrierSignOnMessage.class, this);

		// wait until all slaves have signed on
		while (m_slavesSynced.size() < p_barrierCount) {
			Thread.yield();
		}

		m_loggerService.debug(getClass(), p_barrierCount + " slaves have signed on.");

		// release barrier
		for (Pair<Short, Long> slaves : m_slavesSynced) {
			m_loggerService.debug(getClass(), "Releasing slave " + slaves.first());
			MasterSyncBarrierReleaseMessage message =
					new MasterSyncBarrierReleaseMessage(slaves.first(), m_barrierIdentifer, p_data);
			NetworkErrorCodes error = m_networkService.sendMessage(message);
			if (error != NetworkErrorCodes.SUCCESS) {
				m_loggerService.error(getClass(), "Sending release to " + slaves.first() + " failed: " + error);
				return false;
			}
		}

		m_loggerService.info(getClass(), "Barrier releaseed.");

		m_networkService.unregisterReceiver(SlaveSyncBarrierSignOnMessage.class, this);

		return true;
	}

	public ArrayList<Pair<Short, Long>> getBarrierData() {
		return m_slavesSynced;
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == CoordinatorMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case CoordinatorMessages.SUBTYPE_SLAVE_SYNC_BARRIER_SIGN_ON:
						incomingSlaveSyncBarrierSignOn((SlaveSyncBarrierSignOnMessage) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	/**
	 * Handle incoming SlaveSyncBarrierSignOnMessage.
	 * @param p_message
	 *            Message to handle.
	 */
	private void incomingSlaveSyncBarrierSignOn(final SlaveSyncBarrierSignOnMessage p_message) {
		// from different sync call
		if (p_message.getSyncToken() != m_barrierIdentifer) {
			m_loggerService.warn(getClass(),
					"Received sync barrier sign on message by slave " + Integer.toHexString(p_message.getSource())
							+ " with sync token " + p_message.getSyncToken() + ", does not match master token "
							+ m_barrierIdentifer);
			return;
		}

		synchronized (m_slavesSynced) {
			// avoid dupes
			if (!m_slavesSynced.contains(p_message.getSource())) {
				m_slavesSynced.add(new Pair<Short, Long>(p_message.getSource(), p_message.getData()));
			}
		}

		m_loggerService.debug(getClass(), "Slave " + p_message.getSource() + " has signed on.");
	}

}

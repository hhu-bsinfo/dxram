
package de.hhu.bsinfo.dxcompute.coord;

import de.hhu.bsinfo.dxcompute.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxcompute.coord.messages.MasterSyncBarrierReleaseMessage;
import de.hhu.bsinfo.dxcompute.coord.messages.SlaveSyncBarrierSignOnMessage;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.util.NodeID;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;

/**
 * Counterpart for the SyncBarrierMaster, this is used on a slave node to sync
 * multiple slaves to a single master.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 */
public class SyncBarrierSlave implements MessageReceiver {

	private int m_barrierIdentifer = -1;

	private volatile boolean m_masterBarrierReleased;
	private volatile long m_barrierDataFromMaster;

	private NetworkService m_networkService;
	private LoggerService m_loggerService;

	/**
	 * Constructor
	 * @param p_barrierIdentifier
	 *            Token to identify this barrier (if using multiple barriers), which is used as a sync token.
	 */
	public SyncBarrierSlave(final NetworkService p_networkService, final LoggerService p_loggerService) {
		m_networkService = p_networkService;
		m_loggerService = p_loggerService;

		m_networkService.registerMessageType(CoordinatorMessages.TYPE,
				CoordinatorMessages.SUBTYPE_SLAVE_SYNC_BARRIER_SIGN_ON, SlaveSyncBarrierSignOnMessage.class);
		m_networkService.registerMessageType(CoordinatorMessages.TYPE,
				CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_RELEASE, MasterSyncBarrierReleaseMessage.class);
	}

	public boolean execute(final short p_masterNodeId, final int p_barrierIdentifier, final long p_data) {
		m_barrierIdentifer = p_barrierIdentifier;
		m_masterBarrierReleased = false;

		m_networkService.registerReceiver(MasterSyncBarrierReleaseMessage.class, this);

		m_loggerService.info(getClass(),
				"Waiting to receive master broadcast with token " + m_barrierIdentifer + "...");

		// TODO we need a sign on request here to make sure that we
		// get a proper response when signing on and if the sign on was confirmed by the master
		// (otherwise the master could not have registered to listen to the sign on message so far and
		// miss it)

		// wait until we got a broadcast by the master
		while (m_masterNodeID == NodeID.INVALID_ID) {
			Thread.yield();
		}

		m_loggerService.info(getClass(),
				"Waiting for master " + p_masterNodeId + " to release barrier " + m_barrierIdentifer + "...");

		while (!m_masterBarrierReleased) {
			Thread.yield();
		}

		m_loggerService.info(getClass(), "Master barrier released, data received: " + m_barrierData);

		m_networkService.unregisterReceiver(MasterSyncBarrierReleaseMessage.class, this);

		return true;
	}

	public long getBarrierData() {
		return m_barrierDataFromMaster;
	}

	@Override
	public void onIncomingMessage(AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == CoordinatorMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_RELEASE:
						incomingMasterSyncBarrierRelease((MasterSyncBarrierReleaseMessage) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	/**
	 * Handle incoming MasterSyncBarrierReleaseMessage.
	 * @param p_message
	 *            Message to handle.
	 */
	private void incomingMasterSyncBarrierRelease(final MasterSyncBarrierReleaseMessage p_message) {
		// ignore non matching sync tokens
		if (p_message.getSyncToken() != m_barrierIdentifer) {
			return;
		}

		m_masterBarrierReleased = true;
		m_barrierDataFromMaster = p_message.getData();
	}

}

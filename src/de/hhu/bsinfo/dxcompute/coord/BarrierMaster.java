
package de.hhu.bsinfo.dxcompute.coord;

import java.util.ArrayList;

import de.hhu.bsinfo.dxcompute.coord.messages.BarrierSlaveSignOnRequest;
import de.hhu.bsinfo.dxcompute.coord.messages.BarrierSlaveSignOnResponse;
import de.hhu.bsinfo.dxcompute.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxcompute.coord.messages.MasterSyncBarrierReleaseMessage;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;

/**
 * Implementation for a sync barrier for multiple nodes with one master
 * and multiple slave nodes. The Master sends a broadcast message
 * periodically to catch slaves waiting at the barrier. When the master
 * got enough slaves at the barrier, it sends a message to release them.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 */
// TODO redoc
public class BarrierMaster implements MessageReceiver {

	private int m_barrierIdentifer = -1;
	private ArrayList<Pair<Short, Long>> m_slavesSynced = new ArrayList<Pair<Short, Long>>();

	private NetworkService m_networkService;
	private LoggerService m_loggerService;

	/**
	 * Constructor
	 * @param p_numSlaves
	 *            Num of slaves to expect for synchronization.
	 * @param p_broadcastIntervalMs
	 *            Interval in ms to broadcast a message message to catch slaves waiting for the barrier.
	 * @param p_barrierIdentifier
	 *            Token to identify this barrier (if using multiple barriers), which is used as a sync token.
	 */
	public BarrierMaster(final NetworkService p_networkService, final LoggerService p_loggerService) {
		m_networkService = p_networkService;
		m_loggerService = p_loggerService;

		m_networkService.registerMessageType(CoordinatorMessages.TYPE,
				CoordinatorMessages.SUBTYPE_BARRIER_SLAVE_SIGN_ON_REQUEST, BarrierSlaveSignOnRequest.class);
		m_networkService.registerMessageType(CoordinatorMessages.TYPE,
				CoordinatorMessages.SUBTYPE_BARRIER_SLAVE_SIGN_ON_RESPONSE, BarrierSlaveSignOnResponse.class);
		m_networkService.registerMessageType(CoordinatorMessages.TYPE,
				CoordinatorMessages.SUBTYPE_BARRIER_MASTER_RELEASE, MasterSyncBarrierReleaseMessage.class);
	}

	public boolean execute(final int p_barrierCount, final int p_barrierIdentifier, final long p_data) {

		m_barrierIdentifer = p_barrierIdentifier;
		m_slavesSynced.clear();

		m_networkService.registerReceiver(BarrierSlaveSignOnRequest.class, this);

		m_loggerService.debug(getClass(), "Waiting for " + p_barrierCount + " slaves to signed on...");

		// wait until all slaves have signed on
		while (m_slavesSynced.size() < p_barrierCount) {
			Thread.yield();
		}

		m_loggerService.debug(getClass(), p_barrierCount + " slaves have signed on.");

		// release barrier
		for (Pair<Short, Long> slaves : m_slavesSynced) {
			m_loggerService.debug(getClass(), "Releasing slave " + NodeID.toHexString(slaves.first()));
			MasterSyncBarrierReleaseMessage message =
					new MasterSyncBarrierReleaseMessage(slaves.first(), m_barrierIdentifer, p_data);
			NetworkErrorCodes error = m_networkService.sendMessage(message);
			if (error != NetworkErrorCodes.SUCCESS) {
				m_loggerService.error(getClass(),
						"Sending release to " + NodeID.toHexString(slaves.first()) + " failed: " + error);
				return false;
			}
		}

		m_loggerService.debug(getClass(), "Barrier released.");

		m_networkService.unregisterReceiver(BarrierSlaveSignOnRequest.class, this);

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
					case CoordinatorMessages.SUBTYPE_BARRIER_SLAVE_SIGN_ON_REQUEST:
						incomingBarrierSlaveSignOnRequest((BarrierSlaveSignOnRequest) p_message);
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
	private void incomingBarrierSlaveSignOnRequest(final BarrierSlaveSignOnRequest p_message) {
		BarrierSlaveSignOnResponse response = new BarrierSlaveSignOnResponse(p_message, p_message.getSyncToken());

		// from different sync call
		if (p_message.getSyncToken() != m_barrierIdentifer) {
			m_loggerService.warn(getClass(),
					"Received barrier sign on message by slave " + NodeID.toHexString(p_message.getSource())
							+ " with sync token " + p_message.getSyncToken() + ", does not match master token "
							+ m_barrierIdentifer);
			response.setStatusCode((byte) 1);
			NetworkErrorCodes err = m_networkService.sendMessage(response);
			if (err != NetworkErrorCodes.SUCCESS) {
				m_loggerService.error(getClass(),
						"Could send invalid token status to slave " + NodeID.toHexString(p_message.getSource())
								+ ": " + err);
			}
		} else {
			NetworkErrorCodes err = m_networkService.sendMessage(response);
			if (err != NetworkErrorCodes.SUCCESS) {
				m_loggerService.error(getClass(), "Could not sign on slave " + NodeID.toHexString(p_message.getSource())
						+ " sending response for sign on failed: " + err);
			} else {
				synchronized (m_slavesSynced) {
					// avoid dupes
					if (!m_slavesSynced.contains(p_message.getSource())) {
						m_slavesSynced.add(new Pair<Short, Long>(p_message.getSource(), p_message.getData()));
					}
				}

				m_loggerService.debug(getClass(),
						"Slave " + NodeID.toHexString(p_message.getSource()) + " has signed on.");
			}
		}
	}
}

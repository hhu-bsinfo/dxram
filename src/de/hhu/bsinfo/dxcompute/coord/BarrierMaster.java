
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
 * and multiple slave nodes. The master waits for a specified number of other
 * nodes to sign on to the barrier. Until then it blocks execution. When
 * the number of signed on slaves is reached the barrier is released. It is
 * possible to use the sync mechanism to transfer primitive data as well
 * External version when using the dxram api
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 */
public class BarrierMaster implements MessageReceiver {

	private int m_barrierIdentifer = -1;
	private ArrayList<Pair<Short, Long>> m_slavesSynced = new ArrayList<Pair<Short, Long>>();

	private NetworkService m_network;
	private LoggerService m_logger;

	/**
	 * Constructor
	 * @param p_network
	 *            Network component needed by barrier
	 * @param p_logger
	 *            Logger component needed by barrier
	 */
	public BarrierMaster(final NetworkService p_network, final LoggerService p_logger) {
		m_network = p_network;
		m_logger = p_logger;

		m_network.registerMessageType(CoordinatorMessages.TYPE,
				CoordinatorMessages.SUBTYPE_BARRIER_SLAVE_SIGN_ON_REQUEST, BarrierSlaveSignOnRequest.class);
		m_network.registerMessageType(CoordinatorMessages.TYPE,
				CoordinatorMessages.SUBTYPE_BARRIER_SLAVE_SIGN_ON_RESPONSE, BarrierSlaveSignOnResponse.class);
		m_network.registerMessageType(CoordinatorMessages.TYPE,
				CoordinatorMessages.SUBTYPE_BARRIER_MASTER_RELEASE, MasterSyncBarrierReleaseMessage.class);
	}

	/**
	 * Execute the barrier. This can be called multiple times, i.e. the barrier is reusable.
	 * @param p_barrierCount
	 *            Number of other nodes/slaves to wait for sign on.
	 * @param p_barrierIdentifier
	 *            Identifier/Sync token to identify this particular barrier. Make sure to use different sync tokens when
	 *            using multiple barriers (at the same time)
	 * @param p_data
	 *            Optional data to be passed to the other nodes on sign on.
	 * @return True if execution/syncing was successful, false otherwise.
	 */
	public boolean execute(final int p_barrierCount, final int p_barrierIdentifier, final long p_data) {

		m_barrierIdentifer = p_barrierIdentifier;
		m_slavesSynced.clear();

		m_network.registerReceiver(BarrierSlaveSignOnRequest.class, this);

		m_logger.debug(getClass(), "Waiting for " + p_barrierCount + " slaves to signed on with identifier "
				+ Integer.toHexString(m_barrierIdentifer) + "...");

		// wait until all slaves have signed on
		while (m_slavesSynced.size() < p_barrierCount) {
			Thread.yield();
		}

		m_logger.debug(getClass(),
				p_barrierCount + " slaves have signed on to " + Integer.toHexString(m_barrierIdentifer));

		// release barrier
		for (Pair<Short, Long> slaves : m_slavesSynced) {
			m_logger.debug(getClass(), "Releasing slave " + NodeID.toHexString(slaves.first()));
			MasterSyncBarrierReleaseMessage message =
					new MasterSyncBarrierReleaseMessage(slaves.first(), m_barrierIdentifer, p_data);
			NetworkErrorCodes error = m_network.sendMessage(message);
			if (error != NetworkErrorCodes.SUCCESS) {
				m_logger.error(getClass(),
						"Sending release to " + NodeID.toHexString(slaves.first()) + " failed: " + error);
				return false;
			}
		}

		m_logger.debug(getClass(), "Barrier " + Integer.toHexString(m_barrierIdentifer) + " released.");

		m_network.unregisterReceiver(BarrierSlaveSignOnRequest.class, this);

		return true;
	}

	/**
	 * Get the optional data received from the signed on slaves during sync.
	 * @return Optional data from each slave
	 */
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
			m_logger.warn(getClass(),
					"Received barrier sign on message by slave " + NodeID.toHexString(p_message.getSource())
							+ " with sync token " + p_message.getSyncToken() + ", does not match master token "
							+ m_barrierIdentifer);
			response.setStatusCode((byte) 1);
			NetworkErrorCodes err = m_network.sendMessage(response);
			if (err != NetworkErrorCodes.SUCCESS) {
				m_logger.error(getClass(),
						"Could send invalid token status to slave " + NodeID.toHexString(p_message.getSource())
								+ ": " + err);
			}
		} else {
			NetworkErrorCodes err = m_network.sendMessage(response);
			if (err != NetworkErrorCodes.SUCCESS) {
				m_logger.error(getClass(), "Could not sign on slave " + NodeID.toHexString(p_message.getSource())
						+ " sending response for sign on failed: " + err);
			} else {
				synchronized (m_slavesSynced) {
					// avoid dupes
					if (!m_slavesSynced.contains(p_message.getSource())) {
						m_slavesSynced.add(new Pair<Short, Long>(p_message.getSource(), p_message.getData()));
					}
				}

				m_logger.debug(getClass(),
						"Slave " + NodeID.toHexString(p_message.getSource()) + " has signed on.");
			}
		}
	}
}

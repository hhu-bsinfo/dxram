
package de.hhu.bsinfo.dxcompute.coord;

import de.hhu.bsinfo.dxcompute.coord.messages.BarrierSlaveSignOnRequest;
import de.hhu.bsinfo.dxcompute.coord.messages.BarrierSlaveSignOnResponse;
import de.hhu.bsinfo.dxcompute.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxcompute.coord.messages.MasterSyncBarrierReleaseMessage;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Counterpart for the master barrier. This is used on a slave node to sync
 * multiple slaves to a single master.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 */
public class BarrierSlaveInternal implements MessageReceiver {

	private int m_barrierIdentifer = -1;

	private volatile boolean m_masterBarrierReleased;
	private volatile long m_barrierDataFromMaster;

	private NetworkComponent m_network;
	private LoggerComponent m_logger;

	/**
	 * Constructor
	 * @param p_network
	 *            Network component needed by barrier
	 * @param p_logger
	 *            Logger component needed by barrier
	 */
	public BarrierSlaveInternal(final NetworkComponent p_network, final LoggerComponent p_logger) {
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
	 * @param p_masterNodeId
	 *            Node id of the master to contact for synchronisation
	 * @param p_barrierIdentifier
	 *            Identifier/Sync token to identify this particular barrier. Make sure to use different sync tokens when
	 *            using multiple barriers (at the same time)
	 * @param p_data
	 *            Optional data to be passed to the other nodes on sign on.
	 * @return True if execution/syncing was successful, false otherwise.
	 */
	public boolean execute(final short p_masterNodeId, final int p_barrierIdentifier, final long p_data) {
		m_barrierIdentifer = p_barrierIdentifier;
		m_masterBarrierReleased = false;
		m_barrierDataFromMaster = -1;

		m_network.register(MasterSyncBarrierReleaseMessage.class, this);

		m_logger.debug(getClass(),
				"Sign on request to master " + NodeID.toHexString(p_masterNodeId) + " with identifier "
						+ m_barrierIdentifer + "...");

		// request for sign on and retry until we succeed
		do {
			BarrierSlaveSignOnRequest request =
					new BarrierSlaveSignOnRequest(p_masterNodeId, m_barrierIdentifer, p_data);
			NetworkErrorCodes err = m_network.sendSync(request);
			if (err == NetworkErrorCodes.SUCCESS) {
				BarrierSlaveSignOnResponse response = (BarrierSlaveSignOnResponse) request.getResponse();
				if (response.getSyncToken() == m_barrierIdentifer) {
					if (response.getStatusCode() == 1) {
						m_logger.error(getClass(),
								"Signed on to master " + NodeID.toHexString(p_masterNodeId) + " with identifier "
										+ m_barrierIdentifer + " failed, invalid sync token");
					} else {
						m_logger.debug(getClass(),
								"Signed on to master " + NodeID.toHexString(p_masterNodeId) + " with identifier "
										+ m_barrierIdentifer);
						break;
					}
				}
			} else {
				m_logger.debug(getClass(), "Sign on in progress: " + err);
			}

			try {
				Thread.sleep(1000);
			} catch (final InterruptedException e) {}
		} while (true);

		while (!m_masterBarrierReleased) {
			Thread.yield();
		}

		m_logger.debug(getClass(),
				"Master barrier " + m_barrierIdentifer + " released, data received: " + m_barrierDataFromMaster);

		return true;
	}

	/**
	 * Get the optional data received from the master on sync.
	 * @return Optional data from master.
	 */
	public long getBarrierData() {
		return m_barrierDataFromMaster;
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == CoordinatorMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case CoordinatorMessages.SUBTYPE_BARRIER_MASTER_RELEASE:
						incomingBarrierMasterRelease((MasterSyncBarrierReleaseMessage) p_message);
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
	private void incomingBarrierMasterRelease(final MasterSyncBarrierReleaseMessage p_message) {
		// ignore non matching sync tokens
		if (p_message.getSyncToken() != m_barrierIdentifer) {
			return;
		}

		m_network.unregister(MasterSyncBarrierReleaseMessage.class, this);

		m_barrierDataFromMaster = p_message.getData();
		m_masterBarrierReleased = true;
	}

}

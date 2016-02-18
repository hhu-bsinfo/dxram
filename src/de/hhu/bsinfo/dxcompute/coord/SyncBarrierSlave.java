package de.hhu.bsinfo.dxcompute.coord;

import de.hhu.bsinfo.dxcompute.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxcompute.coord.messages.MasterSyncBarrierBroadcastMessage;
import de.hhu.bsinfo.dxcompute.coord.messages.MasterSyncBarrierReleaseMessage;
import de.hhu.bsinfo.dxcompute.coord.messages.SlaveSyncBarrierSignOnMessage;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.util.NodeID;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;

/**
 * Counterpart for the SyncBarrierMaster, this is used on a slave node to sync
 * multiple slaves to a single master.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public class SyncBarrierSlave extends Coordinator implements MessageReceiver {

	private static boolean ms_setupOnceDone = false;
	
	private volatile short m_masterNodeID = NodeID.INVALID_ID;
	private volatile boolean m_masterBarrierReleased = false;
	
	/**
	 * Constructor
	 */
	public SyncBarrierSlave() {

	}
	
	@Override
	protected boolean setup() {
		// register network messages once
		if (!ms_setupOnceDone)
		{
			m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_SLAVE_SYNC_BARRIER_SIGN_ON, SlaveSyncBarrierSignOnMessage.class);
			m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_BROADCAST, MasterSyncBarrierBroadcastMessage.class);
			m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_RELEASE, MasterSyncBarrierReleaseMessage.class);
			ms_setupOnceDone = true;
		}
		
		m_networkService.registerReceiver(MasterSyncBarrierBroadcastMessage.class, this);
		m_networkService.registerReceiver(MasterSyncBarrierReleaseMessage.class, this);
		
		return true;
	}

	@Override
	protected boolean coordinate() {
		
		m_loggerService.info(getClass(), "Waiting to receive master broadcast...");
		
		m_masterBarrierReleased = false;
		
		// wait until we got a broadcast by the master
		while (m_masterNodeID == NodeID.INVALID_ID)
		{
			Thread.yield();
		}
		
		m_loggerService.info(getClass(), "Waiting for master " + m_masterNodeID + " to release barrier...");
		
		while (!m_masterBarrierReleased)
		{
			Thread.yield();
		}

		m_loggerService.info(getClass(), "Master barrier released.");
		
		return true;
	}
	
	@Override
	public void onIncomingMessage(AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == CoordinatorMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_BROADCAST:
					incomingMasterSyncBarrierBroadcast((MasterSyncBarrierBroadcastMessage) p_message);
					break;
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
	 * Handle incoming MasterSyncBarrierBroadcastMessage.
	 * @param p_message Message to handle.
	 */
	private void incomingMasterSyncBarrierBroadcast(final MasterSyncBarrierBroadcastMessage p_message) {
		m_loggerService.debug(getClass(), "Got master broadcast from " + p_message.getSource());
		m_masterNodeID = p_message.getSource();
		
		// reply with sign on
		SlaveSyncBarrierSignOnMessage message = new SlaveSyncBarrierSignOnMessage(m_masterNodeID);
		NetworkErrorCodes error = m_networkService.sendMessage(message);
		if (error != NetworkErrorCodes.SUCCESS) {
			m_loggerService.error(getClass(), "Sending sign on to " + m_masterNodeID + " failed: " + error);
		} 
	}
	
	/**
	 * Handle incoming MasterSyncBarrierReleaseMessage.
	 * @param p_message Message to handle.
	 */
	private void incomingMasterSyncBarrierRelease(final MasterSyncBarrierReleaseMessage p_message) {
		m_masterBarrierReleased = true;
	}

}

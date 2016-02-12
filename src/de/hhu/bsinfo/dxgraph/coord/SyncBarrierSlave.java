package de.hhu.bsinfo.dxgraph.coord;

import de.hhu.bsinfo.dxgraph.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxgraph.coord.messages.MasterSyncBarrierBroadcastMessage;
import de.hhu.bsinfo.dxgraph.coord.messages.MasterSyncBarrierReleaseMessage;
import de.hhu.bsinfo.dxgraph.coord.messages.SlaveSyncBarrierSignOnMessage;
import de.hhu.bsinfo.dxgraph.load.oel.messages.GraphLoaderOrderedEdgeListMessages;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.util.NodeID;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;

public class SyncBarrierSlave extends Coordinator implements MessageReceiver {

	private volatile short m_masterNodeID = NodeID.INVALID_ID;
	private volatile boolean m_masterBarrierReleased = false;
	
	public SyncBarrierSlave() {

	}
	
	@Override
	protected boolean setup() {
		m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_BROADCAST, MasterSyncBarrierBroadcastMessage.class);
		m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_SLAVE_SYNC_BARRIER_SIGN_ON, SlaveSyncBarrierSignOnMessage.class);
		m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_RELEASE, MasterSyncBarrierReleaseMessage.class);
		
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
			if (p_message.getType() == GraphLoaderOrderedEdgeListMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_BROADCAST:
					incomingMasterSyncBarrierBroadcast((MasterSyncBarrierBroadcastMessage) p_message);
				case CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_RELEASE:
					incomingMasterSyncBarrierRelease((MasterSyncBarrierReleaseMessage) p_message);
					break;
				default:
					break;
				}
			}
		}
	}
	
	private void incomingMasterSyncBarrierBroadcast(final MasterSyncBarrierBroadcastMessage p_message) {
		m_loggerService.debug(getClass(), "Got master broadcast from " + p_message.getSource());
		m_masterNodeID = p_message.getSource();
	}
	
	private void incomingMasterSyncBarrierRelease(final MasterSyncBarrierReleaseMessage p_message) {
		m_masterBarrierReleased = true;
	}

}

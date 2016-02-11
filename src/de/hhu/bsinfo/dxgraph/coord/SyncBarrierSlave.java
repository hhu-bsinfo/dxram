package de.hhu.bsinfo.dxgraph.coord;

import de.hhu.bsinfo.dxgraph.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxgraph.coord.messages.MasterSlaveSyncBarrierMessage;
import de.hhu.bsinfo.dxgraph.load.oel.messages.GraphLoaderOrderedEdgeListMessages;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.util.NodeID;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;

public class SyncBarrierSlave extends Coordinator implements MessageReceiver {

	private short m_masterNodeID = NodeID.INVALID_ID;

	public void setMasterNodeID(final short p_nodeID) {
		m_masterNodeID = p_nodeID;
	}
	
	@Override
	protected boolean setup() {
		m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_SLAVE_SYNC_BARRIER, MasterSlaveSyncBarrierMessage.class);
		
		m_networkService.registerReceiver(MasterSlaveSyncBarrierMessage.class, this);
		
		return true;
	}

	@Override
	protected boolean coordinate() {
		m_loggerService.info(getClass(), "Synchronizing with master " + m_masterNodeID + "...");
		
		// TODO this needs a request response model, message not sufficient
		// if slaves are faster than the master and they send the message, it will
		// be received by the network handler, but not processed as there is no listener registered
		// -> send sync request from slave to master
		// if master is too slow or not ready yet, he can't respond -> timeout on slave
		// slave has to retry after X seconds
		// go message from master when all slaves have arrived is ok, does not have to be a request/respond
		
		while (true)
		{
			MasterSlaveSyncBarrierMessage message = new MasterSlaveSyncBarrierMessage(slavePeerID);
			NetworkErrorCodes error = m_networkService.sendMessage(message);
			if (error != NetworkErrorCodes.SUCCESS) {
				m_loggerService.error(getClass(), "Sending sync to " + slavePeerID + " failed: " + error);
			} 
		}
		
		return true;
	}
	
	@Override
	public void onIncomingMessage(AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == GraphLoaderOrderedEdgeListMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case CoordinatorMessages.SUBTYPE_MASTER_SLAVE_SYNC_BARRIER:
					incomingMasterSlaveSyncBarrier((MasterSlaveSyncBarrierMessage) p_message);
					break;
				default:
					break;
				}
			}
		}
	}
	
	private void incomingMasterSlaveSyncBarrier(final MasterSlaveSyncBarrierMessage p_message) {
		
	}

}

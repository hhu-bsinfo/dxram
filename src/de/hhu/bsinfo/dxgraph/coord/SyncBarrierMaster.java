package de.hhu.bsinfo.dxgraph.coord;

import java.util.List;
import java.util.Vector;

import de.hhu.bsinfo.dxgraph.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxgraph.coord.messages.MasterSlaveSyncBarrierMessage;
import de.hhu.bsinfo.dxgraph.load.oel.messages.GraphLoaderOrderedEdgeListMessages;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;

public class SyncBarrierMaster extends Coordinator implements MessageReceiver {

	private int m_numSlaves = 1;
	private List<Short> m_slavesSynced = new Vector<Short>();
	
	public void setNumberOfSlaves(final int p_numSlaves) {
		m_numSlaves = p_numSlaves;
	}
	
	

	@Override
	protected boolean setup() {
		m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_SLAVE_SYNC_BARRIER, MasterSlaveSyncBarrierMessage.class);
		
		m_networkService.registerReceiver(MasterSlaveSyncBarrierMessage.class, this);
		
		return true;
	}

	@Override
	protected boolean coordinate() {
		
		m_loggerService.info(getClass(), "Waiting until " + m_numSlaves + " have synced...");
		
		// wait until all slaves have synced
		while (m_slavesSynced.size() != m_numSlaves)
		{
			Thread.yield();
		}
		
		m_loggerService.info(getClass(), "All slaves have synced, respond with sync message.");
		
		for (short slavePeerID : m_slavesSynced)
		{
			m_loggerService.debug(getClass(), "Sending sync to slave " + slavePeerID);
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
		m_slavesSynced.add(p_message.getSource());
	}

}

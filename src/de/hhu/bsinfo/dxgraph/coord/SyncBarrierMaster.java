package de.hhu.bsinfo.dxgraph.coord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

import de.hhu.bsinfo.dxgraph.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxgraph.coord.messages.MasterSyncBarrierBroadcastMessage;
import de.hhu.bsinfo.dxgraph.coord.messages.MasterSyncBarrierReleaseMessage;
import de.hhu.bsinfo.dxgraph.coord.messages.SlaveSyncBarrierSignOnMessage;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;
import de.hhu.bsinfo.utils.locks.SpinLock;

public class SyncBarrierMaster extends Coordinator implements MessageReceiver {

	private static boolean ms_setupOnceDone = false;
	
	private int m_numSlaves = 1;
	private int m_broadcastIntervalMs = 2000;
	private ArrayList<Short> m_slavesSynced = new ArrayList<Short>();
	private Lock m_slavesSyncedMutex = new SpinLock();
	
	public SyncBarrierMaster(final int p_numSlaves, final int p_broadcastIntervalMs) {
		m_numSlaves = p_numSlaves;
		m_broadcastIntervalMs = p_broadcastIntervalMs;
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
		
		m_networkService.registerReceiver(SlaveSyncBarrierSignOnMessage.class, this);
		
		return true;
	}

	@Override
	protected boolean coordinate() {
		
		m_loggerService.info(getClass(), "Broadcasting (every " + m_broadcastIntervalMs + "ms) and waiting until " + m_numSlaves + " slaves have signed on...");
		
		// wait until all slaves have signed on
		while (m_slavesSynced.size() != m_numSlaves)
		{
			// broadcast to all peers, which are potential slaves
			List<Short> peers = m_bootService.getAvailablePeerNodeIDs();
			for (short peer : peers)
			{
				// don't send to ourselves
				if (peer != m_bootService.getNodeID())
				{
					MasterSyncBarrierBroadcastMessage message = new MasterSyncBarrierBroadcastMessage(peer);
					NetworkErrorCodes error = m_networkService.sendMessage(message);
					if (error != NetworkErrorCodes.SUCCESS) {
						m_loggerService.error(getClass(), "Sending broadcast message to peer " + peer + " failed: " + error);
					} 
				}
			}

			try {
				Thread.sleep(m_broadcastIntervalMs);
			} catch (InterruptedException e) {
			}		
		}
		
		m_loggerService.info(getClass(), m_numSlaves + " slaves have signed on.");
		
		// release barrier
		for (short slavePeerID : m_slavesSynced)
		{
			m_loggerService.debug(getClass(), "Releasing slave " + slavePeerID);
			MasterSyncBarrierReleaseMessage message = new MasterSyncBarrierReleaseMessage(slavePeerID);
			NetworkErrorCodes error = m_networkService.sendMessage(message);
			if (error != NetworkErrorCodes.SUCCESS) {
				m_loggerService.error(getClass(), "Sending release to " + slavePeerID + " failed: " + error);
			} 
		}
		
		m_loggerService.info(getClass(), "Barrier releaseed.");
		
		return true;
	}
	
	
	@Override
	public void onIncomingMessage(AbstractMessage p_message) {
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
	
	private void incomingSlaveSyncBarrierSignOn(final SlaveSyncBarrierSignOnMessage p_message) {
		m_slavesSyncedMutex.lock();
		
		// avoid dupes
		if (!m_slavesSynced.contains(p_message.getSource())) {
			m_slavesSynced.add(p_message.getSource());
		}
		
		m_slavesSyncedMutex.unlock();
		
		m_loggerService.debug(getClass(), "Slave " + p_message.getSource() + " has signed on.");
	}

}

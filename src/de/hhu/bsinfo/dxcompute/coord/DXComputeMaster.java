package de.hhu.bsinfo.dxcompute.coord;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import de.hhu.bsinfo.dxcompute.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxcompute.coord.messages.MasterBroadcastMessage;
import de.hhu.bsinfo.dxcompute.coord.messages.MasterSyncBarrierBroadcastMessage;
import de.hhu.bsinfo.dxcompute.coord.messages.MasterSyncBarrierReleaseMessage;
import de.hhu.bsinfo.dxcompute.coord.messages.SlaveSignOnMessage;
import de.hhu.bsinfo.dxcompute.coord.messages.SlaveSyncBarrierSignOnMessage;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.Pair;

public class DXComputeMaster implements MessageReceiver
{
	private DXRAM m_dxram;
	private LoggerService m_loggerService;
	private NetworkService m_networkService;
	private BootService m_bootService;
	
	private ConcurrentHashMap<Short, Boolean> m_signedOnSlaves = new ConcurrentHashMap<Short, Boolean>();
	
	private enum State
	{
		STATE_DISCOVER_SLAVES,
		STATE_READY,
		STATE_EXECUTE
	}
	
	private volatile State m_state = State.STATE_DISCOVER_SLAVES;

	public DXComputeMaster(final DXRAM p_dxram) {
		m_dxram = p_dxram;
		m_loggerService = m_dxram.getService(LoggerService.class);
		m_networkService = m_dxram.getService(NetworkService.class);
		m_bootService = m_dxram.getService(BootService.class);
		
		m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_BROADCAST_MESSAGE, MasterBroadcastMessage.class);
		m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_SLAVE_SIGN_ON_MESSAGE, SlaveSignOnMessage.class);
	}

	public boolean run() {
		
		while (true)
		{
			switch (m_state)
			{
				case STATE_DISCOVER_SLAVES:
				case STATE_READY:
				case STATE_EXECUTE:
				default:
					assert 1 == 2;
			}
		}
	}

	@Override
	public void onIncomingMessage(AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == CoordinatorMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case CoordinatorMessages.SUBTYPE_SLAVE_SIGN_ON_MESSAGE:
					incomingSlaveSignOnMessage((SlaveSignOnMessage) p_message);
					break;
				default:
					break;
				}
			}
		}
	}
	
	private void discoverSlaves()
	{
		// broadcast to all peers, which are potential slaves
		List<Short> peers = m_bootService.getAvailablePeerNodeIDs();
		for (short peer : peers)
		{
			// don't send to ourselves
			if (peer != m_bootService.getNodeID())
			{
				MasterBroadcastMessage message = new MasterBroadcastMessage(peer);
				NetworkErrorCodes error = m_networkService.sendMessage(message);
				if (error != NetworkErrorCodes.SUCCESS) {
					m_loggerService.error(getClass(), "Sending broadcast message to peer " + peer + " failed: " + error);
				} 
			}
		}
		
		
		m_networkService.registerReceiver(SlaveSyncBarrierSignOnMessage.class, this);
		
		// wait until all slaves have signed on
		while (m_slavesSynced.size() != m_numSlaves)
		{
		

			try {
				Thread.sleep(m_broadcastIntervalMs);
			} catch (InterruptedException e) {
			}		
		}
		
		m_loggerService.info(getClass(), m_numSlaves + " slaves have signed on.");
		
		// release barrier
		for (Pair<Short, Long> slaves : m_slavesSynced)
		{
			m_loggerService.debug(getClass(), "Releasing slave " + slaves.first());
			MasterSyncBarrierReleaseMessage message = new MasterSyncBarrierReleaseMessage(slaves.first(), m_barrierIdentifer, p_data);
			NetworkErrorCodes error = m_networkService.sendMessage(message);
			if (error != NetworkErrorCodes.SUCCESS) {
				m_loggerService.error(getClass(), "Sending release to " + slaves.first() + " failed: " + error);
				return false;
			} 
		}
		
		m_loggerService.info(getClass(), "Barrier releaseed.");
		
		m_networkService.unregisterReceiver(SlaveSyncBarrierSignOnMessage.class, this);
	}
	
	private void incomingSlaveSignOnMessage(final SlaveSignOnMessage p_message) {
		if (m_signedOnSlaves.contains(p_message.getSource()))
		{
			m_loggerService.error(getClass(), "Slave " + p_message.getSource() + " has already signed on.");
		}
		else
		{
			m_signedOnSlaves.put(p_message.getSource(), true);
			m_loggerService.info(getClass(), "Slave " + p_message.getSource() + " has signed on.");
		}
	}
}

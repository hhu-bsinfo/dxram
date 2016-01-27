package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkStatistic.Operation;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.lock.messages.LockMessages;
import de.hhu.bsinfo.dxram.lock.messages.LockRequest;
import de.hhu.bsinfo.dxram.lock.messages.LockResponse;
import de.hhu.bsinfo.dxram.lock.messages.UnlockMessage;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.Locations;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;

/**
 * Lock service providing exclusive locking of chunks/data structures.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class PeerLockService extends LockService implements MessageReceiver {
	
	private BootComponent m_boot = null;
	private LoggerComponent m_logger = null;
	private NetworkComponent m_network = null;
	private MemoryManagerComponent m_memoryManager = null;
	private LockComponent m_lock = null;
	private LookupComponent m_lookup = null;
	
	private boolean m_statisticsEnabled = false;
	private int m_remoteLockSendIntervalMs = -1;
	private int m_remoteLockTryTimeoutMs = -1;
	
	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {
		p_settings.setDefaultValue(LockConfigurationValues.Service.REMOTE_LOCK_SEND_INTERVAL_MS);
		p_settings.setDefaultValue(LockConfigurationValues.Service.REMOTE_LOCK_TRY_TIMEOUT_MS);
		p_settings.setDefaultValue(LockConfigurationValues.Service.STATISTICS);
	}
	
	@Override
	protected boolean startService(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) {
	
		m_boot = getComponent(BootComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_memoryManager = getComponent(MemoryManagerComponent.class);
		m_lock = getComponent(LockComponent.class);
		m_lookup = getComponent(LookupComponent.class);
		
		m_network.registerMessageType(LockMessages.TYPE, LockMessages.SUBTYPE_LOCK_REQUEST, LockRequest.class);
		m_network.registerMessageType(LockMessages.TYPE, LockMessages.SUBTYPE_LOCK_RESPONSE, LockResponse.class);
		m_network.registerMessageType(LockMessages.TYPE, LockMessages.SUBTYPE_UNLOCK_MESSAGE, UnlockMessage.class);
		
		m_network.register(LockRequest.class, this);
		m_network.register(UnlockMessage.class, this);
		
		m_statisticsEnabled = p_settings.getValue(LockConfigurationValues.Service.STATISTICS);
		m_remoteLockSendIntervalMs = p_settings.getValue(LockConfigurationValues.Service.REMOTE_LOCK_SEND_INTERVAL_MS);
		m_remoteLockTryTimeoutMs = p_settings.getValue(LockConfigurationValues.Service.REMOTE_LOCK_TRY_TIMEOUT_MS);
		
		return true;
	}


	@Override
	protected boolean shutdownService() 
	{
		m_network = null;
		m_memoryManager = null;
		m_lock = null;
		m_lookup = null;
		
		return true;
	}
	
	@Override
	public ErrorCode lock(final boolean p_writeLock, final int p_timeout, final long p_chunkID) {
		assert p_timeout >= 0;
		assert p_chunkID != ChunkID.INVALID_ID;
		
		if (m_statisticsEnabled)
			Operation.LOCK.enter();
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not lock chunks");
			return ErrorCode.INVALID_PEER_ROLE;
		} 
		
		ErrorCode err = ErrorCode.SUCCESS;
		
		m_memoryManager.lockAccess();
		if (m_memoryManager.exists(p_chunkID)) {
			m_memoryManager.unlockAccess();
			
			if (!m_lock.lock(p_chunkID, m_boot.getNodeID(), p_writeLock, p_timeout)) {
				err = ErrorCode.LOCK_TIMEOUT;
			}
		} else {
			m_memoryManager.unlockAccess();
			
			// remote, figure out location
			Locations locations = m_lookup.get(p_chunkID);
			if (locations == null) {
				err = ErrorCode.CHUNK_NOT_AVAILABLE;
			} else {
	
				short peer = locations.getPrimaryPeer();
				if (peer == m_boot.getNodeID()) {
					// local lock
					if (!m_lock.lock(p_chunkID, m_boot.getNodeID(), p_writeLock, p_timeout)) {
						err = ErrorCode.LOCK_TIMEOUT;
					}
				} else {
					long startTime = System.currentTimeMillis();
					boolean idle = false;
					do
					{
						// avoid heavy network load/lock polling
						if (idle) {
							try {
								Thread.sleep(m_remoteLockSendIntervalMs);
							} catch (InterruptedException e) {
							}
						}
						
						// Remote lock
						LockRequest request = new LockRequest(peer, p_writeLock, p_chunkID);
						NetworkComponent.ErrorCode errNet = m_network.sendSync(request);
						if (errNet != NetworkComponent.ErrorCode.SUCCESS) {
							switch (errNet)
							{
								case DESTINATION_UNREACHABLE:
									err = ErrorCode.PEER_NOT_AVAILABLE; 
									break;
								case SEND_DATA:
									m_lookup.invalidate(p_chunkID); 
									err = ErrorCode.NETWORK; 
									break;
								case RESPONSE_TIMEOUT:
									err = ErrorCode.NETWORK; 
									break;
								default:
									assert 1 == 2; 
									break;
							}
							
							break;
						} else {
							LockResponse response = request.getResponse(LockResponse.class);
							if (response != null) {
								if (response.getStatusCode() == 0) {
									// successfully locked on remote
									err = ErrorCode.SUCCESS;
									break;
								} else if (response.getStatusCode() == -1) {
									// timeout for now, but possible retry
									err = ErrorCode.LOCK_TIMEOUT;
									idle = true;
								}
							} else {
								err = ErrorCode.NETWORK;
								break;
							}
						}
					} while (p_timeout == MS_TIMEOUT_UNLIMITED || System.currentTimeMillis() - startTime < p_timeout);
				}
			}
		}
		
		if (m_statisticsEnabled)
			Operation.LOCK.leave();
		
		return err;
	}

	@Override
	public ErrorCode unlock(final boolean p_writeLock, final long p_chunkID) {
		if (m_statisticsEnabled)
			Operation.UNLOCK.enter();
		
		// early returns
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not use chunks");
			return ErrorCode.INVALID_PEER_ROLE;
		} 

		ErrorCode err = ErrorCode.SUCCESS;

		m_memoryManager.lockAccess();
		if (m_memoryManager.exists(p_chunkID)) {
			m_memoryManager.unlockAccess();
			
			if (!m_lock.unlock(p_chunkID, m_boot.getNodeID(), p_writeLock)) {
				return ErrorCode.INVALID_PARAMETER;
			}
		} else {
			m_memoryManager.unlockAccess();
			
			// remote, figure out location
			Locations locations = m_lookup.get(p_chunkID);
			if (locations == null) {
				err = ErrorCode.CHUNK_NOT_AVAILABLE;
			} else {
	
				short peer = locations.getPrimaryPeer();
				if (peer == m_boot.getNodeID()) {
					// local unlock
					if (!m_lock.unlock(p_chunkID, m_boot.getNodeID(), p_writeLock)) {
						return ErrorCode.INVALID_PARAMETER;
					}
				} else {	
					short primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();
					if (primaryPeer == m_boot.getNodeID()) {
						// Local release
						if (!m_lock.unlock(p_chunkID, m_boot.getNodeID(), p_writeLock)) {
							return ErrorCode.INVALID_PARAMETER;
						}
					} else {
						// Remote release
						UnlockMessage message = new UnlockMessage(primaryPeer, p_writeLock, p_chunkID);
						NetworkComponent.ErrorCode errNet = m_network.sendMessage(message);
						if (errNet != NetworkComponent.ErrorCode.SUCCESS) {
							switch (errNet)
							{
								case DESTINATION_UNREACHABLE:
									err = ErrorCode.PEER_NOT_AVAILABLE; 
									break;
								case SEND_DATA:
									m_lookup.invalidate(p_chunkID); 
									err = ErrorCode.NETWORK; 
									break;
								default:
									assert 1 == 2; 
									break;
							}
						} 
					}
				}
			}
		}

		if (m_statisticsEnabled)
			Operation.UNLOCK.leave();
		
		return err;
	}
	
	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		m_logger.trace(getClass(), "Entering incomingMessage with: p_message=" + p_message);

		if (p_message != null) {
			if (p_message.getType() == LockMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case LockMessages.SUBTYPE_LOCK_REQUEST:
					incomingLockRequest((LockRequest) p_message);
					break;
				case LockMessages.SUBTYPE_UNLOCK_MESSAGE:
					incomingUnlockMessage((UnlockMessage) p_message);
					break;
				default:
					break;
				}
			}
		}

		m_logger.trace(getClass(), "Exiting incomingMessage");
	}
	
	/**
	 * Handles an incoming LockRequest
	 * @param p_request
	 *            the LockRequest
	 */
	private void incomingLockRequest(final LockRequest p_request) {
		boolean success = false;
		
		if (m_statisticsEnabled)
			Operation.INCOMING_LOCK.enter();
		
		// the host handles the timeout as we don't want to block the message receiver thread
		// for too long, execute a tryLock instead	
		success = m_lock.lock(p_request.getChunkID(), m_boot.getNodeID(), p_request.isWriteLockOperation(), m_remoteLockTryTimeoutMs);
		
		if (success) {
			m_network.sendMessage(new LockResponse(p_request, (byte) 0));
		} else {
			m_network.sendMessage(new LockResponse(p_request, (byte) -1));
		}

		if (m_statisticsEnabled)
			Operation.INCOMING_LOCK.leave();
	}

	/**
	 * Handles an incoming UnlockMessage
	 * @param p_message
	 *            the UnlockMessage
	 */
	private void incomingUnlockMessage(final UnlockMessage p_message) {
		if (m_statisticsEnabled)
			Operation.INCOMING_UNLOCK.enter();

		m_lock.unlock(p_message.getChunkID(), m_boot.getNodeID(), p_message.isWriteLockOperation());
		
		if (m_statisticsEnabled)
			Operation.INCOMING_UNLOCK.leave();
	}
}

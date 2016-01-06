package de.uniduesseldorf.dxram.core.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Vector;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.chunk.ChunkStatistic.Operation;
import de.uniduesseldorf.dxram.core.chunk.ChunkService;
import de.uniduesseldorf.dxram.core.chunk.messages.ChunkMessages;
import de.uniduesseldorf.dxram.core.data.Chunk;
import de.uniduesseldorf.dxram.core.data.DataStructure;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.engine.DXRAMService;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodeRole;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener.ConnectionLostEvent;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler.ExceptionSource;
import de.uniduesseldorf.dxram.core.lock.messages.LockMessages;
import de.uniduesseldorf.dxram.core.lock.messages.LockRequest;
import de.uniduesseldorf.dxram.core.lock.messages.LockResponse;
import de.uniduesseldorf.dxram.core.lock.messages.UnlockMessage;
import de.uniduesseldorf.dxram.core.lookup.Locations;
import de.uniduesseldorf.dxram.core.lookup.LookupComponent;
import de.uniduesseldorf.dxram.core.mem.MemoryManagerComponent;
import de.uniduesseldorf.dxram.core.net.NetworkComponent;
import de.uniduesseldorf.dxram.core.net.NetworkComponent.ErrorCode;
import de.uniduesseldorf.dxram.core.statistics.StatisticsConfigurationValues;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.ChunkLockOperation;
import de.uniduesseldorf.dxram.core.util.ChunkSortByOrigin;

import de.uniduesseldorf.menet.AbstractMessage;
import de.uniduesseldorf.menet.NetworkException;
import de.uniduesseldorf.menet.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.utils.Contract;
import de.uniduesseldorf.utils.config.Configuration;

public class LockService extends DXRAMService implements MessageReceiver, ConnectionLostListener {
	
	public static final String SERVICE_NAME = "Lock";
	
	private final Logger LOGGER = Logger.getLogger(LockService.class);
	
	public enum ErrorCode
	{
		SUCCESS,
		UNKNOWN,
		INVALID_PEER_ROLE,
		INVALID_PARAMETER,
		LOCK_TIMEOUT,
		CHUNK_NOT_AVAILABLE,
		PEER_NOT_AVAILABLE,
		NETWORK
	}
	
	private NetworkComponent m_network = null;
	private MemoryManagerComponent m_memoryManager = null;
	private LockComponent m_lock = null;
	private LookupComponent m_lookup = null;
	
	private boolean m_statisticsEnabled = false;
	private int m_remoteLockSendIntervalMs = -1;
	private int m_remoteLockTryTimeout = -1;
	
	public LockService() {
		super(SERVICE_NAME);
	}
	
	@Override
	protected boolean startService(Configuration p_configuration) {
	
		m_network = getComponent(NetworkComponent.COMPONENT_IDENTIFIER);
		m_memoryManager = getComponent(MemoryManagerComponent.COMPONENT_IDENTIFIER);
		m_lock = getComponent(LockComponent.COMPONENT_IDENTIFIER);
		m_lookup = getComponent(LookupComponent.COMPONENT_IDENTIFIER);
		
		m_network.registerMessageType(ChunkMessages.TYPE, LockMessages.SUBTYPE_LOCK_REQUEST, LockRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, LockMessages.SUBTYPE_LOCK_RESPONSE, LockResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, LockMessages.SUBTYPE_UNLOCK_MESSAGE, UnlockMessage.class);
		
		m_statisticsEnabled = p_configuration.getBooleanValue(StatisticsConfigurationValues.STATISTIC_CHUNK);
		
		// TODO add to config and get
		m_remoteLockSendIntervalMs 
		m_remoteLockTryTimeout
		
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
	
	public ErrorCode lock(final boolean p_writeLock, final int p_timeout, final DataStructure p_dataStructure) {
		assert p_timeout >= 0;
		
		if (m_statisticsEnabled)
			Operation.LOCK.enter();
		
		// early returns
		if (p_timeout < 0) 
			return ErrorCode.INVALID_PARAMETER;
		
		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not lock chunks");
			return ErrorCode.INVALID_PEER_ROLE;
		} 
		
		ErrorCode err = ErrorCode.SUCCESS;
		
		m_memoryManager.lockAccess();
		if (m_memoryManager.exists(p_dataStructure.getID())) {
			m_memoryManager.unlockAccess();
			
			if (!m_lock.lock(p_dataStructure.getID(), p_writeLock, p_timeout)) {
				err = ErrorCode.LOCK_TIMEOUT;
			}
		} else {
			m_memoryManager.unlockAccess();
			
			// remote, figure out location
			Locations locations = m_lookup.get(p_dataStructure.getID());
			if (locations == null) {
				err = ErrorCode.CHUNK_NOT_AVAILABLE;
			} else {
	
				short peer = locations.getPrimaryPeer();
				if (peer == getSystemData().getNodeID()) {
					// local lock
					if (!m_lock.lock(p_dataStructure.getID(), p_writeLock, p_timeout)) {
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
						LockRequest request = new LockRequest(peer, p_writeLock, p_dataStructure);
						NetworkComponent.ErrorCode errNet = m_network.sendSync(request);
						if (errNet != NetworkComponent.ErrorCode.SUCCESS) {
							switch (errNet)
							{
								case DESTINATION_UNREACHABLE:
									err = ErrorCode.PEER_NOT_AVAILABLE; 
									break;
								case SEND_DATA:
									m_lookup.invalidate(p_dataStructure.getID()); 
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
					} while (p_timeout == 0 || System.currentTimeMillis() - startTime < p_timeout);
				}
			}
		}
		
		if (m_statisticsEnabled)
			Operation.LOCK.leave();
		
		return err;
	}

	public ErrorCode unlock(final boolean p_writeLock, final DataStructure p_dataStructure) {
		if (m_statisticsEnabled)
			Operation.UNLOCK.enter();
		
		// early returns
		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
			return ErrorCode.INVALID_PEER_ROLE;
		} 

		ErrorCode err = ErrorCode.SUCCESS;

		m_memoryManager.lockAccess();
		if (m_memoryManager.exists(p_dataStructure.getID())) {
			m_memoryManager.unlockAccess();
			
			m_lock.unlock(p_dataStructure.getID(), p_writeLock);
		} else {
			m_memoryManager.unlockAccess();
			
			// remote, figure out location
			Locations locations = m_lookup.get(p_dataStructure.getID());
			if (locations == null) {
				err = ErrorCode.CHUNK_NOT_AVAILABLE;
			} else {
	
				short peer = locations.getPrimaryPeer();
				if (peer == getSystemData().getNodeID()) {
					// local unlock
					m_lock.unlock(p_dataStructure.getID(), p_writeLock);
				} else {	
					short primaryPeer = m_lookup.get(p_dataStructure.getID()).getPrimaryPeer();
					if (primaryPeer == getSystemData().getNodeID()) {
						// Local release
						m_lock.unlock(p_dataStructure.getID(), p_writeLock);

					} else {
						// Remote release
						UnlockMessage message = new UnlockMessage(primaryPeer, p_writeLock, p_dataStructure);
						NetworkComponent.ErrorCode errNet = m_network.sendMessage(message);
						if (errNet != NetworkComponent.ErrorCode.SUCCESS) {
							switch (errNet)
							{
								case DESTINATION_UNREACHABLE:
									err = ErrorCode.PEER_NOT_AVAILABLE; 
									break;
								case SEND_DATA:
									m_lookup.invalidate(p_dataStructure.getID()); 
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
	public void triggerEvent(ConnectionLostEvent p_event) {
		LOGGER.debug("Connection to " + p_event.getSource() + " lost, unlocking all chunks locked by lost instance.");
		
		// TODO this has to get the locked chunks from the superpeer overlay
		// in order to unlock them one by one
		//m_lock.unlockAll(p_event.getSource());
	}
	
	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		LOGGER.trace("Entering incomingMessage with: p_message=" + p_message);

		if (p_message != null) {
			if (p_message.getType() == ChunkMessages.TYPE) {
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

		LOGGER.trace("Exiting incomingMessage");
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
		success = m_lock.lock(p_request.getChunkID(), p_request.isWriteLockOperation(), m_remoteLockTryTimeout);
		
		informSuperpeerChunkLocked(p_request.getChunkID());
		
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

		m_lock.unlock(p_message.getChunkID(), p_message.isWriteLockOperation());
		
		informSuperpeerChunkUnlocked(p_message.getChunkID());
		
		if (m_statisticsEnabled)
			Operation.INCOMING_UNLOCK.leave();
	}
	
	private void informSuperpeerChunkLocked(final long p_chunkID) {
		// TODO send to superpeer overlay that locking chunk X was successful
		// this is needed if:
		// - this peer is crashing to recover the lock state
		// - another peer is crashing which locked chunks but is not able to unlock them anymore
		
		// TODO superpeer has to store a list of locked chunks of each peer that locked something:
		// Locks on Peer 1
		// Peer 0: chunk0, chunk5, chunk6...
		// Peer 1: chunk1, chunk3, ...
		// Peer 2: 
		// Peer 3: chunk10, ...
		// etc.
	}
	
	private void informSuperpeerChunkUnlocked(final long p_chunkID) {
		// TODO send to superpeer overlay that unlocking chunk X was successful
		// this is needed if:
		// - this peer is crashing to recover the locked/unlocked state
		// - another peer is crashing which locked chunks but is not able to unlock them anymore	
	}
}

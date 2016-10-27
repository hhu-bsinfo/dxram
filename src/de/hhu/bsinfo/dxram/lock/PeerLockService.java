
package de.hhu.bsinfo.dxram.lock;

import java.util.ArrayList;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lock.messages.GetLockedListRequest;
import de.hhu.bsinfo.dxram.lock.messages.GetLockedListResponse;
import de.hhu.bsinfo.dxram.lock.messages.LockMessages;
import de.hhu.bsinfo.dxram.lock.messages.LockRequest;
import de.hhu.bsinfo.dxram.lock.messages.LockResponse;
import de.hhu.bsinfo.dxram.lock.messages.UnlockMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkDestinationUnreachableException;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NetworkResponseTimeoutException;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.unit.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Lock service providing exclusive locking of chunks/data structures.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class PeerLockService extends AbstractLockService implements MessageReceiver, EventListener<NodeFailureEvent> {

	private static final Logger LOGGER = LogManager.getFormatterLogger(PeerLockService.class.getSimpleName());

	// statistics recorder
	private static final StatisticsOperation SOP_LOCK =
			StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "Lock");
	private static final StatisticsOperation SOP_UNLOCK =
			StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "Unlock");
	private static final StatisticsOperation SOP_INCOMING_LOCK =
			StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "IncomingLock");
	private static final StatisticsOperation SOP_INCOMING_UNLOCK =
			StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "IncomingUnlock");

	// configuration values
	@Expose
	private TimeUnit m_remoteLockSendInterval = new TimeUnit(10, TimeUnit.MS);
	@Expose
	private TimeUnit m_remoteLockTryTimeout = new TimeUnit(100, TimeUnit.MS);

	// dependent components
	private AbstractBootComponent m_boot;
	private NetworkComponent m_network;
	private MemoryManagerComponent m_memoryManager;
	private AbstractLockComponent m_lock;
	private LookupComponent m_lookup;
	private EventComponent m_event;

	@Override
	protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
		m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
		m_network = p_componentAccessor.getComponent(NetworkComponent.class);
		m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
		m_lock = p_componentAccessor.getComponent(AbstractLockComponent.class);
		m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
		m_event = p_componentAccessor.getComponent(EventComponent.class);
	}

	@Override
	protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {

		m_event.registerListener(this, NodeFailureEvent.class);

		m_network.registerMessageType(DXRAMMessageTypes.LOCK_MESSAGES_TYPE, LockMessages.SUBTYPE_LOCK_REQUEST,
				LockRequest.class);
		m_network.registerMessageType(DXRAMMessageTypes.LOCK_MESSAGES_TYPE, LockMessages.SUBTYPE_LOCK_RESPONSE,
				LockResponse.class);
		m_network.registerMessageType(DXRAMMessageTypes.LOCK_MESSAGES_TYPE, LockMessages.SUBTYPE_UNLOCK_MESSAGE,
				UnlockMessage.class);
		m_network.registerMessageType(DXRAMMessageTypes.LOCK_MESSAGES_TYPE,
				LockMessages.SUBTYPE_GET_LOCKED_LIST_REQUEST,
				GetLockedListRequest.class);
		m_network.registerMessageType(DXRAMMessageTypes.LOCK_MESSAGES_TYPE,
				LockMessages.SUBTYPE_GET_LOCKED_LIST_RESPONSE,
				GetLockedListResponse.class);

		m_network.register(LockRequest.class, this);
		m_network.register(UnlockMessage.class, this);
		m_network.register(GetLockedListRequest.class, this);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		return true;
	}

	@Override
	public ArrayList<Pair<Long, Short>> getLockedList() {
		if (!m_boot.getNodeRole().equals(NodeRole.PEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A %s must not lock chunks", m_boot.getNodeRole());
			// #endif /* LOGGER >= ERROR */
			return null;
		}

		return m_lock.getLockedList();
	}

	@Override
	public ArrayList<Pair<Long, Short>> getLockedList(final short p_nodeId) {
		if (p_nodeId == m_boot.getNodeID()) {
			return getLockedList();
		}

		GetLockedListRequest request = new GetLockedListRequest(p_nodeId);

		try {
			m_network.sendSync(request);
		} catch (final NetworkException e) {
			// #if LOGGER >= ERROR
			LOGGER.error("Sending request to get locked list from node 0x%X failed: %s", p_nodeId, e);
			// #endif /* LOGGER >= ERROR */
			return null;
		}

		return ((GetLockedListResponse) request.getResponse()).getList();
	}

	@Override
	public ErrorCode lock(final boolean p_writeLock, final int p_timeout, final long p_chunkID) {
		assert p_timeout >= 0;
		assert p_chunkID != ChunkID.INVALID_ID;

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer must not lock chunks");
			// #endif /* LOGGER >= ERROR */
			return ErrorCode.INVALID_PEER_ROLE;
		}

		// #ifdef STATISTICS
		SOP_LOCK.enter();
		// #endif /* STATISTICS */

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
			LookupRange lookupRange = m_lookup.getLookupRange(p_chunkID);
			if (lookupRange == null) {
				err = ErrorCode.CHUNK_NOT_AVAILABLE;
			} else {

				short peer = lookupRange.getPrimaryPeer();
				if (peer == m_boot.getNodeID()) {
					// local lock
					if (!m_lock.lock(p_chunkID, m_boot.getNodeID(), p_writeLock, p_timeout)) {
						err = ErrorCode.LOCK_TIMEOUT;
					}
				} else {
					long startTime = System.currentTimeMillis();
					boolean idle = false;
					do {
						// avoid heavy network load/lock polling
						if (idle) {
							try {
								Thread.sleep(m_remoteLockSendInterval.getMs());
							} catch (final InterruptedException ignored) {
							}
						}

						// Remote lock
						LockRequest request = new LockRequest(peer, p_writeLock, p_chunkID);

						try {
							m_network.sendSync(request);
						} catch (final NetworkDestinationUnreachableException e) {
							err = ErrorCode.PEER_NOT_AVAILABLE;
							break;
						} catch (final NetworkResponseTimeoutException e) {
							err = ErrorCode.NETWORK;
							break;
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_chunkID);
							err = ErrorCode.NETWORK;
							break;
						}

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
					} while (p_timeout == MS_TIMEOUT_UNLIMITED || System.currentTimeMillis() - startTime < p_timeout);
				}
			}
		}

		// #ifdef STATISTICS
		SOP_LOCK.leave();
		// #endif /* STATISTICS */

		return err;
	}

	@Override
	public ErrorCode unlock(final boolean p_writeLock, final long p_chunkID) {
		// early returns
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			LOGGER.error("A superpeer must not use chunks");
			// #endif /* LOGGER >= ERROR */
			return ErrorCode.INVALID_PEER_ROLE;
		}

		// #ifdef STATISTICS
		SOP_UNLOCK.enter();
		// #endif /* STATISTICS */

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
			LookupRange lookupRange = m_lookup.getLookupRange(p_chunkID);
			if (lookupRange == null) {
				err = ErrorCode.CHUNK_NOT_AVAILABLE;
			} else {

				short peer = lookupRange.getPrimaryPeer();
				if (peer == m_boot.getNodeID()) {
					// local unlock
					if (!m_lock.unlock(p_chunkID, m_boot.getNodeID(), p_writeLock)) {
						return ErrorCode.INVALID_PARAMETER;
					}
				} else {
					short primaryPeer = m_lookup.getLookupRange(p_chunkID).getPrimaryPeer();
					if (primaryPeer == m_boot.getNodeID()) {
						// Local release
						if (!m_lock.unlock(p_chunkID, m_boot.getNodeID(), p_writeLock)) {
							return ErrorCode.INVALID_PARAMETER;
						}
					} else {
						// Remote release
						UnlockMessage message = new UnlockMessage(primaryPeer, p_writeLock, p_chunkID);

						try {
							m_network.sendMessage(message);
						} catch (final NetworkDestinationUnreachableException e) {
							err = ErrorCode.PEER_NOT_AVAILABLE;
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_chunkID);
							err = ErrorCode.NETWORK;
						}
					}
				}
			}
		}

		// #ifdef STATISTICS
		SOP_UNLOCK.leave();
		// #endif /* STATISTICS */

		return err;
	}

	@Override
	public void eventTriggered(final NodeFailureEvent p_event) {
		if (p_event.getRole() == NodeRole.PEER) {
			// #if LOGGER >= DEBUG
			LOGGER.debug("Connection to peer 0x%X lost, unlocking all chunks locked by lost instance",
					p_event.getNodeID());
			// #endif /* LOGGER >= DEBUG */

			if (!m_lock.unlockAllByNodeID(p_event.getNodeID())) {
				// #if LOGGER >= ERROR
				LOGGER.error("Unlocking all locked chunks of crashed peer 0x%X failed", p_event.getNodeID());
				// #endif /* LOGGER >= ERROR */
			}
		}
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		// #if LOGGER == TRACE
		LOGGER.trace("Entering incomingMessage with: p_message=%s", p_message);
		// #endif /* LOGGER == TRACE */

		if (p_message != null) {
			if (p_message.getType() == DXRAMMessageTypes.LOCK_MESSAGES_TYPE) {
				switch (p_message.getSubtype()) {
					case LockMessages.SUBTYPE_LOCK_REQUEST:
						incomingLockRequest((LockRequest) p_message);
						break;
					case LockMessages.SUBTYPE_UNLOCK_MESSAGE:
						incomingUnlockMessage((UnlockMessage) p_message);
						break;
					case LockMessages.SUBTYPE_GET_LOCKED_LIST_REQUEST:
						incomingLockedListRequest((GetLockedListRequest) p_message);
						break;
					default:
						break;
				}
			}
		}

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting incomingMessage");
		// #endif /* LOGGER == TRACE */
	}

	/**
	 * Handles an incoming LockRequest
	 *
	 * @param p_request the LockRequest
	 */
	private void incomingLockRequest(final LockRequest p_request) {
		boolean success;

		// #ifdef STATISTICS
		SOP_INCOMING_LOCK.enter();
		// #endif /* STATISTICS */

		// the host handles the timeout as we don't want to block the message receiver thread
		// for too long, execute a tryLock instead
		success = m_lock.lock(ChunkID.getLocalID(p_request.getChunkID()), m_boot.getNodeID(),
				p_request.isWriteLockOperation(), (int) m_remoteLockTryTimeout.getMs());

		try {
			if (success) {
				m_network.sendMessage(new LockResponse(p_request, (byte) 0));
			} else {
				m_network.sendMessage(new LockResponse(p_request, (byte) -1));
			}
		} catch (final NetworkException e) {

		}

		// #ifdef STATISTICS
		SOP_INCOMING_LOCK.leave();
		// #endif /* STATISTICS */
	}

	/**
	 * Handles an incoming UnlockMessage
	 *
	 * @param p_message the UnlockMessage
	 */
	private void incomingUnlockMessage(final UnlockMessage p_message) {
		// #ifdef STATISTICS
		SOP_INCOMING_UNLOCK.enter();
		// #endif /* STATISTICS */

		m_lock.unlock(ChunkID.getLocalID(p_message.getChunkID()), m_boot.getNodeID(), p_message.isWriteLockOperation());

		// #ifdef STATISTICS
		SOP_INCOMING_UNLOCK.leave();
		// #endif /* STATISTICS */
	}

	/**
	 * Handles an incoming GetLockedListRequest
	 *
	 * @param p_request the GetLockedListRequest
	 */
	private void incomingLockedListRequest(final GetLockedListRequest p_request) {
		ArrayList<Pair<Long, Short>> list = m_lock.getLockedList();

		GetLockedListResponse response = new GetLockedListResponse(p_request, list);

		try {
			m_network.sendMessage(response);
		} catch (final NetworkException e) {
			// #if LOGGER >= ERROR
			LOGGER.error("Sending locked list response for request %s failed: %s", p_request, e);
			// #endif /* LOGGER >= ERROR */
		}
	}
}

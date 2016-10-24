
package de.hhu.bsinfo.dxram.lock;

import java.util.ArrayList;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceManager;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lock.messages.GetLockedListRequest;
import de.hhu.bsinfo.dxram.lock.messages.GetLockedListResponse;
import de.hhu.bsinfo.dxram.lock.messages.LockMessages;
import de.hhu.bsinfo.dxram.lock.messages.LockRequest;
import de.hhu.bsinfo.dxram.lock.messages.LockResponse;
import de.hhu.bsinfo.dxram.lock.messages.UnlockMessage;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.stats.StatisticsComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Lock service providing exclusive locking of chunks/data structures.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class PeerLockService extends AbstractLockService implements MessageReceiver, EventListener<NodeFailureEvent> {

	// configuration values
	@Expose
	private TimeUnit m_remoteLockSendInterval = new TimeUnit(10, TimeUnit.MS);
	@Expose
	private TimeUnit m_remoteLockTryTimeout = new TimeUnit(100, TimeUnit.MS);

	// dependent components
	private AbstractBootComponent m_boot;
	private LoggerComponent m_logger;
	private NetworkComponent m_network;
	private MemoryManagerComponent m_memoryManager;
	private AbstractLockComponent m_lock;
	private LookupComponent m_lookup;
	private StatisticsComponent m_statistics;

	private LockStatisticsRecorderIDs m_statisticsRecorderIDs;

	@Override
	protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {

		m_boot = getComponent(AbstractBootComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_memoryManager = getComponent(MemoryManagerComponent.class);
		m_lock = getComponent(AbstractLockComponent.class);
		m_lookup = getComponent(LookupComponent.class);
		// #ifdef STATISTICS
		m_statistics = getComponent(StatisticsComponent.class);
		// #endif /* STATISTICS */

		getComponent(EventComponent.class).registerListener(this, NodeFailureEvent.class);

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

		// #ifdef STATISTICS
		m_statisticsRecorderIDs = new LockStatisticsRecorderIDs();
		m_statisticsRecorderIDs.m_id = m_statistics.createRecorder(getClass());
		m_statisticsRecorderIDs.m_operations.m_lock = m_statistics.createOperation(m_statisticsRecorderIDs.m_id,
				LockStatisticsRecorderIDs.Operations.MS_LOCK);
		m_statisticsRecorderIDs.m_operations.m_unlock = m_statistics.createOperation(m_statisticsRecorderIDs.m_id,
				LockStatisticsRecorderIDs.Operations.MS_UNLOCK);
		m_statisticsRecorderIDs.m_operations.m_incomingLock = m_statistics.createOperation(m_statisticsRecorderIDs.m_id,
				LockStatisticsRecorderIDs.Operations.MS_INCOMING_LOCK);
		m_statisticsRecorderIDs.m_operations.m_incomingUnlock = m_statistics
				.createOperation(m_statisticsRecorderIDs.m_id, LockStatisticsRecorderIDs.Operations.MS_INCOMING_UNLOCK);
		// #endif /* STATISTICS */

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_network = null;
		m_memoryManager = null;
		m_lock = null;
		m_lookup = null;

		return true;
	}

	@Override
	public ArrayList<Pair<Long, Short>> getLockedList() {
		if (!m_boot.getNodeRole().equals(NodeRole.PEER)) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "a " + m_boot.getNodeRole() + " must not lock chunks");
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
		NetworkErrorCodes err = m_network.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(),
					"Sending request to get locked list from node " + NodeID.toHexString(p_nodeId) + " failed: " + err);
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
			m_logger.error(getClass(), "a superpeer must not lock chunks");
			// #endif /* LOGGER >= ERROR */
			return ErrorCode.INVALID_PEER_ROLE;
		}

		// #ifdef STATISTICS
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_lock);
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
						NetworkErrorCodes errNet = m_network.sendSync(request);
						if (errNet != NetworkErrorCodes.SUCCESS) {
							switch (errNet) {
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
									assert false;
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

		// #ifdef STATISTICS
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_lock);
		// #endif /* STATISTICS */

		return err;
	}

	@Override
	public ErrorCode unlock(final boolean p_writeLock, final long p_chunkID) {
		// early returns
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "a superpeer must not use chunks");
			// #endif /* LOGGER >= ERROR */
			return ErrorCode.INVALID_PEER_ROLE;
		}

		// #ifdef STATISTICS
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_unlock);
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
						NetworkErrorCodes errNet = m_network.sendMessage(message);
						if (errNet != NetworkErrorCodes.SUCCESS) {
							switch (errNet) {
								case DESTINATION_UNREACHABLE:
									err = ErrorCode.PEER_NOT_AVAILABLE;
									break;
								case SEND_DATA:
									m_lookup.invalidate(p_chunkID);
									err = ErrorCode.NETWORK;
									break;
								default:
									assert false;
									break;
							}
						}
					}
				}
			}
		}

		// #ifdef STATISTICS
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_unlock);
		// #endif /* STATISTICS */

		return err;
	}

	@Override
	public void eventTriggered(final NodeFailureEvent p_event) {
		if (p_event.getRole() == NodeRole.PEER) {
			// #if LOGGER >= DEBUG
			m_logger.debug(getClass(), "Connection to peer " + p_event.getNodeID()
					+ " lost, unlocking all chunks locked by lost instance.");
			// #endif /* LOGGER >= DEBUG */

			if (!m_lock.unlockAllByNodeID(p_event.getNodeID())) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Unlocking all locked chunks of crashed peer "
						+ p_event.getNodeID() + " failed.");
				// #endif /* LOGGER >= ERROR */
			}
		}
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Entering incomingMessage with: p_message=" + p_message);
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
		m_logger.trace(getClass(), "Exiting incomingMessage");
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
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingLock);
		// #endif /* STATISTICS */

		// the host handles the timeout as we don't want to block the message receiver thread
		// for too long, execute a tryLock instead
		success = m_lock.lock(ChunkID.getLocalID(p_request.getChunkID()), m_boot.getNodeID(),
				p_request.isWriteLockOperation(), (int) m_remoteLockTryTimeout.getMs());

		if (success) {
			m_network.sendMessage(new LockResponse(p_request, (byte) 0));
		} else {
			m_network.sendMessage(new LockResponse(p_request, (byte) -1));
		}

		// #ifdef STATISTICS
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingLock);
		// #endif /* STATISTICS */
	}

	/**
	 * Handles an incoming UnlockMessage
	 *
	 * @param p_message the UnlockMessage
	 */
	private void incomingUnlockMessage(final UnlockMessage p_message) {
		// #ifdef STATISTICS
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingUnlock);
		// #endif /* STATISTICS */

		m_lock.unlock(ChunkID.getLocalID(p_message.getChunkID()), m_boot.getNodeID(), p_message.isWriteLockOperation());

		// #ifdef STATISTICS
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingUnlock);
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
		NetworkErrorCodes err = m_network.sendMessage(response);
		// #if LOGGER >= ERROR
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Sending locked list response for request " + p_request + " failed: " + err);
		}
		// #endif /* LOGGER >= ERROR */
	}
}

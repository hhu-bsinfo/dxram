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
import de.uniduesseldorf.dxram.core.lookup.LookupComponent;
import de.uniduesseldorf.dxram.core.mem.MemoryManagerComponent;
import de.uniduesseldorf.dxram.core.net.NetworkComponent;
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
	
	private NetworkComponent m_network = null;
	private MemoryManagerComponent m_memoryManager = null;
	private LockComponent m_lock = null;
	private LookupComponent m_lookup = null;
	
	private boolean m_statisticsEnabled = false;
	
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
	
	public int lock(final boolean p_writeLock, final DataStructure... p_dataStructures) {
		int lockCount = 0;
		
		if (m_statisticsEnabled)
			Operation.LOCK.enter();
		
		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not lock chunks");
		} else {
			ChunkSortByOrigin chunksSorted = ChunkSortByOrigin.sort(m_lookup, m_memoryManager, p_dataStructures);
			
			// execute local locks
			for (DataStructure dataStructure : chunksSorted.getLocalChunks()) {
				m_lock.lock(dataStructure.getID(), p_writeLock);
				lockCount++;
			}
			
			// remote locks
			for (Entry<Short, ArrayList<DataStructure>> entry : chunksSorted.getRemoteChunksByPeers().entrySet()) {
				ArrayList<DataStructure> chunksForMessage = new ArrayList<DataStructure>();
				
				for (DataStructure dataStructure : entry.getValue()) {
					short primaryPeer = m_lookup.get(dataStructure.getID()).getPrimaryPeer();
					if (primaryPeer == getSystemData().getNodeID()) {
						int size;
						int bytesRead;

						// local lock
						m_lock.lock(dataStructure.getID(), p_writeLock);
					} else {
						// Remote lock
						chunksForMessage.add(dataStructure);
					}
				}
				
				LockRequest request;
				if (p_writeLock) {
					request = new LockRequest(entry.getKey(), ChunkLockOperation.WRITE_LOCK, (DataStructure[]) chunksForMessage.toArray());
				} else {
					request = new LockRequest(entry.getKey(), ChunkLockOperation.READ_LOCK, (DataStructure[]) chunksForMessage.toArray());
				}
				// XXX ???
				request.setIgnoreTimeout(true);
				try {
					request.sendSync(m_network);
				} catch (final NetworkException e) {
					// TODO handle error if message could not be sent
					//m_lookup.invalidate(p_chunkID);
					//continue;
				}
				LockResponse response = request.getResponse(LockResponse.class);
				if (response != null) {
					ret = response.getChunk();
				} else {
					// TODO no response?
				}
				// TODO so for locking this is a little tricky here:
				// when sending the request and the chunk on the remote side is locked, we can't
				// just wait until the chunk gets unlocked. instead a kind of try lock is executed
				// by trying a specific amount of time and returning an error code to the host
				// the host then waits a little a tries again until the chunk is unlocked
				// TODO add stuff of lock reponse (if all locks were successful just send a single byte)
				// to put request and others as well
				if (-1 == ret.getChunkID()) {
					try {
						// Chunk is locked, wait a bit
						Thread.sleep(10);
					} catch (final InterruptedException e) {}
				}
			}
//			for (DataStructure dataStructure : remoteChunks) {
//				while (null == ret || -1 == ret.getChunkID()) {
//					short primaryPeer = m_lookup.get(dataStructure.getID()).getPrimaryPeer();
//					if (primaryPeer == getSystemData().getNodeID()) {
//						int size;
//						int bytesRead;
//
//						// local lock
//						m_lock.lock(dataStructure.getID(), p_writeLock);
//					} else {
//						// Remote lock
//						LockRequest request;
//						if (p_writeLock) {
//							request = new LockRequest(primaryPeer, ChunkLockOperation.WRITE_LOCK, dataStructure.getID());
//						} else {
//							
//						}
//						request.setIgnoreTimeout(true);
//						try {
//							request.sendSync(m_network);
//						} catch (final NetworkException e) {
//							m_lookup.invalidate(p_chunkID);
//							continue;
//						}
//						response = request.getResponse(LockResponse.class);
//						if (response != null) {
//							ret = response.getChunk();
//						}
//						if (-1 == ret.getChunkID()) {
//							try {
//								// Chunk is locked, wait a bit
//								Thread.sleep(10);
//							} catch (final InterruptedException e) {}
//						}
//					}
//				}
//			}
		}
		
		if (m_statisticsEnabled)
			Operation.LOCK.leave();
	}

	public int lock(final DataStructure p_dataStructure, final int p_writeLock) {
		DefaultLock lock;
		short primaryPeer;
		LockRequest request;
		LockResponse response;
		boolean success = false;

		if (m_statisticsEnabled)
			Operation.LOCK.enter();

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not use/lock chunks");
		} else {
			m_memoryManager.lockAccess();
			if (m_memoryManager.exists(p_dataStructure.getID())) {
				int size;
				int bytesRead;

				// Local lock
				lock = new DefaultLock(p_dataStructure.getID(), getSystemData().getNodeID(), p_readLock);
				m_lock.lock(lock);

				success = m_memoryManager.get(p_dataStructure);
				m_memoryManager.unlockAccess();
			} else {
				while (null == ret || -1 == ret.getChunkID()) {
					primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();
					if (primaryPeer == getSystemData().getNodeID()) {
						int size;
						int bytesRead;

						// Local lock
						lock = new DefaultLock(p_chunkID, getSystemData().getNodeID(), p_readLock);
						m_lock.lock(lock);

						m_memoryManager.lockAccess();
						size = m_memoryManager.getSize(p_chunkID);
						ret = new Chunk(p_chunkID, size);
						bytesRead = m_memoryManager.get(p_chunkID, ret.getData().array(), 0, size);
						m_memoryManager.unlockAccess();
					} else {
						// Remote lock
						request = new LockRequest(primaryPeer, p_chunkID, p_readLock);
						request.setIgnoreTimeout(true);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_chunkID);
							continue;
						}
						response = request.getResponse(LockResponse.class);
						if (response != null) {
							ret = response.getChunk();
						}
						if (-1 == ret.getChunkID()) {
							try {
								// Chunk is locked, wait a bit
								Thread.sleep(10);
							} catch (final InterruptedException e) {}
						}
					}
				}
			}
		}

		if (m_statisticsEnabled)
			Operation.LOCK.leave();

		return ret;
	}

	public void unlock(final long p_chunkID) {
		short primaryPeer;
		UnlockMessage request;

		if (m_statisticsEnabled)
			Operation.UNLOCK.enter();

		ChunkID.check(p_chunkID);

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (m_memoryManager.exists(p_chunkID)) {
				// Local release
				m_lock.unlock(p_chunkID, getSystemData().getNodeID());
			} else {
				while (true) {
					primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();
					if (primaryPeer == getSystemData().getNodeID()) {
						// Local release
						m_lock.unlock(p_chunkID, getSystemData().getNodeID());
						break;
					} else {
						try {
							// Remote release
							request = new UnlockMessage(primaryPeer, p_chunkID);
							request.send(m_network);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_chunkID);
							continue;
						}
						break;
					}
				}
			}
		}

		if (m_statisticsEnabled)
			Operation.UNLOCK.leave();
	}
	
	@Override
	public void triggerEvent(ConnectionLostEvent p_event) {
		LOGGER.debug("Connection to " + p_event.getSource() + " lost, unlocking all chunks locked by lost instance.");
		
		m_lock.unlockAll(p_event.getSource());
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
		DefaultLock lock;

		if (m_statisticsEnabled)
			Operation.INCOMING_LOCK.enter();

	
			lock = new DefaultLock(p_request.getChunkID(), p_request.getSource(), p_request.isReadLock());
			m_lock.lock(lock);

			// TODO
			// object without variable to store it?
			// new LockResponse(p_request, m_memoryManager.get(lock.getChunk())).send(m_network);

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

		m_lock.unlock(p_message.getChunkID(), p_message.getSource());

		if (m_statisticsEnabled)
			Operation.INCOMING_UNLOCK.leave();
	}
}

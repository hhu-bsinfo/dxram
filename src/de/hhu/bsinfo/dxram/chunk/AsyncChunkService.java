
package de.hhu.bsinfo.dxram.chunk;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.PutMessage;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceManager;
import de.hhu.bsinfo.dxram.lock.AbstractLockComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent.MemoryErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.stats.StatisticsComponent;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * This service provides access to the backend storage system.
 * It does not replace the normal ChunkService, but extends it capabilities with async operations.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.02.16
 */
public class AsyncChunkService extends AbstractDXRAMService implements MessageReceiver {

	// dependent components
	private AbstractBootComponent m_boot;
	private LoggerComponent m_logger;
	private MemoryManagerComponent m_memoryManager;
	private NetworkComponent m_network;
	private LookupComponent m_lookup;
	private AbstractLockComponent m_lock;
	private StatisticsComponent m_statistics;

	private ChunkStatisticsRecorderIDs m_statisticsRecorderIDs;

	/**
	 * Constructor
	 */
	public AsyncChunkService() {
		super("achunk");
	}

	@Override
	protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		m_boot = getComponent(AbstractBootComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_memoryManager = getComponent(MemoryManagerComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_lookup = getComponent(LookupComponent.class);
		m_lock = getComponent(AbstractLockComponent.class);
		// #ifdef STATISTICS
		m_statistics = getComponent(StatisticsComponent.class);
		// #endif /* STATISTICS */
		getComponent(TerminalComponent.class);

		registerNetworkMessages();
		registerNetworkMessageListener();
		// #ifdef STATISTICS
		registerStatisticsOperations();
		// #endif /* STATISTICS */

		// if (getSystemData().getNodeRole().equals(NodeRole.PEER)) {
		// m_backup.registerPeer();
		// }

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_memoryManager = null;
		m_network = null;
		m_lookup = null;

		return true;
	}

	/**
	 * Put/Update the contents of the provided data structures in the backend storage.
	 *
	 * @param p_dataStructres Data structures to put/update.
	 */
	public void put(final DataStructure... p_dataStructres) {
		put(ChunkLockOperation.NO_LOCK_OPERATION, p_dataStructres);
	}

	/**
	 * Put/Update the contents of the provided data structures in the backend storage.
	 *
	 * @param p_chunkUnlockOperation Unlock operation to execute right after the put operation.
	 * @param p_dataStructures       Data structures to put/update.
	 */
	public void put(final ChunkLockOperation p_chunkUnlockOperation, final DataStructure... p_dataStructures) {
		if (p_dataStructures.length == 0) {
			return;
		}

		if (p_dataStructures[0] == null) {
			// #if LOGGER == TRACE
			m_logger.trace(getClass(),
					"put[unlockOp " + p_chunkUnlockOperation + ", dataStructures(" + p_dataStructures.length
							+ ") ...]");
			// #endif /* LOGGER == TRACE */
		} else {
			// #if LOGGER == TRACE
			m_logger.trace(getClass(), "put[unlockOp " + p_chunkUnlockOperation + ", dataStructures("
					+ p_dataStructures.length + ") " + ChunkID.toHexString(p_dataStructures[0].getID()) + ", ...]");
			// #endif /* LOGGER == TRACE */
		}

		// #if LOGGER >= ERROR
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not put chunks");
		}
		// #endif /* LOGGER >= ERROR */

		// #ifdef STATISTICS
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_putAsync,
				p_dataStructures.length);
		// #endif /* STATISTICS */

		Map<Short, ArrayList<DataStructure>> remoteChunksByPeers = new TreeMap<Short, ArrayList<DataStructure>>();

		// sort by local/remote chunks
		m_memoryManager.lockAccess();
		for (DataStructure dataStructure : p_dataStructures) {
			// allowing nulls -> filter
			if (dataStructure == null) {
				continue;
			}

			MemoryErrorCodes err = m_memoryManager.put(dataStructure);
			switch (err) {
				case SUCCESS: {
					// unlock chunks
					if (p_chunkUnlockOperation != ChunkLockOperation.NO_LOCK_OPERATION) {
						boolean writeLock = false;
						if (p_chunkUnlockOperation == ChunkLockOperation.WRITE_LOCK) {
							writeLock = true;
						}

						m_lock.unlock(dataStructure.getID(), m_boot.getNodeID(), writeLock);
					}
					break;
				}
				case DOES_NOT_EXIST: {
					// remote or migrated, figure out location and sort by peers
					LookupRange lookupRange = m_lookup.getLookupRange(dataStructure.getID());
					if (lookupRange == null) {
						continue;
					} else {
						short peer = lookupRange.getPrimaryPeer();

						ArrayList<DataStructure> remoteChunksOfPeer = remoteChunksByPeers.get(peer);
						if (remoteChunksOfPeer == null) {
							remoteChunksOfPeer = new ArrayList<DataStructure>();
							remoteChunksByPeers.put(peer, remoteChunksOfPeer);
						}
						remoteChunksOfPeer.add(dataStructure);
					}
					break;
				}
				default: {
					// #if LOGGER >= ERROR
					m_logger.error(getClass(),
							"Putting local chunk " + ChunkID.toHexString(dataStructure.getID()) + " failed.");
					// #endif /* LOGGER >= ERROR */
					break;
				}
			}
		}

		m_memoryManager.unlockAccess();

		// go for remote chunks
		for (Entry<Short, ArrayList<DataStructure>> entry : remoteChunksByPeers.entrySet()) {
			short peer = entry.getKey();

			if (peer == m_boot.getNodeID()) {
				// local put, migrated data to current node
				m_memoryManager.lockAccess();
				for (final DataStructure dataStructure : entry.getValue()) {
					if (m_memoryManager.put(dataStructure) != MemoryErrorCodes.SUCCESS) {
						// #if LOGGER >= ERROR
						m_logger.error(getClass(),
								"Putting local chunk " + ChunkID.toHexString(dataStructure.getID()) + " failed.");
						// #endif /* LOGGER >= ERROR */
					}
				}
				m_memoryManager.unlockAccess();
			} else {
				// Remote put
				ArrayList<DataStructure> chunksToPut = entry.getValue();
				PutMessage message = new PutMessage(peer, p_chunkUnlockOperation,
						chunksToPut.toArray(new DataStructure[chunksToPut.size()]));
				NetworkErrorCodes error = m_network.sendMessage(message);
				if (error != NetworkErrorCodes.SUCCESS) {
					// #if LOGGER >= ERROR
					m_logger.error(getClass(), "Sending chunk put message to peer " + NodeID.toHexString(peer)
							+ " failed: " + error);
					// #endif /* LOGGER >= ERROR */

					// TODO
					// m_lookup.invalidate(dataStructure.getID());
				}
			}
		}

		// #ifdef STATISTICS
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_putAsync);
		// #endif /* STATISTICS */

		if (p_dataStructures[0] == null) {
			// #if LOGGER == TRACE
			m_logger.trace(getClass(), "put[unlockOp " + p_chunkUnlockOperation + ", dataStructures("
					+ p_dataStructures.length + ") ...]");
			// #endif /* LOGGER == TRACE */
		} else {
			// #if LOGGER == TRACE
			m_logger.trace(getClass(), "put[unlockOp " + p_chunkUnlockOperation + ", dataStructures("
					+ p_dataStructures.length + ") " + ChunkID.toHexString(p_dataStructures[0].getID()) + ", ...]");
			// #endif /* LOGGER == TRACE */
		}
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Entering incomingMessage with: p_message=" + p_message);
		// #endif /* LOGGER == TRACE */

		if (p_message != null) {
			if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE) {
				switch (p_message.getSubtype()) {
					case ChunkMessages.SUBTYPE_PUT_MESSAGE:
						incomingPutMessage((PutMessage) p_message);
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

	// -----------------------------------------------------------------------------------

	/**
	 * Register network messages we use in here.
	 */
	private void registerNetworkMessages() {
		m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_MESSAGE,
				PutMessage.class);
	}

	/**
	 * Register network messages we want to listen to in here.
	 */
	private void registerNetworkMessageListener() {
		m_network.register(PutMessage.class, this);
	}

	/**
	 * Register statistics operations for this service.
	 */
	private void registerStatisticsOperations() {
		m_statisticsRecorderIDs = new ChunkStatisticsRecorderIDs();
		m_statisticsRecorderIDs.m_id = m_statistics.createRecorder(this.getClass());

		m_statisticsRecorderIDs.m_operations.m_putAsync = m_statistics.createOperation(m_statisticsRecorderIDs.m_id,
				ChunkStatisticsRecorderIDs.Operations.MS_PUT_ASYNC);
		m_statisticsRecorderIDs.m_operations.m_incomingPutAsync = m_statistics.createOperation(
				m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_INCOMING_PUT_ASYNC);
	}

	// -----------------------------------------------------------------------------------

	/**
	 * Handles an incoming PutRequest
	 *
	 * @param p_request the PutRequest
	 */
	private void incomingPutMessage(final PutMessage p_request) {
		DataStructure[] chunks = p_request.getDataStructures();

		// #ifdef STATISTICS
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingPutAsync,
				chunks.length);
		// #endif /* STATISTICS */

		m_memoryManager.lockAccess();
		for (DataStructure chunk : chunks) {
			MemoryErrorCodes err = m_memoryManager.put(chunk);
			// #if LOGGER >= WARN
			if (err != MemoryErrorCodes.SUCCESS) {
				m_logger.warn(getClass(),
						"Putting chunk " + ChunkID.toHexString(chunk.getID()) + " failed: " + err);
			}
			// #endif /* LOGGER >= WARN */
		}
		m_memoryManager.unlockAccess();

		// unlock chunks
		if (p_request.getUnlockOperation() != ChunkLockOperation.NO_LOCK_OPERATION) {
			boolean writeLock = false;
			if (p_request.getUnlockOperation() == ChunkLockOperation.WRITE_LOCK) {
				writeLock = true;
			}

			for (DataStructure dataStructure : chunks) {
				m_lock.unlock(dataStructure.getID(), m_boot.getNodeID(), writeLock);
			}
		}

		// #ifdef STATISTICS
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingPutAsync);
		// #endif /* STATISTICS */
	}
}

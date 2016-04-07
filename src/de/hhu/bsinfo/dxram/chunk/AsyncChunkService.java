
package de.hhu.bsinfo.dxram.chunk;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.PutMessage;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.lock.LockComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent.MemoryErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.stats.StatisticsComponent;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;

/**
 * This service provides access to the backend storage system.
 * It does not replace the normal ChunkService, but extends it capabilities with async operations.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.02.16
 */
public class AsyncChunkService extends AbstractDXRAMService implements MessageReceiver {
	private AbstractBootComponent m_boot;
	private LoggerComponent m_logger;
	private MemoryManagerComponent m_memoryManager;
	private NetworkComponent m_network;
	private LookupComponent m_lookup;
	private LockComponent m_lock;
	private StatisticsComponent m_statistics;
	private ChunkStatisticsRecorderIDs m_statisticsRecorderIDs;

	/**
	 * Constructor
	 */
	public AsyncChunkService() {
		super();
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {}

	@Override
	protected boolean startService(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) {
		m_boot = getComponent(AbstractBootComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_memoryManager = getComponent(MemoryManagerComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_lookup = getComponent(LookupComponent.class);
		m_lock = getComponent(LockComponent.class);
		m_statistics = getComponent(StatisticsComponent.class);
		getComponent(TerminalComponent.class);

		registerNetworkMessages();
		registerNetworkMessageListener();
		registerStatisticsOperations();

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
	 * @param p_dataStructres
	 *            Data structures to put/update.
	 */
	public void put(final DataStructure... p_dataStructres) {
		put(ChunkLockOperation.NO_LOCK_OPERATION, p_dataStructres);
	}

	/**
	 * Put/Update the contents of the provided data structures in the backend storage.
	 * @param p_chunkUnlockOperation
	 *            Unlock operation to execute right after the put operation.
	 * @param p_dataStructures
	 *            Data structures to put/update.
	 */
	public void put(final ChunkLockOperation p_chunkUnlockOperation, final DataStructure... p_dataStructures) {
		if (p_dataStructures.length == 0) {
			return;
		}

		if (p_dataStructures[0] == null) {
			m_logger.trace(getClass(), "put[unlockOp " + p_chunkUnlockOperation + ", dataStructures("
					+ p_dataStructures.length + ") ...]");
		} else {
			m_logger.trace(getClass(), "put[unlockOp " + p_chunkUnlockOperation + ", dataStructures("
					+ p_dataStructures.length + ") " + Long.toHexString(p_dataStructures[0].getID()) + ", ...]");
		}

		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not put chunks");
		}

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.OPERATIONS.m_putAsync,
				p_dataStructures.length);

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
				m_logger.error(getClass(),
						"Putting local chunk " + Long.toHexString(dataStructure.getID()) + " failed.");
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
						m_logger.error(getClass(),
								"Putting local chunk " + Long.toHexString(dataStructure.getID()) + " failed.");
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
					m_logger.error(getClass(), "Sending chunk put message to peer " + Integer.toHexString(peer & 0xFFFF)
							+ " failed: " + error);

					// TODO
					// m_lookup.invalidate(dataStructure.getID());

					continue;
				}
			}
		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.OPERATIONS.m_putAsync);

		if (p_dataStructures[0] == null) {
			m_logger.trace(getClass(), "put[unlockOp " + p_chunkUnlockOperation + ", dataStructures("
					+ p_dataStructures.length + ") ...]");
		} else {
			m_logger.trace(getClass(), "put[unlockOp " + p_chunkUnlockOperation + ", dataStructures("
					+ p_dataStructures.length + ") " + Long.toHexString(p_dataStructures[0].getID()) + ", ...]");
		}
		return;
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		m_logger.trace(getClass(), "Entering incomingMessage with: p_message=" + p_message);

		if (p_message != null) {
			if (p_message.getType() == ChunkMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case ChunkMessages.SUBTYPE_PUT_MESSAGE:
					incomingPutMessage((PutMessage) p_message);
					break;
				default:
					break;
				}
			}
		}

		m_logger.trace(getClass(), "Exiting incomingMessage");
	}

	// -----------------------------------------------------------------------------------

	/**
	 * Register network messages we use in here.
	 */
	private void registerNetworkMessages() {
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_PUT_MESSAGE, PutMessage.class);
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

		m_statisticsRecorderIDs.OPERATIONS.m_putAsync = m_statistics.createOperation(m_statisticsRecorderIDs.m_id,
				ChunkStatisticsRecorderIDs.Operations.MS_PUT_ASYNC);
		m_statisticsRecorderIDs.OPERATIONS.m_incomingPutAsync = m_statistics.createOperation(
				m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_INCOMING_PUT_ASYNC);
	}

	// -----------------------------------------------------------------------------------

	/**
	 * Handles an incoming PutRequest
	 * @param p_request
	 *            the PutRequest
	 */
	private void incomingPutMessage(final PutMessage p_request) {
		DataStructure[] chunks = p_request.getDataStructures();

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.OPERATIONS.m_incomingPutAsync,
				chunks.length);

		m_memoryManager.lockAccess();
		for (int i = 0; i < chunks.length; i++) {
			MemoryErrorCodes err = m_memoryManager.put(chunks[i]);
			if (err != MemoryErrorCodes.SUCCESS) {
				// does not exist (anymore)
				m_logger.warn(getClass(), "Putting chunk " + Long.toHexString(chunks[i].getID()) + " failed: " + err);
			}
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

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.OPERATIONS.m_incomingPutAsync);
	}
}

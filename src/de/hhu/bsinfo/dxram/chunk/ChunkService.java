package de.hhu.bsinfo.dxram.chunk;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.CreateRequest;
import de.hhu.bsinfo.dxram.chunk.messages.CreateResponse;
import de.hhu.bsinfo.dxram.chunk.messages.GetLocalChunkIDRangesRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetLocalChunkIDRangesResponse;
import de.hhu.bsinfo.dxram.chunk.messages.GetRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetResponse;
import de.hhu.bsinfo.dxram.chunk.messages.PutRequest;
import de.hhu.bsinfo.dxram.chunk.messages.PutResponse;
import de.hhu.bsinfo.dxram.chunk.messages.RemoveRequest;
import de.hhu.bsinfo.dxram.chunk.messages.RemoveResponse;
import de.hhu.bsinfo.dxram.chunk.messages.StatusRequest;
import de.hhu.bsinfo.dxram.chunk.messages.StatusResponse;
import de.hhu.bsinfo.dxram.chunk.tcmds.TcmdChunkCreate;
import de.hhu.bsinfo.dxram.chunk.tcmds.TcmdChunkGet;
import de.hhu.bsinfo.dxram.chunk.tcmds.TcmdChunkPut;
import de.hhu.bsinfo.dxram.chunk.tcmds.TcmdChunkRemove;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.lock.LockComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.Locations;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent.MemoryErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.stats.StatisticsComponent;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * This service provides access to the backend storage system.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public class ChunkService extends DXRAMService implements MessageReceiver
{
	private BootComponent m_boot = null;
	private LoggerComponent m_logger = null;
	private MemoryManagerComponent m_memoryManager = null;
	private NetworkComponent m_network = null;
	private LookupComponent m_lookup = null;
	private LockComponent m_lock = null;
	private StatisticsComponent m_statistics = null;
	private TerminalComponent m_terminal = null;
//	private BackupComponent m_backup = null;
	
	private ChunkStatisticsRecorderIDs m_statisticsRecorderIDs = null;
	
	private boolean m_logActive = false;
	private boolean m_firstBackupRangeInitialized = false;
	private boolean m_removePerformanceCriticalSection = false;
	
	/**
	 * Constructor
	 */
	public ChunkService() {
		super();
	}
	
	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {
		p_settings.setDefaultValue(ChunkConfigurationValues.Service.REMOVE_PERFORMANCE_CRITICAL_SECTIONS);
	}
	
	@Override
	protected boolean startService(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) 
	{
		m_removePerformanceCriticalSection = p_settings.getValue(ChunkConfigurationValues.Service.REMOVE_PERFORMANCE_CRITICAL_SECTIONS);
		
		m_boot = getComponent(BootComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_memoryManager = getComponent(MemoryManagerComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_lookup = getComponent(LookupComponent.class);
		m_lock = getComponent(LockComponent.class);
		m_statistics = getComponent(StatisticsComponent.class);
		m_terminal = getComponent(TerminalComponent.class);

		registerNetworkMessages();
		registerNetworkMessageListener();
		registerStatisticsOperations();
		
		m_terminal.registerCommand(new TcmdChunkCreate());
		m_terminal.registerCommand(new TcmdChunkRemove());
		m_terminal.registerCommand(new TcmdChunkGet());
		m_terminal.registerCommand(new TcmdChunkPut());
		
		m_logActive = false;
		m_firstBackupRangeInitialized = false;
		
//		if (getSystemData().getNodeRole().equals(NodeRole.PEER)) {
//			m_backup.registerPeer();
//		}
		
		return true;
	}

	@Override
	protected boolean shutdownService() 
	{
		m_memoryManager = null;
		m_network = null;
		m_lookup = null;
		
		return true;
	}
	
	/**
	 * Get the status of the chunk service.
	 * @return Status object with current status of the service.
	 */
	public Status getStatus()
	{
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer does not provide a status");
			return null;
		} 
		
		Status status = new Status();
		
		MemoryManagerComponent.Status memManStatus = m_memoryManager.getStatus();
		
		status.m_freeMemoryBytes = memManStatus.getFreeMemory();
		status.m_totalMemoryBytes = memManStatus.getTotalMemory();
		status.m_numberOfActiveChunks = m_memoryManager.getNumberOfActiveChunks();
		
		return status;
	}
	
	/**
	 * Get the status of a remote node specified by a node id.
	 * @param p_nodeID Node id to get the status from.
	 * @return Status object with status information of the remote node or null if getting status failed.
	 */
	public Status getStatus(final short p_nodeID)
	{
		Status status = null;
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer does not provide a status");
			return status;
		} 
			
		// own status?
		if (p_nodeID == m_boot.getNodeID()) {
			status = getStatus();
		} else {
			// grab from remote
			StatusRequest request = new StatusRequest(p_nodeID);
			NetworkErrorCodes err = m_network.sendSync(request);
			if (err != NetworkErrorCodes.SUCCESS) {
				m_logger.error(getClass(), "Sending get status request to peer " + Integer.toHexString(p_nodeID & 0xFFFF) + " failed: " + err);
			} else {
				StatusResponse response = request.getResponse(StatusResponse.class);
				status = response.getStatus();
			}
		}
		
		return status;
	}
	
	/**
	 * Get the total amount of memory.
	 * @return Total amount of memory in bytes.
	 */
	public long getTotalMemory()
	{
		return m_memoryManager.getStatus().getTotalMemory();
	}
	
	/**
	 * Get the amounf of free memory.
	 * @return Amount of free memory in bytes.
	 */
	public long getFreeMemory()
	{
		return m_memoryManager.getStatus().getFreeMemory();
	}
	
	/**
	 * Create a new chunk.
	 * @param p_size Size of the new chunk.
	 * @param p_count Number of chunks to create with the specified size.
	 * @return ChunkIDs/Handles identifying the created chunks.
	 */
	public long[] create(final int p_size, final int p_count)
	{		
		long[] chunkIDs = null;

		// TODO have parameter checks for all other calls as well
		if (p_size <= 0) {
			return null;
		}
		
		if (p_count <= 0) {
			return null;
		}
		
		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "create[size " + p_size + ", count " + p_count + "]");
		}
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not create chunks");
			return null;
		} 
		
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_create, p_count);
		
		chunkIDs = new long[p_count];

		m_memoryManager.lockManage();
		// keep loop tight and execute everything
		// that we don't have to lock outside of this section
		for (int i = 0; i < p_count; i++) {
			chunkIDs[i] = m_memoryManager.create(p_size);		
		}
		m_memoryManager.unlockManage();
		
		// tell the superpeer overlay about our newly created chunks, otherwise they can not be found
		// by other peers
		for (int i = 0; i < p_count; i++) {
			initBackupRange(chunkIDs[i], p_size);
		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_create);
		
		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "create[size " + p_size + ", count " + p_count + "] -> " + Long.toHexString(chunkIDs[0]) + ", ...");
		}
		
		return chunkIDs;
	}
	
	/**
	 * Create new chunks according to the data structures provided.
	 * Important: This does NOT put/write the contents of the data structure provided.
	 * It creates chunks with the sizes of the data structures and sets the IDs.
	 * @param p_dataStructures Data structures to create chunks for.
	 * @return Number of successfully created chunks.
	 */
	public int create(final DataStructure... p_dataStructures)
	{
		int count = 0;
		
		if (p_dataStructures.length == 0)
			return count;

		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "create[numDataStructures " + p_dataStructures.length + "...]");
		}
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not create chunks");
			return count;
		} 
		
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_create, p_dataStructures.length);

		m_memoryManager.lockManage();
		// keep loop tight and execute everything
		// that we don't have to lock outside of this section
		for (int i = 0; i < p_dataStructures.length; i++) {
			long chunkID = m_memoryManager.create(p_dataStructures[i].sizeofObject());
			if (chunkID != ChunkID.INVALID_ID) {
				count++;
				p_dataStructures[i].setID(chunkID);
			} else {	
				p_dataStructures[i].setID(ChunkID.INVALID_ID);
			}
		}
		m_memoryManager.unlockManage();
		
		// tell the superpeer overlay about our newly created chunks, otherwise they can not be found
		// by other peers
		for (int i = 0; i < p_dataStructures.length; i++) {
			if (p_dataStructures[i].getID() != ChunkID.INVALID_ID) {
				initBackupRange(p_dataStructures[i].getID(), p_dataStructures[i].sizeofObject());
			}
		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_create);
		
		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "create[numDataStructures(" + p_dataStructures.length + ") " + p_dataStructures[0].sizeofObject() + ", ...] -> " + Long.toHexString(p_dataStructures[0].getID()) + ", ...");
		}
		
		return count;
	}
	
	/**
	 * Create chunks with different sizes.
	 * @param p_sizes List of sizes to create chunks for.
	 * @return ChunkIDs/Handles identifying the created chunks.
	 */
	public long[] create(final int... p_sizes) {
		long[] chunkIDs = null;
		
		if (p_sizes.length == 0)
			return new long[0];

		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "create[sizes(" + p_sizes.length + ") " + p_sizes[0] + ", ...]");
		}
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not create chunks");
			return null;
		} 
		
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_create, p_sizes.length);
		
		chunkIDs = new long[p_sizes.length];

		m_memoryManager.lockManage();
		// keep loop tight and execute everything
		// that we don't have to lock outside of this section
		for (int i = 0; i < p_sizes.length; i++) {
			chunkIDs[i] = m_memoryManager.create(p_sizes[i]);		
		}
		m_memoryManager.unlockManage();
		
		// tell the superpeer overlay about our newly created chunks, otherwise they can not be found
		// by other peers
		for (int i = 0; i < p_sizes.length; i++) {
			initBackupRange(chunkIDs[i], p_sizes[i]);
		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_create);
		
		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "create[sizes(" + p_sizes.length + ") " + p_sizes[0] + ", ...] -> " + Long.toHexString(chunkIDs[0]) + ", ...");
		}
		
		return chunkIDs;
	}

	/**
	 * Create chunks on another node.
	 * @param p_peer NodeID of the peer to create the chunks on.
	 * @param p_sizes Sizes to create chunks of.
	 * @return ChunkIDs/Handles identifying the created chunks.
	 */
	public long[] create(final short p_peer, final int... p_sizes) {
		long[] chunkIDs = null;
		
		if (p_sizes.length == 0)
			return new long[0];

		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "create[peer " + Integer.toHexString(p_peer & 0xFFFF) + ", sizes(" + p_sizes.length + ") " + p_sizes[0] + ", ...]");
		}
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not create chunks");
		}
		
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_remoteCreate, p_sizes.length);

		CreateRequest request = new CreateRequest(p_peer, p_sizes);
		NetworkErrorCodes error = m_network.sendSync(request);
		if (error != NetworkErrorCodes.SUCCESS)
		{
			m_logger.error(getClass(), "Sending chunk create request to peer " + Integer.toHexString(p_peer & 0xFFFF) + " failed: " + error);
		} else {
			CreateResponse response = request.getResponse(CreateResponse.class);
			chunkIDs = response.getChunkIDs();
		}
		
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_remoteCreate);
		
		if (!m_removePerformanceCriticalSection) {
			if (chunkIDs != null)
				m_logger.trace(getClass(), "create[peer " + Integer.toHexString(p_peer & 0xFFFF) + ", sizes(" + p_sizes.length + ") " + p_sizes[0] + ", ...] -> " + Long.toHexString(chunkIDs[0]) + ", ...");
			else
				m_logger.trace(getClass(), "create[peer " + Integer.toHexString(p_peer & 0xFFFF) + ", sizes(" + p_sizes.length + ") " + p_sizes[0] + ", ...] -> -1");
		}
		
		return chunkIDs;
	}

	/**
	 * Remove chunks/data structures from the storage.
	 * @param p_dataStructures Data structures to remove from the storage.
	 * @return Number of successfully removed data structures.
	 */
	public int remove(final DataStructure... p_dataStructures) {
		long[] chunkIDs = new long[p_dataStructures.length];
		for (int i = 0; i < chunkIDs.length; i++) {
			chunkIDs[i] = p_dataStructures[i].getID();
		}
		
		return remove(chunkIDs);
	}
	
	/**
	 * Remove chunks/data structures from the storage (by handle/ID).
	 * @param p_chunkIDs ChunkIDs/Handles of the data structures to remove. Invalid values are ignored.
	 * @return Number of successfully removed data structures.
	 */
	public int remove(final long... p_chunkIDs) {
		int chunksRemoved = 0;
		
		if (p_chunkIDs.length == 0)
			return chunksRemoved;
		
		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "remove[dataStructures(" + p_chunkIDs.length + ") " + Long.toHexString(p_chunkIDs[0]) + ", ...]");
		}
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not remove chunks");
		}
		
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_remove, p_chunkIDs.length);
		
		// sort by local and remote data first
		Map<Short, ArrayList<Long>> remoteChunksByPeers = new TreeMap<>();
		ArrayList<Long> localChunks = new ArrayList<Long>();

		m_memoryManager.lockAccess();
		for (int i = 0; i < p_chunkIDs.length; i++) {
			// invalid values allowed -> filter
			if (p_chunkIDs[i] == ChunkID.INVALID_ID) {
				continue;
			}
			
			if (m_memoryManager.exists(p_chunkIDs[i])) {
				// local
				localChunks.add(p_chunkIDs[i]);
			} else {
				// remote or migrated, figure out location and sort by peers
				Locations locations;
				
				locations = m_lookup.get(p_chunkIDs[i]);
				if (locations == null) {
					continue;
				} else {
					short peer = locations.getPrimaryPeer();

					ArrayList<Long> remoteChunksOfPeer = remoteChunksByPeers.get(peer);
					if (remoteChunksOfPeer == null) {
						remoteChunksOfPeer = new ArrayList<Long>();
						remoteChunksByPeers.put(peer, remoteChunksOfPeer);
					}
					remoteChunksOfPeer.add(p_chunkIDs[i]);
				}
			}
		}
		m_memoryManager.unlockAccess();

		// remove chunks from superpeer overlay first, so cannot be found before being deleted
		for (final Long chunkID : localChunks) {
			m_lookup.remove(chunkID);
		}
		
		// remove local chunkIDs
		m_memoryManager.lockManage();
		for (final Long chunkID : localChunks) {
			MemoryErrorCodes err = m_memoryManager.remove(chunkID);
			if (err == MemoryErrorCodes.SUCCESS) {
				chunksRemoved++;
			} else {
				m_logger.error(getClass(), "Removing chunk ID " + Long.toHexString(chunkID) + " failed: " + err);
			}
		}
		m_memoryManager.unlockManage();
		
		// go for remote ones by each peer
		for (final Entry<Short, ArrayList<Long>> peerWithChunks : remoteChunksByPeers.entrySet()) {
			short peer = peerWithChunks.getKey();
			ArrayList<Long> remoteChunks = peerWithChunks.getValue();

			if (peer == m_boot.getNodeID()) {
				// local remove, migrated data to current node
				m_memoryManager.lockManage();
				for (final Long chunkID : remoteChunks) {
					MemoryErrorCodes err = m_memoryManager.remove(chunkID);
					if (err == MemoryErrorCodes.SUCCESS) {
						chunksRemoved++;
					} else {
						m_logger.error(getClass(), "Removing chunk ID " + Long.toHexString(chunkID) + " failed: " + err);
					}
				}
				m_memoryManager.unlockManage();
			} else {					
				// Remote remove from specified peer
				RemoveRequest request = new RemoveRequest(peer, remoteChunks.toArray(new Long[0]));
				NetworkErrorCodes error = m_network.sendSync(request);
				if (error != NetworkErrorCodes.SUCCESS)
				{
					m_logger.error(getClass(), "Sending chunk remove request to peer " + Integer.toHexString(peer & 0xFFFF) + " failed: " + error);
					continue;
				}

				RemoveResponse response = request.getResponse(RemoveResponse.class);
				if (response != null) {
					byte[] statusCodes = response.getStatusCodes();
					// short cut if everything is ok
					if (statusCodes[0] == 2) {
						chunksRemoved += remoteChunks.size();
					} else {
						for (int i = 0; i < statusCodes.length; i++) {
							if (statusCodes[i] < 0) {
								m_logger.error(getClass(), "Remote removing chunk " + Long.toHexString(remoteChunks.get(i)) + " failed: " + statusCodes[i]);
							} else {
								chunksRemoved++;
							}
						}
					}
				}
			}
		}
		
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_remove);
		
		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "remove[dataStructures(" + p_chunkIDs.length + ") " + Long.toHexString(p_chunkIDs[0]) + ", ...] -> " + chunksRemoved);
		}
		
		return chunksRemoved;
	}
	
	/**
	 * Put/Update the contents of the provided data structures in the backend storage.
	 * @param p_dataStructres Data structures to put/update. Null values are ignored.
	 * @return Number of successfully updated data structures.
	 */
	public int put(final DataStructure... p_dataStructres) {
		return put(ChunkLockOperation.NO_LOCK_OPERATION, p_dataStructres);
	}
	
	/**
	 * Put/Update the contents of the provided data structures in the backend storage.
	 * @param p_chunkUnlockOperation Unlock operation to execute right after the put operation.
	 * @param p_dataStructures Data structures to put/update. Null values or chunks with invalid IDs are ignored.
	 * @return Number of successfully updated data structures.
	 */
	public int put(final ChunkLockOperation p_chunkUnlockOperation, DataStructure... p_dataStructures)
	{
		return put(p_chunkUnlockOperation, p_dataStructures, 0, p_dataStructures.length);
	}
	
	/**
	 * Put/Update the contents of the provided data structures in the backend storage.
	 * @param p_chunkUnlockOperation Unlock operation to execute right after the put operation.
	 * @param p_dataStructures Data structures to put/update. Null values or chunks with invalid IDs are ignored.
	 * @param p_offset Start offset within the array.
	 * @param p_count Number of items to put.
	 * @return Number of successfully updated data structures.
	 */
	public int put(final ChunkLockOperation p_chunkUnlockOperation, final DataStructure[] p_dataStructures, int p_offset, int p_count)
	{
		int chunksPut = 0;
		
		if (p_dataStructures.length == 0)
			return chunksPut;
		
		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "put[unlockOp " + p_chunkUnlockOperation + ", dataStructures(" + p_dataStructures.length + ") ...]");
		}
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not put chunks");
		}
		
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_put, p_count);
		
		Map<Short, ArrayList<DataStructure>> remoteChunksByPeers = new TreeMap<Short, ArrayList<DataStructure>>();
		
		// sort by local/remote chunks
		m_memoryManager.lockAccess();
		for (int i = 0; i < p_count; i++) {
			// filter null values
			if (p_dataStructures[i + p_offset] == null 
			|| p_dataStructures[i + p_offset].getID() == ChunkID.INVALID_ID) {
				continue;
			}
			
			// try to put every chunk locally, returns false if it does not exist
			// and saves us an additional check
			if (m_memoryManager.put(p_dataStructures[i + p_offset]) == MemoryErrorCodes.SUCCESS) {
				chunksPut++;
				
				// unlock chunk as well
				if (p_chunkUnlockOperation != ChunkLockOperation.NO_LOCK_OPERATION) {
					boolean writeLock = false;
					if (p_chunkUnlockOperation == ChunkLockOperation.WRITE_LOCK) {
						writeLock = true;
					}

					m_lock.unlock(p_dataStructures[i + p_offset].getID(), m_boot.getNodeID(), writeLock);
				}
			} else {
				// remote or migrated, figure out location and sort by peers
				Locations locations = m_lookup.get(p_dataStructures[i + p_offset].getID());
				if (locations == null) {
					continue;
				} else {
					short peer = locations.getPrimaryPeer();

					ArrayList<DataStructure> remoteChunksOfPeer = remoteChunksByPeers.get(peer);
					if (remoteChunksOfPeer == null) {
						remoteChunksOfPeer = new ArrayList<DataStructure>();
						remoteChunksByPeers.put(peer, remoteChunksOfPeer);
					}
					remoteChunksOfPeer.add(p_dataStructures[i + p_offset]);
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
					MemoryErrorCodes err = m_memoryManager.put(dataStructure);
					if (err == MemoryErrorCodes.SUCCESS)
						chunksPut++;
					else
						m_logger.error(getClass(), "Putting local chunk " + Long.toHexString(dataStructure.getID()) + " failed: " + err);
				}
				m_memoryManager.unlockAccess();
			} else {
				// Remote put
				ArrayList<DataStructure> chunksToPut = entry.getValue();
				PutRequest request = new PutRequest(peer, p_chunkUnlockOperation, chunksToPut.toArray(new DataStructure[chunksToPut.size()]));
				NetworkErrorCodes error = m_network.sendSync(request);
				if (error != NetworkErrorCodes.SUCCESS) {
					m_logger.error(getClass(), "Sending chunk put request to peer " + Integer.toHexString(peer & 0xFFFF) + " failed: " + error);
					
					// TODO
					//m_lookup.invalidate(dataStructure.getID());
					
					continue;
				}
			
				PutResponse response = request.getResponse(PutResponse.class);
				byte[] statusCodes = response.getStatusCodes();
				// try short cut, i.e. all puts successful
				if (statusCodes.length == 1 && statusCodes[0] == 1) {
					chunksPut += chunksToPut.size();
				} else {
					for (int i = 0; i < statusCodes.length; i++) {
						if (statusCodes[i] < 0) {
							m_logger.error(getClass(), "Remote put chunk " + Long.toHexString(chunksToPut.get(i).getID()) + " failed: " + statusCodes[i]);
						} else {
							chunksPut++;
						}
					}
				}
			}
		}		
			
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_put);		
		
		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "put[unlockOp " + p_chunkUnlockOperation + ", dataStructures(" + p_dataStructures.length + ") ...] -> " + chunksPut);
		}
		
		return chunksPut;
	}
	
	/**
	 * Get/Read the data stored in the backend storage into the provided data structures.
	 * @param p_dataStructures Data structures to read the stored data into. Null values or invalid IDs are ignored.
	 * @return Number of successfully read data structures.
	 */
	public int get(DataStructure... p_dataStructures) {
		return get(p_dataStructures, 0, p_dataStructures.length);
	}
	
	/**
	 * Get/Read the data stored in the backend storage into the provided data structures.
	 * @param p_dataStructures Array with data structures to read the stored data to. Null values or invalid IDs are ignored.
	 * @param p_offset Start offset within the array.
	 * @param p_count Number of elements to read.
	 * @return Number of successfully read data structures.
	 */
	public int get(final DataStructure[] p_dataStructures, int p_offset, int p_count) {
		int totalChunksGot = 0;
		
		if (p_dataStructures.length == 0) {
			return totalChunksGot;
		}
		
		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "get[dataStructures(" + p_count + ") ...]");
		}
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not get chunks");
		}
		
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_get, p_count);	

		// sort by local and remote data first
		Map<Short, ArrayList<DataStructure>> remoteChunksByPeers = new TreeMap<>();
		
		m_memoryManager.lockAccess();
		for (int i = 0; i < p_count; i++) {
			// filter null values
			if (p_dataStructures[i + p_offset] == null 
			|| p_dataStructures[i + p_offset].getID() == ChunkID.INVALID_ID) {
				continue;
			}
			
			// try to get locally, will check first if it exists and
			// returns false if it doesn't exist
			MemoryErrorCodes err = m_memoryManager.get(p_dataStructures[i + p_offset]);
			if (err == MemoryErrorCodes.SUCCESS) {
				totalChunksGot++;
			} else {
				// remote or migrated, figure out location and sort by peers
				Locations locations;
				
				locations = m_lookup.get(p_dataStructures[i + p_offset].getID());
				if (locations == null) {
					continue;
				} else {
					short peer = locations.getPrimaryPeer();

					ArrayList<DataStructure> remoteChunksOfPeer = remoteChunksByPeers.get(peer);
					if (remoteChunksOfPeer == null) {
						remoteChunksOfPeer = new ArrayList<DataStructure>();
						remoteChunksByPeers.put(peer, remoteChunksOfPeer);
					}
					remoteChunksOfPeer.add(p_dataStructures[i + p_offset]);
				}
			}
		}
		m_memoryManager.unlockAccess();

		// go for remote ones by each peer
		for (final Entry<Short, ArrayList<DataStructure>> peerWithChunks : remoteChunksByPeers.entrySet()) {
			short peer = peerWithChunks.getKey();
			ArrayList<DataStructure> remoteChunks = peerWithChunks.getValue();

			if (peer == m_boot.getNodeID()) {
				// local get, migrated data to current node
				m_memoryManager.lockAccess();
				for (final DataStructure dataStructure : remoteChunks) {
					MemoryErrorCodes err = m_memoryManager.get(dataStructure);
					if (err == MemoryErrorCodes.SUCCESS) {
						totalChunksGot++;
					} else {
						m_logger.error(getClass(), "Getting local chunk " + Long.toHexString(dataStructure.getID()) + " failed: " + err);
					}
				}
				m_memoryManager.unlockAccess();
			} else {					
				// Remote get from specified peer
				GetRequest request = new GetRequest(peer, remoteChunks.toArray(new DataStructure[remoteChunks.size()]));
				NetworkErrorCodes error = m_network.sendSync(request);
				if (error != NetworkErrorCodes.SUCCESS)
				{
					m_logger.error(getClass(), "Sending chunk get request to peer " + peer + " failed: " + error);
					continue;
				}

				GetResponse response = request.getResponse(GetResponse.class);
				if (response != null) {
					if (response.getNumberOfChunksGot() != remoteChunks.size())
					{
						// TODO not all chunks were found
						m_logger.warn(getClass(), "Could not find all chunks on peer " + Integer.toHexString(peer & 0xFFFF) + " for chunk request.");
					}

					totalChunksGot += response.getNumberOfChunksGot();
				}
			}
		}
		

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_get);	
		
		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "get[dataStructures(" + p_dataStructures.length + ") ...] -> " + totalChunksGot);
		}
		
		return totalChunksGot;
	}
	
	public ArrayList<Long> getAllLocalChunkIDRanges() {
		ArrayList<Long> list = null;
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must have local chunk ID ranges");
		}
		
		m_memoryManager.lockAccess();
		list = m_memoryManager.getCIDrangesOfAllLocalChunks();
		m_memoryManager.unlockAccess();
		
		return list;
	}
	
	public ArrayList<Long> getAllLocalChunkIDRanges(final short p_nodeID) {
		ArrayList<Long> list = null;
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must have local chunk ID ranges");
		}
		
		if (p_nodeID == m_boot.getNodeID()) {
			list = getAllLocalChunkIDRanges();
		} else {
			
		}
		
		return list;
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "Entering incomingMessage with: p_message=" + p_message);
		}
		
		if (p_message != null) {
			if (p_message.getType() == ChunkMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case ChunkMessages.SUBTYPE_GET_REQUEST:
					incomingGetRequest((GetRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_PUT_REQUEST:
					incomingPutRequest((PutRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_REMOVE_REQUEST:
					incomingRemoveRequest((RemoveRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_CREATE_REQUEST:
					incomingCreateRequest((CreateRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_STATUS_REQUEST:
					incomingStatusRequest((StatusRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST:
					incomingGetLocalChunkIDRangesRequest((GetLocalChunkIDRangesRequest) p_message);
					break;
				default:
					break;
				}
			}
		}

		if (!m_removePerformanceCriticalSection) {
			m_logger.trace(getClass(), "Exiting incomingMessage");
		}
	}
	
	// -----------------------------------------------------------------------------------
	
	/**
	 * Register network messages we use in here.
	 */
	private void registerNetworkMessages()
	{
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_GET_REQUEST, GetRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_GET_RESPONSE, GetResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_PUT_REQUEST, PutRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_PUT_RESPONSE, PutResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_REMOVE_REQUEST, RemoveRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_REMOVE_RESPONSE, RemoveResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_CREATE_REQUEST, CreateRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_CREATE_RESPONSE, CreateResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_STATUS_REQUEST, StatusRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_STATUS_RESPONSE, StatusResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST, GetLocalChunkIDRangesRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_RESPONSE, GetLocalChunkIDRangesResponse.class);
	}
	
	/**
	 * Register network messages we want to listen to in here.
	 */
	private void registerNetworkMessageListener()
	{
		m_network.register(GetRequest.class, this);
		m_network.register(PutRequest.class, this);
		m_network.register(RemoveRequest.class, this);
		m_network.register(CreateRequest.class, this);
		m_network.register(StatusRequest.class, this);
		m_network.register(GetLocalChunkIDRangesRequest.class, this);
	}
	
	/**
	 * Register statistics operations for this service.
	 */
	private void registerStatisticsOperations() 
	{
		m_statisticsRecorderIDs = new ChunkStatisticsRecorderIDs();
		m_statisticsRecorderIDs.m_id = m_statistics.createRecorder(this.getClass());
		
		m_statisticsRecorderIDs.m_operations.m_create = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_CREATE);
		m_statisticsRecorderIDs.m_operations.m_remoteCreate = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_REMOTE_CREATE);
		m_statisticsRecorderIDs.m_operations.m_size = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_SIZE);
		m_statisticsRecorderIDs.m_operations.m_get = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_GET);
		m_statisticsRecorderIDs.m_operations.m_put = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_PUT);
		m_statisticsRecorderIDs.m_operations.m_remove = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_REMOVE);
		m_statisticsRecorderIDs.m_operations.m_incomingCreate = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_INCOMING_CREATE);
		m_statisticsRecorderIDs.m_operations.m_incomingGet = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_INCOMING_GET);
		m_statisticsRecorderIDs.m_operations.m_incomingPut = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_INCOMING_PUT);
		m_statisticsRecorderIDs.m_operations.m_incomingRemove = m_statistics.createOperation(m_statisticsRecorderIDs.m_id, ChunkStatisticsRecorderIDs.Operations.MS_REMOVE);
	}
	
	// -----------------------------------------------------------------------------------
	
	/**
	 * Initializes the backup range for current locations
	 * and determines new backup peers if necessary
	 * @param p_localID
	 *            the current LocalID
	 * @param p_size
	 *            the size of the new created chunk
	 * @throws LookupException
	 *             if range could not be initialized
	 */
	private void initBackupRange(final long p_localID, final int p_size) {
		int size;

		// TODO Kevin: fix this
		if (m_logActive) {
//			size = p_size + m_log.getAproxHeaderSize(m_nodeID, p_localID, p_size);
//			if (!m_firstRangeInitialized && p_localID == 1) {
//				// First Chunk has LocalID 1, but there is a Chunk with LocalID 0 for hosting the name service
//				// This is the first put and p_localID is not reused
//				determineBackupPeers(0);
//				m_lookup.initRange((long) m_nodeID << 48, new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
//				m_log.initBackupRange((long) m_nodeID << 48, m_currentBackupRange.getBackupPeers());
//				m_rangeSize = size;
//				m_firstRangeInitialized = true;
//			} else if (m_rangeSize + size > SECONDARY_LOG_SIZE / 2) {
//				determineBackupPeers(p_localID);
//				m_lookup.initRange(((long) m_nodeID << 48) + p_localID, new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
//				m_log.initBackupRange(((long) m_nodeID << 48) + p_localID, m_currentBackupRange.getBackupPeers());
//				m_rangeSize = size;
//			} else {
//				m_rangeSize += size;
//			}
		} else if (!m_firstBackupRangeInitialized && p_localID == 1) {
			short nodeId = m_boot.getNodeID();
			m_lookup.initRange(((long) nodeId << 48) + 0xFFFFFFFFFFFFL, new Locations(nodeId, new short[] {-1, -1, -1}, null));
			m_firstBackupRangeInitialized = true;
		}
	}
	
	/**
	 * Handles an incoming GetRequest
	 * @param p_request
	 *            the GetRequest
	 */
	private void incomingGetRequest(final GetRequest p_request) {

		long[] chunkIDs = p_request.getChunkIDs();
		DataStructure[] chunks = new DataStructure[chunkIDs.length];
		int numChunksGot = 0;

		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingGet, p_request.getChunkIDs().length);
		
		m_memoryManager.lockAccess();
		for (int i = 0; i < chunks.length; i++) {
			// also does exist check
			int size = m_memoryManager.getSize(chunkIDs[i]);
			if (size < 0) {
				m_logger.warn(getClass(), "Getting size of chunk " + Long.toHexString(chunkIDs[i]) + " failed, does not exist.");
				size = 0;
			} else {
				numChunksGot++;
			}
			
			chunks[i] = new Chunk(chunkIDs[i], size);
			m_memoryManager.get(chunks[i]);
		}
		m_memoryManager.unlockAccess();

		GetResponse response = new GetResponse(p_request, numChunksGot, chunks);
		NetworkErrorCodes error = m_network.sendMessage(response);
		if (error != NetworkErrorCodes.SUCCESS)
		{
			m_logger.error(getClass(), "Sending GetResponse for " + numChunksGot + " chunks failed: " + error);
		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingGet);	
	}

	/**
	 * Handles an incoming PutRequest
	 * @param p_request
	 *            the PutRequest
	 */
	private void incomingPutRequest(final PutRequest p_request) {		
		DataStructure[] chunks = p_request.getDataStructures();
		byte[] statusChunks = new byte[chunks.length];
		boolean allSuccessful = true;
		
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingPut, chunks.length);
		
		m_memoryManager.lockAccess();
		for (int i = 0; i < chunks.length; i++) {
			MemoryErrorCodes err = m_memoryManager.put(chunks[i]);
			if (err != MemoryErrorCodes.SUCCESS) {
				// does not exist (anymore)
				statusChunks[i] = -1;
				m_logger.warn(getClass(), "Putting chunk " + Long.toHexString(chunks[i].getID()) + " failed, does not exist.");
				allSuccessful = false;
			} else {
				// put successful
				statusChunks[i] = 0;
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
		
		PutResponse response = null;
		// cut message length if all were successful
		if (allSuccessful){
			response = new PutResponse(p_request, (byte) 1);
		} else {
			// we got errors, default message
			response = new PutResponse(p_request, statusChunks);
		}
		
		NetworkErrorCodes error = m_network.sendMessage(response);
		if (error != NetworkErrorCodes.SUCCESS)
		{
			m_logger.error(getClass(), "Sending chunk put respond to request " + p_request + " failed: " + error);
		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingPut);
	}

	/**
	 * Handles an incoming RemoveRequest
	 * @param p_request
	 *            the RemoveRequest
	 */
	private void incomingRemoveRequest(final RemoveRequest p_request) {
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingRemove, p_request.getChunkIDs().length);

		Long[] chunkIDs = p_request.getChunkIDs();
		byte[] chunkStatusCodes = new byte[chunkIDs.length];
		boolean allSuccessful = true;
		
		// remove chunks from superpeer overlay first, so cannot be found before being deleted
		for (int i = 0; i < chunkIDs.length; i++)  {
			m_lookup.remove(chunkIDs[i]);
		}
		
		// remove chunks first (local)
		m_memoryManager.lockManage();
		for (int i = 0; i < chunkIDs.length; i++) 
		{
			MemoryErrorCodes err = m_memoryManager.remove(chunkIDs[i]);
			if (err == MemoryErrorCodes.SUCCESS) {
				// remove successful
				chunkStatusCodes[i] = 0;
			} else {
				// remove failed, might be removed recently by someone else
				chunkStatusCodes[i] = -1;
				allSuccessful = false;
			}
		}
		m_memoryManager.unlockManage();

		RemoveResponse response = null;
		if (allSuccessful) {
			// use a short version to indicate everything is ok
			response = new RemoveResponse(p_request, (byte) 2);
		} else {
			// errors occured, send full status report
			response = new RemoveResponse(p_request, chunkStatusCodes);
		}
		
		NetworkErrorCodes error = m_network.sendMessage(response);
		if (error != NetworkErrorCodes.SUCCESS)
		{
			m_logger.error(getClass(), "Sending chunk remove respond to request " + p_request + " failed: " + error);
		}	
		
		// TODO for migrated chunks, send remove request to peer currently holding the chunk data
//		for (int i = 0; i < chunkIDs.length; i++) {
//			byte rangeID = m_backup.getBackupRange(chunkIDs[i]);
//			short[] backupPeers = m_backup.getBackupPeersForLocalChunks(chunkIDs[i]);
//			m_backup.removeChunk(chunkIDs[i]);
//			
//			if (m_memoryManager.dataWasMigrated(chunkIDs[i])) {	
//				// Inform peer who got the migrated data about removal
//				RemoveRequest request = new RemoveRequest(ChunkID.getCreatorID(chunkIDs[i]), new Chunk(chunkIDs[i], 0));
//				try {
//					request.sendSync(m_network);
//					request.getResponse(RemoveResponse.class);
//				} catch (final NetworkException e) {
//					LOGGER.error("Informing creator about removal of chunk " + chunkIDs[i] + " failed.", e);
//				}
//			}
//		}

		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingRemove);
	}
	
	/**
	 * Handle incoming create requests.
	 * @param p_request Request to handle
	 */
	private void incomingCreateRequest(final CreateRequest p_request) {
		m_statistics.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingCreate, p_request.getSizes().length);
		
		int[] sizes = p_request.getSizes();
		long[] chunkIDs = new long[sizes.length];
		
		m_memoryManager.lockManage();
		for (int i = 0; i < sizes.length; i++) {
			chunkIDs[i] = m_memoryManager.create(sizes[i]);
		}
		m_memoryManager.unlockManage();
		
		// tell the superpeer overlay about our newly created chunks, otherwise they can not be found
		// by other peers
		for (int i = 0; i < sizes.length; i++) {
			m_lookup.initRange(chunkIDs[i], new Locations(m_boot.getNodeID(), new short[] {-1, -1, -1}, null));	
		}
		
		CreateResponse response = new CreateResponse(p_request, chunkIDs);
		NetworkErrorCodes error = m_network.sendMessage(response);
		if (error != NetworkErrorCodes.SUCCESS)
		{
			m_logger.error(getClass(), "Sending chunk create respond to request " + p_request + " failed: " + error);
		}
		
		
		m_statistics.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_incomingCreate);
	}
	
	/**
	 * Handle incoming status requests.
	 * @param p_request Request to handle
	 */
	private void incomingStatusRequest(final StatusRequest p_request) {
		Status status = getStatus();
		
		StatusResponse response = new StatusResponse(p_request, status);
		NetworkErrorCodes error = m_network.sendMessage(response);
		if (error != NetworkErrorCodes.SUCCESS)
		{
			m_logger.error(getClass(), "Sending status respond to request " + p_request + " failed: " + error);
		}
	}
	
	private void incomingGetLocalChunkIDRangesRequest(final GetLocalChunkIDRangesRequest p_request) {
		ArrayList<Long> cidRangesLocalChunks = null;
		
		m_memoryManager.lockAccess();
		cidRangesLocalChunks = m_memoryManager.getCIDrangesOfAllLocalChunks();
		m_memoryManager.unlockAccess();
		
		if (cidRangesLocalChunks == null) {
			cidRangesLocalChunks = new ArrayList<Long>(0);
			m_logger.error(getClass(), "Getting local chunk id ranges failed, sending back empty range.");		
		}
		
		GetLocalChunkIDRangesResponse response = new GetLocalChunkIDRangesResponse(p_request, cidRangesLocalChunks);
		NetworkErrorCodes error = m_network.sendMessage(response);
		if (error != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Responding to local chunk id ranges request " + p_request + " failed: " + error);
		}
	}
	
	/**
	 * Status object for the chunk service containing various information
	 * about it.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.03.16
	 */
	public static class Status implements Importable, Exportable
	{
		private long m_freeMemoryBytes = -1;
		private long m_totalMemoryBytes = -1;
		private long m_numberOfActiveChunks = -1;
		
		/**
		 * Default constructor
		 */
		public Status()
		{
			
		}
		
		/**
		 * Get the amount of free memory in bytes.
		 * @return Free memory in bytes.
		 */
		public long getFreeMemory()
		{
			return m_freeMemoryBytes;
		}
		
		/**
		 * Get the total amount of memory in bytes available.
		 * @return Total amount of memory in bytes.
		 */
		public long getTotalMemory()
		{
			return m_totalMemoryBytes;
		}
		
		/**
		 * Get number of currently active/allocated chunks.
		 * @return Number of active chunks.
		 */
		public long getNumberOfActiveChunks() {
			return m_numberOfActiveChunks;
		}

		@Override
		public int sizeofObject() {
			return Long.BYTES * 3;
		}

		@Override
		public boolean hasDynamicObjectSize() {
			return false;
		}

		@Override
		public int exportObject(Exporter p_exporter, int p_size) {
			p_exporter.writeLong(m_freeMemoryBytes);
			p_exporter.writeLong(m_totalMemoryBytes);
			p_exporter.writeLong(m_numberOfActiveChunks);
			return sizeofObject();
		}

		@Override
		public int importObject(Importer p_importer, int p_size) {
			m_freeMemoryBytes = p_importer.readLong();
			m_totalMemoryBytes = p_importer.readLong();
			m_numberOfActiveChunks = p_importer.readLong();
			return sizeofObject();
		}
	}
}

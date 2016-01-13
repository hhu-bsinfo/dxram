package de.hhu.bsinfo.dxram.chunk;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.boot.NodeRole;
import de.hhu.bsinfo.dxram.chunk.ChunkStatistic.Operation;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.CreateRequest;
import de.hhu.bsinfo.dxram.chunk.messages.CreateResponse;
import de.hhu.bsinfo.dxram.chunk.messages.GetRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetResponse;
import de.hhu.bsinfo.dxram.chunk.messages.PutRequest;
import de.hhu.bsinfo.dxram.chunk.messages.PutResponse;
import de.hhu.bsinfo.dxram.chunk.messages.RemoveRequest;
import de.hhu.bsinfo.dxram.chunk.messages.RemoveResponse;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.Locations;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent.ErrorCode;
import de.hhu.bsinfo.dxram.util.ChunkID;
import de.hhu.bsinfo.dxram.util.ChunkLockOperation;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;
import de.hhu.bsinfo.utils.StatisticsManager;

public class ChunkService extends DXRAMService implements MessageReceiver
{
	private BootComponent m_boot = null;
	private LoggerComponent m_logger = null;
	private MemoryManagerComponent m_memoryManager = null;
	private NetworkComponent m_network = null;
	private LookupComponent m_lookup = null;
//	private LockComponent m_lock = null;
//	private BackupComponent m_backup = null;
	
	private boolean m_statisticsEnabled = false;
	
	public ChunkService() {
		super();
	}
	
	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {
		p_settings.setDefaultValue(ChunkConfigurationValues.Service.STATISTICS);
	}
	
	@Override
	protected boolean startService(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) 
	{
		m_boot = getComponent(BootComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_memoryManager = getComponent(MemoryManagerComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_lookup = getComponent(LookupComponent.class);
		
		m_statisticsEnabled = p_settings.getValue(ChunkConfigurationValues.Service.STATISTICS);

		registerNetworkMessages();
		registerNetworkMessageListener();

//		if (getSystemData().getNodeRole().equals(NodeRole.PEER)) {
//			m_backup.registerPeer();
//		}

		if (m_statisticsEnabled) {
			StatisticsManager.registerStatistic("Chunk", ChunkStatistic.getInstance());
		}
		
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
		
		m_logger.trace(getClass(), "create[size " + p_size + ", count " + p_count + "]");
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not create chunks");
			return null;
		} 
		
		if (m_statisticsEnabled) {
			Operation.MULTI_CREATE.enter();
		}
			
		
		chunkIDs = new long[p_count];

		m_memoryManager.lockManage();
		// keep loop tight and execute everything
		// that we don't have to lock outside of this section
		for (int i = 0; i < p_count; i++) {
			chunkIDs[i] = m_memoryManager.create(p_size);		
		}
		m_memoryManager.unlockManage();
		

		if (m_statisticsEnabled) {
			Operation.MULTI_CREATE.leave();
		}
		
		m_logger.trace(getClass(), "create[size " + p_size + ", count " + p_count + "] -> " + Long.toHexString(chunkIDs[0]) + ", ...");
		
		return chunkIDs;
	}
	
	public long[] create(final int... p_sizes) {
		long[] chunkIDs = null;
		
		if (p_sizes.length == 0)
			return new long[0];

		m_logger.trace(getClass(), "create[sizes(" + p_sizes.length + ") " + p_sizes[0] + ", ...]");
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not create chunks");
			return null;
		} 
		
		if (m_statisticsEnabled) {
			Operation.MULTI_CREATE.enter();
		}
			
		
		chunkIDs = new long[p_sizes.length];

		m_memoryManager.lockManage();
		// keep loop tight and execute everything
		// that we don't have to lock outside of this section
		for (int i = 0; i < p_sizes.length; i++) {
			chunkIDs[i] = m_memoryManager.create(p_sizes[i]);		
		}
		m_memoryManager.unlockManage();
		

		if (m_statisticsEnabled) {
			Operation.MULTI_CREATE.leave();
		}
		
		m_logger.trace(getClass(), "create[sizes(" + p_sizes.length + ") " + p_sizes[0] + ", ...] -> " + Long.toHexString(chunkIDs[0]) + ", ...");
		
		return chunkIDs;
	}

	// remote create
	public long[] create(final short p_peer, final int... p_sizes) {
		long[] chunkIDs = null;
		
		if (p_sizes.length == 0)
			return new long[0];

		m_logger.trace(getClass(), "create[peer " + Integer.toHexString(p_peer & 0xFFFF) + ", sizes(" + p_sizes.length + ") " + p_sizes[0] + ", ...]");
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not create chunks");
		}
		
		if (m_statisticsEnabled) {
			Operation.MULTI_CREATE.enter();
		}
			

		CreateRequest request = new CreateRequest(p_peer, p_sizes);
		ErrorCode error = m_network.sendSync(request);
		if (error != ErrorCode.SUCCESS)
		{
			m_logger.error(getClass(), "Sending chunk create request to peer " + Integer.toHexString(p_peer & 0xFFFF) + " failed: " + error);
		} else {
			CreateResponse response = request.getResponse(CreateResponse.class);
			chunkIDs = response.getChunkIDs();
		}
		

		if (m_statisticsEnabled) {
			Operation.MULTI_CREATE.leave();
		}
		
		m_logger.trace(getClass(), "create[peer " + Integer.toHexString(p_peer & 0xFFFF) + ", sizes(" + p_sizes.length + ") " + Long.toHexString(p_sizes[0]) + ", ...] -> " + chunkIDs[0] + ", ...");
		
		return chunkIDs;
	}
	
	public int remove(final DataStructure[] p_dataStructures) {
		int chunksRemoved = 0;
		
		if (p_dataStructures.length == 0)
			return chunksRemoved;
		
		m_logger.trace(getClass(), "remove[dataStructures(" + p_dataStructures.length + ") " + Long.toHexString(p_dataStructures[0].getID()) + ", ...]");
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not remove chunks");
		}
		
		if (m_statisticsEnabled) {
			Operation.REMOVE.enter();
		}
		
		
		// sort by local and remote data first
		Map<Short, ArrayList<DataStructure>> remoteChunksByPeers = new TreeMap<>();
		ArrayList<DataStructure> localChunks = new ArrayList<DataStructure>();
		m_memoryManager.lockAccess();
		for (int i = 0; i < p_dataStructures.length; i++) {
			if (m_memoryManager.exists(p_dataStructures[i].getID())) {
				// local
				localChunks.add(p_dataStructures[i]);
			} else {
				// remote, figure out location and sort by peers
				Locations locations;
				
				locations = m_lookup.get(p_dataStructures[i].getID());
				if (locations == null) {
					continue;
				} else {
					short peer = locations.getPrimaryPeer();

					ArrayList<DataStructure> remoteChunksOfPeer = remoteChunksByPeers.get(peer);
					if (remoteChunksOfPeer == null) {
						remoteChunksOfPeer = new ArrayList<DataStructure>();
						remoteChunksByPeers.put(peer, remoteChunksOfPeer);
					}
					remoteChunksOfPeer.add(p_dataStructures[i]);
				}
			}
		}
		m_memoryManager.unlockAccess();

		// remove local chunkIDs
		m_memoryManager.lockManage();
		for (final DataStructure chunk : localChunks) {
			if (m_memoryManager.remove(chunk.getID())) {
				chunksRemoved++;
			} else {
				m_logger.error(getClass(), "Removing chunk ID " + Long.toHexString(chunk.getID()) + " failed.");
			}
		}
		m_memoryManager.unlockManage();
		
		// go for remote ones by each peer
		for (final Entry<Short, ArrayList<DataStructure>> peerWithChunks : remoteChunksByPeers.entrySet()) {
			short peer = peerWithChunks.getKey();
			ArrayList<DataStructure> remoteChunks = peerWithChunks.getValue();

			if (peer == m_boot.getNodeID()) {
				// local remove, migrated data to current node
				m_memoryManager.lockManage();
				for (final DataStructure chunk : remoteChunks) {
					if (m_memoryManager.remove(chunk.getID())) {
						chunksRemoved++;
					} else {
						m_logger.error(getClass(), "Removing chunk ID " + Long.toHexString(chunk.getID()) + " failed.");
					}
				}
				m_memoryManager.unlockManage();
			} else {					
				// Remote remove from specified peer
				RemoveRequest request = new RemoveRequest(peer, (DataStructure[]) remoteChunks.toArray());
				ErrorCode error = m_network.sendSync(request);
				if (error != ErrorCode.SUCCESS)
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
								m_logger.error(getClass(), "Remote removing chunk " + Long.toHexString(remoteChunks.get(i).getID()) + " failed: " + statusCodes[i]);
							} else {
								chunksRemoved++;
							}
						}
					}
				}
			}
		}
		
		if (m_statisticsEnabled) {
			Operation.REMOVE.leave();
		}
		
		m_logger.trace(getClass(), "remove[dataStructures(" + p_dataStructures.length + ") " + Long.toHexString(p_dataStructures[0].getID()) + ", ...] -> " + chunksRemoved);
		
		return chunksRemoved;
	}
	
	public int put(final DataStructure... p_dataStructres) {
		return put(ChunkLockOperation.NO_LOCK_OPERATION, p_dataStructres);
	}
	
	public int put(final ChunkLockOperation p_chunkUnlockOperation, DataStructure... p_dataStructures)
	{
		int chunksPut = 0;
		
		if (p_dataStructures.length == 0)
			return chunksPut;
		
		m_logger.trace(getClass(), "put[unlockOp " + p_chunkUnlockOperation + ", dataStructures(" + p_dataStructures.length + ") " + Long.toHexString(p_dataStructures[0].getID()) + ", ...]");
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not put chunks");
		}
		
		if (m_statisticsEnabled) {
			Operation.PUT.enter();
		}	
		
		ArrayList<DataStructure> localChunks = new ArrayList<DataStructure>();
		Map<Short, ArrayList<DataStructure>> remoteChunksByPeers = new TreeMap<Short, ArrayList<DataStructure>>();
		
		// sort by local/remote chunks
		m_memoryManager.lockAccess();
		for (DataStructure dataStructure : p_dataStructures) {
			if (m_memoryManager.exists(dataStructure.getID())) {
				localChunks.add(dataStructure);
			} else {
				// remote, figure out location and sort by peers
				Locations locations = m_lookup.get(dataStructure.getID());
				if (locations == null) {
					continue;
				} else {
					short peer = locations.getPrimaryPeer();

					ArrayList<DataStructure> remoteChunksOfPeer = remoteChunksByPeers.get(peer);
					if (remoteChunksOfPeer == null) {
						remoteChunksOfPeer = new ArrayList<DataStructure>();
						remoteChunksByPeers.put(peer, remoteChunksOfPeer);
					}
					remoteChunksOfPeer.add(dataStructure);
				}
			}
		}
		
		// have local puts first
		for (DataStructure dataStructure : localChunks) {
			if (m_memoryManager.put(dataStructure))
				chunksPut++;
			else
				m_logger.error(getClass(), "Putting local chunk " + Long.toHexString(dataStructure.getID()) + " failed.");
		}
		m_memoryManager.unlockAccess();
		
		// TODO unlock chunks	
	
		// go for remote chunks
		for (Entry<Short, ArrayList<DataStructure>> entry : remoteChunksByPeers.entrySet()) {
			short peer = entry.getKey();
			
			if (peer == m_boot.getNodeID()) {
				// local put, migrated data to current node
				m_memoryManager.lockAccess();
				for (final DataStructure dataStructure : entry.getValue()) {
					if (m_memoryManager.put(dataStructure))
						chunksPut++;
					else
						m_logger.error(getClass(), "Putting local chunk " + Long.toHexString(dataStructure.getID()) + " failed.");
				}
				m_memoryManager.unlockAccess();
			} else {
				// Remote put
				ArrayList<DataStructure> chunksToPut = entry.getValue();
				PutRequest request = new PutRequest(peer, p_chunkUnlockOperation, (DataStructure[]) chunksToPut.toArray());
				ErrorCode error = m_network.sendSync(request);
				if (error != ErrorCode.SUCCESS) {
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
			
		if (m_statisticsEnabled)
			Operation.PUT.leave();		
		
		m_logger.trace(getClass(), "put[unlockOp " + p_chunkUnlockOperation + ", dataStructures(" + p_dataStructures.length + ") " + Long.toHexString(p_dataStructures[0].getID()) + ", ...] -> " + chunksPut);
		
		return chunksPut;
	}

	public int get(final DataStructure... p_dataStructures) {
		return get(ChunkLockOperation.NO_LOCK_OPERATION, p_dataStructures);
	}
	
	public int get(final ChunkLockOperation p_chunkLockOperation, DataStructure... p_dataStructures) {
		int totalChunksGot = 0;
		
		if (p_dataStructures.length == 0)
			return totalChunksGot;
		
		m_logger.trace(getClass(), "get[lockOp " + p_chunkLockOperation + ", dataStructures(" + p_dataStructures.length + ") " + Long.toHexString(p_dataStructures[0].getID()) + ", ...]");
		
		if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			m_logger.error(getClass(), "a superpeer must not get chunks");
		}
		
		if (m_statisticsEnabled) {
			Operation.GET.enter();
		}	

		// sort by local and remote data first
		Map<Short, ArrayList<DataStructure>> remoteChunksByPeers = new TreeMap<>();
		ArrayList<DataStructure> localChunks = new ArrayList<DataStructure>();
		m_memoryManager.lockAccess();
		for (int i = 0; i < p_dataStructures.length; i++) {
			if (m_memoryManager.exists(p_dataStructures[i].getID())) {
				// local
				localChunks.add(p_dataStructures[i]);
			} else {
				// remote, figure out location and sort by peers
				Locations locations;
				
				locations = m_lookup.get(p_dataStructures[i].getID());
				if (locations == null) {
					continue;
				} else {
					short peer = locations.getPrimaryPeer();

					ArrayList<DataStructure> remoteChunksOfPeer = remoteChunksByPeers.get(peer);
					if (remoteChunksOfPeer == null) {
						remoteChunksOfPeer = new ArrayList<DataStructure>();
						remoteChunksByPeers.put(peer, remoteChunksOfPeer);
					}
					remoteChunksOfPeer.add(p_dataStructures[i]);
				}
			}
		}

		// get local chunkIDs
		for (final DataStructure dataStructure : localChunks) {
			if (m_memoryManager.get(dataStructure)) {
				totalChunksGot++;
			} else {
				m_logger.error(getClass(), "Getting local chunk " + Long.toHexString(dataStructure.getID()) + " failed.");
			}
		}
		m_memoryManager.unlockAccess();
			
		// TODO p_aquireLock for local chunks

		// go for remote ones by each peer
		for (final Entry<Short, ArrayList<DataStructure>> peerWithChunks : remoteChunksByPeers.entrySet()) {
			short peer = peerWithChunks.getKey();
			ArrayList<DataStructure> remoteChunks = peerWithChunks.getValue();

			if (peer == m_boot.getNodeID()) {
				// local get, migrated data to current node
				m_memoryManager.lockAccess();
				for (final DataStructure dataStructure : remoteChunks) {
					if (m_memoryManager.get(dataStructure)) {
						totalChunksGot++;
					} else {
						m_logger.error(getClass(), "Getting local chunk " + Long.toHexString(dataStructure.getID()) + " failed.");
					}
				}
				m_memoryManager.unlockAccess();
			} else {					
				// Remote get from specified peer
				GetRequest request = new GetRequest(peer, p_chunkLockOperation, (DataStructure[]) remoteChunks.toArray());
				ErrorCode error = m_network.sendSync(request);
				if (error != ErrorCode.SUCCESS)
				{
					m_logger.error(getClass(), "Sending chunk get request to peer " + Integer.toHexString(peer & 0xFFFF) + " failed: " + error);
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
		

		if (m_statisticsEnabled)
			Operation.MULTI_GET.leave();
		
		m_logger.trace(getClass(), "get[lockOp " + p_chunkLockOperation + ", dataStructures(" + p_dataStructures.length + ") " + Long.toHexString(p_dataStructures[0].getID()) + ", ...] -> " + totalChunksGot);

		return totalChunksGot;
	}


	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		m_logger.trace(getClass(), "Entering incomingMessage with: p_message=" + p_message);

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
				default:
					break;
				}
			}
		}

		m_logger.trace(getClass(), "Exiting incomingMessage");
	}
	
	// -----------------------------------------------------------------------------------
	
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
	}
	
	private void registerNetworkMessageListener()
	{
		m_network.register(GetResponse.class, this);
		m_network.register(PutResponse.class, this);
		m_network.register(RemoveResponse.class, this);
		m_network.register(CreateResponse.class, this);
	}
	
	// -----------------------------------------------------------------------------------
	
	/**
	 * Handles an incoming GetRequest
	 * @param p_request
	 *            the GetRequest
	 */
	private void incomingGetRequest(final GetRequest p_request) {

		if (m_statisticsEnabled)
			Operation.INCOMING_GET.enter();

		long[] chunkIDs = p_request.getChunkIDs();
		DataStructure[] chunks = new DataStructure[chunkIDs.length];
		int numChunksGot = 0;
		
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
		ErrorCode error = m_network.sendMessage(response);
		if (error != ErrorCode.SUCCESS)
		{
			m_logger.error(getClass(), "Sending GetResponse for " + numChunksGot + " chunks failed: " + error);
		}

		if (m_statisticsEnabled)
			Operation.INCOMING_GET.leave();
	}

	/**
	 * Handles an incoming PutRequest
	 * @param p_request
	 *            the PutRequest
	 */
	private void incomingPutRequest(final PutRequest p_request) {		
		if (m_statisticsEnabled)
			Operation.INCOMING_PUT.enter();

		DataStructure[] chunks = p_request.getDataStructures();
		byte[] statusChunks = new byte[chunks.length];
		boolean allSuccessful = true;
		
		m_memoryManager.lockAccess();
		for (int i = 0; i < chunks.length; i++) {
			if (m_memoryManager.exists(chunks[i].getID())) {
				if (!m_memoryManager.put(chunks[i])) {
					// does not exist (anymore)
					statusChunks[i] = -1;
					m_logger.warn(getClass(), "Putting chunk " + Long.toHexString(chunks[i].getID()) + " failed, does not exist.");
					allSuccessful = false;
				} else {
					// put successful
					statusChunks[i] = 0;
				}
			} else {
				// got migrated, not responsible anymore
				statusChunks[i] = -2;
				m_logger.warn(getClass(), "Putting chunk " + Long.toHexString(chunks[i].getID()) + " failed, was migrated.");
				allSuccessful = false;
			}
		}
		m_memoryManager.unlockAccess();
		
		PutResponse response = null;
		// cut message length if all were successful
		if (allSuccessful){
			response = new PutResponse(p_request, (byte) 1);
		} else {
			// we got errors, default message
			response = new PutResponse(p_request, statusChunks);
		}
		
		ErrorCode error = m_network.sendMessage(response);
		if (error != ErrorCode.SUCCESS)
		{
			m_logger.error(getClass(), "Sending chunk put respond to request " + p_request + " failed: " + error);
		}

		if (m_statisticsEnabled)
			Operation.INCOMING_PUT.leave();
	}

	/**
	 * Handles an incoming RemoveRequest
	 * @param p_request
	 *            the RemoveRequest
	 */
	private void incomingRemoveRequest(final RemoveRequest p_request) {
		if (m_statisticsEnabled)
			Operation.INCOMING_REMOVE.enter();

		long[] chunkIDs = p_request.getChunkIDs();
		byte[] chunkStatusCodes = new byte[chunkIDs.length];
		boolean allSuccessful = true;
		
		// remove chunks first (local)
		m_memoryManager.lockManage();
		for (int i = 0; i < chunkIDs.length; i++) 
		{
			if (m_memoryManager.exists(chunkIDs[i])) {
				if (m_memoryManager.remove(chunkIDs[i])) {
					// remove successful
					chunkStatusCodes[i] = 0;
				} else {
					// remove failed
					chunkStatusCodes[i] = -1;
					allSuccessful = false;
				}
			} else if (ChunkID.getCreatorID(chunkIDs[i]) == m_boot.getNodeID()) {
				// chunk data was migrated, "migrate back" id
				m_memoryManager.prepareChunkIDForReuse(chunkIDs[i]);
				chunkStatusCodes[i] = 1;
			} else {
				// remove failed, does not exist
				chunkStatusCodes[i] = -2;
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
		
		ErrorCode error = m_network.sendMessage(response);
		if (error != ErrorCode.SUCCESS)
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

		if (m_statisticsEnabled)
			Operation.INCOMING_REMOVE.leave();
	}
	
	private void incomingCreateRequest(final CreateRequest p_request) {
//		if (m_statisticsEnabled)
//			Operation.INCOMING_CREATE.enter();
		
		int[] sizes = p_request.getSizes();
		long[] chunkIDs = new long[sizes.length];
		
		m_memoryManager.lockManage();
		for (int i = 0; i < sizes.length; i++) {
			chunkIDs[i] = m_memoryManager.create(sizes[i]);
		}
		m_memoryManager.unlockManage();
		
		CreateResponse response = new CreateResponse(p_request, chunkIDs);
		ErrorCode error = m_network.sendMessage(response);
		if (error != ErrorCode.SUCCESS)
		{
			m_logger.error(getClass(), "Sending chunk create respond to request " + p_request + " failed: " + error);
		}
		
//		if (m_statisticsEnabled)
//			Operation.INCOMING_CREATE.leave();
	}
}

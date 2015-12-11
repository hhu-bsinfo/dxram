package de.uniduesseldorf.dxram.core.chunk;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.chunk.ChunkStatistic.Operation;
import de.uniduesseldorf.dxram.core.dxram.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.engine.DXRAMService;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodeRole;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.events.IncomingChunkListener;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener.ConnectionLostEvent;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler.ExceptionSource;
import de.uniduesseldorf.dxram.core.lock.DefaultLock;
import de.uniduesseldorf.dxram.core.log.LogMessages.RemoveMessage;
import de.uniduesseldorf.dxram.core.lookup.Locations;
import de.uniduesseldorf.dxram.core.lookup.LookupComponent;
import de.uniduesseldorf.dxram.core.lookup.LookupException;
import de.uniduesseldorf.dxram.core.mem.Chunk;
import de.uniduesseldorf.dxram.core.mem.DataStructure;
import de.uniduesseldorf.dxram.core.mem.MigrationsTree;
import de.uniduesseldorf.dxram.core.mem.storage.MemoryManagerComponent;
import de.uniduesseldorf.dxram.core.net.NetworkComponent;
import de.uniduesseldorf.dxram.core.util.ChunkID;

import de.uniduesseldorf.menet.AbstractMessage;
import de.uniduesseldorf.menet.MessageDirectory;
import de.uniduesseldorf.menet.NetworkException;
import de.uniduesseldorf.menet.NetworkInterface;
import de.uniduesseldorf.menet.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.utils.Contract;
import de.uniduesseldorf.utils.Pair;
import de.uniduesseldorf.utils.StatisticsManager;
import de.uniduesseldorf.utils.config.Configuration;
import de.uniduesseldorf.utils.unsafe.IntegerLongList;
import de.uniduesseldorf.utils.unsafe.AbstractKeyValueList.KeyValuePair;

public class ChunkService extends DXRAMService implements MessageReceiver, ConnectionLostListener
{
	private static final String SERVICE_NAME = "Chunk";
	
	// Constants
	private final Logger LOGGER = Logger.getLogger(ChunkService.class);
	
	private MemoryManagerComponent m_memoryManager = null;
	private NetworkComponent m_network = null;
	private LookupComponent m_lookup = null;
	
	private boolean m_statisticsEnabled = false;
	private boolean m_logActive = false;
	private long m_secondaryLogSize = 0;
	private int m_replicationFactor = 0;
	
	public ChunkService()
	{
		super(SERVICE_NAME);
		
		m_nodeID = NodeID.INVALID_ID;

		m_memoryManager = null;

		if (LOG_ACTIVE && NodeID.getRole().equals(Role.PEER)) {
			m_ownBackupRanges = new ArrayList<BackupRange>();
			m_migrationBackupRanges = new ArrayList<BackupRange>();
			m_migrationsTree = new MigrationsTree((short) 10);
			m_currentBackupRange = null;
			m_currentMigrationBackupRange = new BackupRange(-1, null);
			m_rangeSize = 0;
		}
		m_firstRangeInitialized = false;

		m_network = null;
		m_lookup = null;
		m_log = null;
		m_lock = null;

		m_listener = null;

		m_migrationLock = null;
		m_mappingLock = null;
	}
	
	@Override
	public void setListener(final IncomingChunkListener p_listener) {
		m_listener = p_listener;
	}
	
	@Override
	protected boolean startService(final Configuration p_configuration) 
	{
		final boolean p_statisticsEnabled, final boolean p_logActive, final long p_secondaryLogSize, final int p_replicationFactor
		
		m_nodeID = NodeID.getLocalNodeID();

		m_network = CoreComponentFactory.getNetworkInterface();
		m_network.register(GetRequest.class, this);
		m_network.register(PutRequest.class, this);
		m_network.register(RemoveRequest.class, this);
		m_network.register(LockRequest.class, this);
		m_network.register(UnlockMessage.class, this);
		m_network.register(DataRequest.class, this);
		m_network.register(DataMessage.class, this);
		m_network.register(MultiGetRequest.class, this);
		m_network.register(ChunkCommandMessage.class, this);
		m_network.register(ChunkCommandRequest.class, this);

		if (LOG_ACTIVE && NodeID.getRole().equals(Role.PEER)) {
			m_log = CoreComponentFactory.getLogInterface();
		}

		m_lookup = CoreComponentFactory.getLookupInterface();
		if (NodeID.getRole().equals(Role.PEER)) {

			m_lock = CoreComponentFactory.getLockInterface();

			m_memoryManager = new MemoryManagerComponent(NodeID.getLocalNodeID());
			m_memoryManager.initialize(Core.getConfiguration().getLongValue(DXRAMConfigurationConstants.RAM_SIZE),
					Core.getConfiguration().getLongValue(DXRAMConfigurationConstants.RAM_SEGMENT_SIZE),
					Core.getConfiguration().getBooleanValue(DXRAMConfigurationConstants.STATISTIC_MEMORY));

			m_migrationLock = new ReentrantLock(false);
			registerPeer();
			m_mappingLock = new ReentrantLock(false);
		}

		if (Core.getConfiguration().getBooleanValue(DXRAMConfigurationConstants.STATISTIC_CHUNK)) {
			StatisticsManager.registerStatistic("Chunk", ChunkStatistic.getInstance());
		}
		
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean shutdownService() 
	{
		try {
			if (NodeID.getRole().equals(Role.PEER)) {
				m_memoryManager.disengage();
			}
		} catch (final MemoryException e) {}
		return false;
	}
	
	public long create(final int p_size) 
	{
		long chunkID = -1;

		if (m_statisticsEnabled)
			Operation.CREATE.enter();

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			m_memoryManager.lockManage();
			chunkID = m_memoryManager.create(p_size);
			if (chunkID != ChunkID.INVALID_ID) {
				initBackupRange(ChunkID.getLocalID(chunkID), p_size);
				m_memoryManager.unlockManage();
			} else {
				m_memoryManager.unlockManage();
			}
		}

		if (m_statisticsEnabled)
			Operation.CREATE.leave();

		return chunkID;
	}

	public long[] create(final int[] p_sizes) {
		long[] chunkIDs = null;

		if (m_statisticsEnabled) {
			Operation.MULTI_CREATE.enter();
		}
			
		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			chunkIDs = new long[p_sizes.length];

			m_memoryManager.lockManage();
			// keep first loop tight and execute everything
			// that we don't have to lock outside of this section
			for (int i = 0; i < p_sizes.length; i++) {
				chunkIDs[i] = m_memoryManager.create(p_sizes[i]);		
			}
			m_memoryManager.unlockManage();
			
			for (int i = 0; i < p_sizes.length; i++) {
				initBackupRange(ChunkID.getLocalID(chunkIDs[i]), p_sizes[i]);
			}
		}

		if (m_statisticsEnabled) {
			Operation.MULTI_CREATE.leave();
		}
		
		return chunkIDs;
	}

	public int get(final DataStructure p_dataStructure) {
		int ret = 0;
		short primaryPeer;
		GetRequest request;
		GetResponse response;
		Locations locations;

		if (m_statisticsEnabled)
			Operation.GET.enter();

		ChunkID.check(p_dataStructure.getID());

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			m_memoryManager.lockAccess();
			if (m_memoryManager.isResponsible(p_dataStructure.getID())) {
				// local get
				if (m_memoryManager.get(p_dataStructure))
					ret = 1;
				m_memoryManager.unlockAccess();
			} else {
				m_memoryManager.unlockAccess();

				locations = m_lookup.get(p_dataStructure.getID());
				if (locations == null) {
					break;
				}

				primaryPeer = locations.getPrimaryPeer();
				
				// local get of migrated chunk
				if (primaryPeer == getSystemData().getNodeID()) {
					// Local get
					int size = -1;
					int bytesRead = -1;

					m_memoryManager.lockAccess();
					if (m_memoryManager.get(p_dataStructure))
						ret = 1;
					m_memoryManager.unlockAccess();
				} else {
					// busy loop, sync get 
					while (true) {
						// Remote get
						request = new GetRequest(primaryPeer, p_dataStructure);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_dataStructure.getID());
							if (m_logActive) {
								// TODO: Start Recovery
							}
							continue;
						}
						response = request.getResponse(GetResponse.class);
						if (response != null) {
							ret = response.getChunk();
							break;
						}
					}
				}
			}
		}

		if (m_statisticsEnabled)
			Operation.GET.leave();

		return ret;
	}

	public int get(DataStructure[] p_dataStructures) {
		Map<Short, Vector<DataStructure>> remoteChunksByPeers;
		Vector<DataStructure> remoteChunksOfPeer;
		MultiGetRequest request;
		MultiGetResponse response;
		Vector<DataStructure> localChunks;
		int totalChunksGot;

		if (m_statisticsEnabled)
			Operation.MULTI_GET.enter();

		Contract.checkNotNull(p_chunkIDs, "no IDs given");
		ChunkID.check(p_chunkIDs);

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			// sort by local and remote data first
			remoteChunksByPeers = new TreeMap<>();
			localChunks = new Vector<DataStructure>();
			for (int i = 0; i < p_dataStructures.length; i++) {
				if (m_memoryManager.isResponsible(p_dataStructures[i].getID())) {
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

						remoteChunksOfPeer = remoteChunksByPeers.get(peer);
						if (remoteChunksOfPeer == null) {
							remoteChunksOfPeer = new Vector<DataStructure>();
							remoteChunksByPeers.put(peer, remoteChunksOfPeer);
						}
						remoteChunksOfPeer.add(p_dataStructures[i]);
					}
				}
			}

			// get local chunkIDs in a tight loop
			m_memoryManager.lockAccess();
			for (final DataStructure chunk : localChunks) {
				if (m_memoryManager.get(chunk))
					totalChunksGot++;
			}
			m_memoryManager.unlockAccess();

			// go for remote ones by each peer
			for (final Entry<Short, Vector<DataStructure>> peerWithChunks : remoteChunksByPeers.entrySet()) {
				short peer = peerWithChunks.getKey();
				Vector<DataStructure> remoteChunks = peerWithChunks.getValue();

				if (peer == getSystemData().getNodeID()) {
					// local get
					m_memoryManager.lockAccess();
					for (final DataStructure chunk : remoteChunks) {
						if (m_memoryManager.get(chunk))
							totalChunksGot++;
					}
					m_memoryManager.unlockAccess();
				} else {
					// gather chunk ids for message
					long[] ids = new long[remoteChunks.size()];
					for (int i = 0; i < ids.length; i++) {
						ids[i] = remoteChunks.get(i).getID();
					}

					// Remote get
					request = new MultiGetRequest(peer, ids);
					try {
						request.sendSync(m_network);
					} catch (final NetworkException e) {
						m_lookup.invalidate(ids);
						if (m_logActive) {
							// TODO: Start Recovery
						}
						continue;
					}
					response = request.getResponse(MultiGetResponse.class);
					if (response != null) {
						chunks = response.getChunks();
						for (int i = 0; i < list.size(); i++) {
							ret[list.get(i).getKey()] = chunks[i];
						}
					}
				}
			}

			// check if some chunks are null
			list = new IntegerLongList();
			for (int i = 0; i < ret.length; i++) {
				if (ret[i] == null) {
					list.add(i, p_chunkIDs[i]);
				}
			}
			if (!list.isEmpty()) {
				ids = new long[list.size()];
				for (int i = 0; i < list.size(); i++) {
					ids[i] = list.get(i).getValue();
				}

				chunks = get(ids);

				for (int i = 0; i < list.size(); i++) {
					ret[list.get(i).getKey()] = chunks[i];
				}
			}
		}

		Operation.MULTI_GET.leave();

		return ret;
	}

	public int put(DataStructure p_dataStrucutre) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int put(DataStructure[] p_dataStructure) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int remove(DataStructure p_dataStructure) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int remove(DataStructure[] p_dataStructures) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	

	@Override
	public void triggerEvent(ConnectionLostEvent p_event) {
		// TODO move to lock service?
		Contract.checkNotNull(p_event, "no event given");

		try {
			m_lock.unlockAll(p_event.getSource());
		} catch (final DXRAMException e) {}
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		LOGGER.trace("Entering incomingMessage with: p_message=" + p_message);

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
				case ChunkMessages.SUBTYPE_LOCK_REQUEST:
					incomingLockRequest((LockRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_UNLOCK_MESSAGE:
					incomingUnlockMessage((UnlockMessage) p_message);
					break;
				case ChunkMessages.SUBTYPE_DATA_REQUEST:
					incomingDataRequest((DataRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_DATA_MESSAGE:
					incomingDataMessage((DataMessage) p_message);
					break;
				case ChunkMessages.SUBTYPE_MULTIGET_REQUEST:
					incomingMultiGetRequest((MultiGetRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_CHUNK_COMMAND_MESSAGE:
					incomingCommandMessage((ChunkCommandMessage) p_message);
					break;
				case ChunkMessages.SUBTYPE_CHUNK_COMMAND_REQUEST:
					incomingCommandRequest((ChunkCommandRequest) p_message);
					break;
				default:
					break;
				}
			}
		}

		LOGGER.trace("Exiting incomingMessage");
	}
	
	// -----------------------------------------------------------------------------------
	
	private void registerNetworkMessages()
	{
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_GET_REQUEST, ChunkMessages.GetRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_GET_RESPONSE, ChunkMessages.GetResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_PUT_REQUEST, ChunkMessages.PutRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_PUT_RESPONSE, ChunkMessages.PutResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_REMOVE_REQUEST, ChunkMessages.RemoveRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_REMOVE_RESPONSE, ChunkMessages.RemoveResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_LOCK_REQUEST, ChunkMessages.LockRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_LOCK_RESPONSE, ChunkMessages.LockResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_UNLOCK_MESSAGE, ChunkMessages.UnlockMessage.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_DATA_REQUEST, ChunkMessages.DataRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_DATA_RESPONSE, ChunkMessages.DataResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_DATA_MESSAGE, ChunkMessages.DataMessage.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_MULTIGET_REQUEST, ChunkMessages.MultiGetRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_MULTIGET_RESPONSE, ChunkMessages.MultiGetResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_CHUNK_COMMAND_MESSAGE, ChunkMessages.ChunkCommandMessage.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_CHUNK_COMMAND_REQUEST, ChunkMessages.ChunkCommandRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_CHUNK_COMMAND_RESPONSE, ChunkMessages.ChunkCommandResponse.class);

		final byte logType = LogMessages.TYPE;
		final byte lookupType = LookupMessages.TYPE;
		final byte recoveryType = RecoveryMessages.TYPE;
		// TODO move
		// Log Messages
		m_network.registerMessageType(logType, LogMessages.SUBTYPE_LOG_MESSAGE, LogMessages.LogMessage.class);
		m_network.registerMessageType(logType, LogMessages.SUBTYPE_REMOVE_MESSAGE, LogMessages.RemoveMessage.class);
		m_network.registerMessageType(logType, LogMessages.SUBTYPE_INIT_REQUEST, LogMessages.InitRequest.class);
		m_network.registerMessageType(logType, LogMessages.SUBTYPE_INIT_RESPONSE, LogMessages.InitResponse.class);
		m_network.registerMessageType(logType, LogMessages.SUBTYPE_LOG_COMMAND_REQUEST, LogMessages.LogCommandRequest.class);
		m_network.registerMessageType(logType, LogMessages.SUBTYPE_LOG_COMMAND_RESPONSE, LogMessages.LogCommandResponse.class);

		// Lookup Messages
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_JOIN_REQUEST, LookupMessages.JoinRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_JOIN_RESPONSE, LookupMessages.JoinResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST, LookupMessages.InitRangeRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_INIT_RANGE_RESPONSE, LookupMessages.InitRangeResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_LOOKUP_REQUEST, LookupMessages.LookupRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_LOOKUP_RESPONSE, LookupMessages.LookupResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_BACKUP_RANGES_REQUEST, LookupMessages.GetBackupRangesRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_BACKUP_RANGES_RESPONSE, LookupMessages.GetBackupRangesResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_UPDATE_ALL_MESSAGE, LookupMessages.UpdateAllMessage.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_MIGRATE_REQUEST, LookupMessages.MigrateRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_MIGRATE_RESPONSE, LookupMessages.MigrateResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_MIGRATE_MESSAGE, LookupMessages.MigrateMessage.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST, LookupMessages.MigrateRangeRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_MIGRATE_RANGE_RESPONSE, LookupMessages.MigrateRangeResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_REMOVE_REQUEST, LookupMessages.RemoveRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_REMOVE_RESPONSE, LookupMessages.RemoveResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE, LookupMessages.SendBackupsMessage.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE, LookupMessages.SendSuperpeersMessage.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST, LookupMessages.AskAboutBackupsRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE, LookupMessages.AskAboutBackupsResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST, LookupMessages.AskAboutSuccessorRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE, LookupMessages.AskAboutSuccessorResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE,
				LookupMessages.NotifyAboutNewPredecessorMessage.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE, LookupMessages.NotifyAboutNewSuccessorMessage.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE, LookupMessages.PingSuperpeerMessage.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_REQUEST, LookupMessages.SearchForPeerRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_RESPONSE, LookupMessages.SearchForPeerResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_PROMOTE_PEER_REQUEST, LookupMessages.PromotePeerRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_PROMOTE_PEER_RESPONSE, LookupMessages.PromotePeerResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_DELEGATE_PROMOTE_PEER_MESSAGE, LookupMessages.DelegatePromotePeerMessage.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE, LookupMessages.NotifyAboutFailedPeerMessage.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_START_RECOVERY_MESSAGE, LookupMessages.StartRecoveryMessage.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_INSERT_ID_REQUEST, LookupMessages.InsertIDRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_INSERT_ID_RESPONSE, LookupMessages.InsertIDResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_CHUNKID_REQUEST, LookupMessages.GetChunkIDRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_CHUNKID_RESPONSE, LookupMessages.GetChunkIDResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_MAPPING_COUNT_REQUEST, LookupMessages.GetMappingCountRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_GET_MAPPING_COUNT_RESPONSE, LookupMessages.GetMappingCountResponse.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_REQUEST, LookupMessages.LookupReflectionRequest.class);
		m_network.registerMessageType(lookupType, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_RESPONSE, LookupMessages.LookupReflectionResponse.class);

		// Recovery Messages
		m_network.registerMessageType(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_MESSAGE, RecoveryMessages.RecoverMessage.class);
		m_network.registerMessageType(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST, RecoveryMessages.RecoverBackupRangeRequest.class);
		m_network.registerMessageType(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE, RecoveryMessages.RecoverBackupRangeResponse.class);
	
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

		try {
			boolean res = false;
			Chunk chunk = new Chunk();
			
			m_memoryManager.lockAccess();
			res = m_memoryManager.get(chunk);
			m_memoryManager.unlockAccess();
			
			

			new GetResponse(p_request, chunk).send(m_network);
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle request", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}

		if (m_statisticsEnabled)
			Operation.INCOMING_GET.leave();
	}

	/**
	 * Handles an incoming MultiGetRequest
	 * @param p_request
	 *            the MultiGetRequest
	 */
	private void incomingMultiGetRequest(final MultiGetRequest p_request) {
		long[] chunkIDs;
		Chunk[] chunks;

		Operation.INCOMING_MULTI_GET.enter();

		try {
			chunkIDs = p_request.getChunkIDs();
			chunks = new Chunk[chunkIDs.length];
			for (int i = 0; i < chunkIDs.length; i++) {
				// TODO
				// chunks[i] = m_memoryManager.get(chunkIDs[i]);
			}

			new MultiGetResponse(p_request, chunks).send(m_network);
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle request", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}

		Operation.INCOMING_MULTI_GET.leave();
	}

	/**
	 * Handles an incoming PutRequest
	 * @param p_request
	 *            the PutRequest
	 */
	private void incomingPutRequest(final PutRequest p_request) {
		boolean success = false;
		Chunk chunk;

		Operation.INCOMING_PUT.enter();

		chunk = p_request.getChunk();

		try {
			m_migrationLock.lock();
			if (m_memoryManager.isResponsible(chunk.getChunkID())) {
				// TODO
				// m_memoryManager.put(chunk);
				success = true;
			}
			m_migrationLock.unlock();

			new PutResponse(p_request, success).send(m_network);
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle message", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}

		Operation.INCOMING_PUT.leave();
	}

	/**
	 * Handles an incoming RemoveRequest
	 * @param p_request
	 *            the RemoveRequest
	 */
	private void incomingRemoveRequest(final RemoveRequest p_request) {
		boolean success = false;
		boolean replicate = false;
		byte rangeID = -1;
		long chunkID;
		short[] backupPeers = null;
		RemoveRequest request;

		Operation.INCOMING_REMOVE.enter();

		chunkID = p_request.getChunkID();

		try {

			m_migrationLock.lock();
			if (m_memoryManager.isResponsible(chunkID)) {
				if (!m_memoryManager.wasMigrated(chunkID)) {
					// Local remove
					m_memoryManager.lockManage();
					m_memoryManager.remove(chunkID);
					m_memoryManager.unlockManage();
				} else {
					rangeID = m_migrationsTree.getBackupRange(chunkID);
					backupPeers = getBackupPeersForLocalChunks(chunkID);

					// Local remove
					m_memoryManager.lockManage();
					m_memoryManager.remove(chunkID);
					m_memoryManager.unlockManage();

					m_migrationsTree.removeObject(chunkID);

					// Inform creator about removal
					request = new RemoveRequest(ChunkID.getCreatorID(chunkID), chunkID);
					try {
						request.sendSync(m_network);
						request.getResponse(RemoveResponse.class);
					} catch (final NetworkException e) {
						System.out.println("Cannot inform creator about removal! Is not available!");
					}
				}

				replicate = true;
				success = true;
			} else if (ChunkID.getCreatorID(chunkID) == m_nodeID) {
				m_memoryManager.prepareChunkIDForReuse(chunkID);
				success = true;
			}
			m_migrationLock.unlock();

			new RemoveResponse(p_request, success).send(m_network);

			if (LOG_ACTIVE && replicate) {
				// Send backups for logging (unreliable)
				if (backupPeers != null) {
					for (int i = 0; i < backupPeers.length; i++) {
						if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
							new RemoveMessage(backupPeers[i], new long[] {chunkID}, rangeID).send(m_network);
						}
					}
				}
			}
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle message", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}

		Operation.INCOMING_REMOVE.leave();
	}

	/**
	 * Handles an incoming LockRequest
	 * @param p_request
	 *            the LockRequest
	 */
	private void incomingLockRequest(final LockRequest p_request) {
		DefaultLock lock;

		Operation.INCOMING_LOCK.enter();

		try {
			lock = new DefaultLock(p_request.getChunkID(), p_request.getSource(), p_request.isReadLock());
			m_lock.lock(lock);

			// TODO
			// object without variable to store it?
			// new LockResponse(p_request, m_memoryManager.get(lock.getChunk())).send(m_network);
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle message", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}

		Operation.INCOMING_LOCK.leave();
	}

	/**
	 * Handles an incoming UnlockMessage
	 * @param p_message
	 *            the UnlockMessage
	 */
	private void incomingUnlockMessage(final UnlockMessage p_message) {
		Operation.INCOMING_UNLOCK.enter();

		try {
			m_lock.unlock(p_message.getChunkID(), p_message.getSource());
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle message", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_message);
		}

		Operation.INCOMING_UNLOCK.leave();
	}

	/**
	 * Handles an incoming DataRequest
	 * @param p_request
	 *            the DataRequest
	 */
	private void incomingDataRequest(final DataRequest p_request) {
		try {
			putForeignChunks(p_request.getChunks());

			new DataResponse(p_request).send(m_network);
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle request", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}
	}

	/**
	 * Handles an incoming DataMessage
	 * @param p_message
	 *            the DataMessage
	 */
	private void incomingDataMessage(final DataMessage p_message) {
		try {
			putForeignChunks(p_message.getChunks());
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle message", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_message);
		}
	}
}

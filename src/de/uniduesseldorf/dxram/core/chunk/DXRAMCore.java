package de.uniduesseldorf.dxram.core.chunk;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.api.DXRAMCoreInterface;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.GetRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.GetResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkStatistic.Operation;
import de.uniduesseldorf.dxram.core.chunk.storage.MemoryManager;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;
import de.uniduesseldorf.dxram.core.lookup.LookupInterface;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.NodeID;

import de.uniduesseldorf.menet.AbstractMessage;
import de.uniduesseldorf.menet.ChunkMessages;
import de.uniduesseldorf.menet.LogMessages;
import de.uniduesseldorf.menet.LookupMessages;
import de.uniduesseldorf.menet.MessageDirectory;
import de.uniduesseldorf.menet.NetworkException;
import de.uniduesseldorf.menet.NetworkInterface;
import de.uniduesseldorf.menet.RecoveryMessages;
import de.uniduesseldorf.menet.NetworkInterface.MessageReceiver;

public class DXRAMCore implements DXRAMCoreInterface, MessageReceiver, ConnectionLostListener
{
	// Constants
	private final Logger LOGGER = Logger.getLogger(DXRAMCore.class);
	
	// configuration values
	private final boolean M_STATISTICS_ENABLED;
	private final boolean M_LOG_ACTIVE;
	private final long M_SECONDARY_LOG_SIZE;
	private final int M_REPLICATION_FACTOR;
	
	private MemoryManager m_memoryManager;
	private NetworkInterface m_network;
	private LookupInterface m_lookup;
	
	public DXRAMCore(final boolean p_statisticsEnabled, final boolean p_logActive, final long p_secondaryLogSize, final int p_replicationFactor)
	{
		M_STATISTICS_ENABLED = p_statisticsEnabled;
		M_LOG_ACTIVE = p_logActive;
		M_SECONDARY_LOG_SIZE = p_secondaryLogSize;
		M_REPLICATION_FACTOR = p_replicationFactor;
	}
	
	@Override
	public boolean initialize() {
		// TODO Auto-generated method stub
		
		registerNetworkMessages();
		return false;
	}

	@Override
	public boolean shutdown() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public long create(final int p_size) 
	{
		long chunkID = -1;

		if (M_STATISTICS_ENABLED)
			Operation.CREATE.enter();

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
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

		if (M_STATISTICS_ENABLED)
			Operation.CREATE.leave();

		return chunkID;
	}

	@Override
	public long[] create(final int[] p_sizes) {
		long[] chunkIDs = null;

		if (M_STATISTICS_ENABLED) {
			Operation.MULTI_CREATE.enter();
		}
			
		if (NodeID.getRole().equals(Role.SUPERPEER)) {
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

		if (M_STATISTICS_ENABLED) {
			Operation.MULTI_CREATE.leave();
		}
		
		return chunkIDs;
	}

	@Override
	public int get(final DataStructure p_dataStructure) {
		int ret = 0;
		short primaryPeer;
		GetRequest request;
		GetResponse response;
		Locations locations;

		if (M_STATISTICS_ENABLED)
			Operation.GET.enter();

		ChunkID.check(p_dataStructure.getID());

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
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
				if (primaryPeer == NodeID.getLocalNodeID()) {
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
						request = new GetRequest(primaryPeer, p_dataStructure.getID());
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_dataStructure.getID());
							if (M_LOG_ACTIVE) {
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

		if (M_STATISTICS_ENABLED)
			Operation.GET.leave();

		return ret;
	}

	@Override
	public int get(DataStructure[] p_dataStructures) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int put(DataStructure p_dataStrucutre) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int put(DataStructure[] p_dataStructure) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int remove(DataStructure p_dataStructure) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int remove(DataStructure[] p_dataStructures) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	

	@Override
	public void triggerEvent(ConnectionLostEvent p_event) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onIncomingMessage(AbstractMessage p_message) {
		// TODO Auto-generated method stub
		
	}
	
	// -----------------------------------------------------------------------------------
	
	/**
	 * Initializes the backup range for current locations
	 * and determines new backup peers if necessary
	 * @param p_localID
	 *            the current LocalID
	 * @param p_size
	 *            the size of the new created chunk
	 * @param p_version
	 *            the version of the new created chunk
	 * @throws LookupException
	 *             if range could not be initialized
	 */
	private void initBackupRange(final long p_localID, final int p_size) throws LookupException {
		if (M_LOG_ACTIVE) {
			m_rangeSize += p_size + m_log.getHeaderSize(m_nodeID, p_localID, p_size, p_version);
			if (p_localID == 1 && p_version == 0) {
				// First Chunk has LocalID 1, but there is a Chunk with LocalID 0 for hosting the name service
				determineBackupPeers(0);
				m_lookup.initRange((long) m_nodeID << 48, new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_log.initBackupRange((long) m_nodeID << 48, m_currentBackupRange.getBackupPeers());
				m_rangeSize = 0;
			} else if (m_rangeSize > SECONDARY_LOG_SIZE / 2) {
				determineBackupPeers(p_localID);
				m_lookup.initRange(((long) m_nodeID << 48) + p_localID, new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_log.initBackupRange(((long) m_nodeID << 48) + p_localID, m_currentBackupRange.getBackupPeers());
				m_rangeSize = 0;
			}
		} else if (p_localID == 1 && p_version == 0) {
			m_lookup.initRange(((long) m_nodeID << 48) + 0xFFFFFFFFFFFFL, new Locations(m_nodeID, new short[] {-1, -1, -1}, null));
		}
	}
	
	private void registerNetworkMessages()
	{
		final byte chunkType = ChunkMessages.TYPE;
		final byte logType = LogMessages.TYPE;
		final byte lookupType = LookupMessages.TYPE;
		final byte recoveryType = RecoveryMessages.TYPE;
		
		// Chunk Messages
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_GET_REQUEST, ChunkMessages.GetRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_GET_RESPONSE, ChunkMessages.GetResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_PUT_REQUEST, ChunkMessages.PutRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_PUT_RESPONSE, ChunkMessages.PutResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_REMOVE_REQUEST, ChunkMessages.RemoveRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_REMOVE_RESPONSE, ChunkMessages.RemoveResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_LOCK_REQUEST, ChunkMessages.LockRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_LOCK_RESPONSE, ChunkMessages.LockResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_UNLOCK_MESSAGE, ChunkMessages.UnlockMessage.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_DATA_REQUEST, ChunkMessages.DataRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_DATA_RESPONSE, ChunkMessages.DataResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_DATA_MESSAGE, ChunkMessages.DataMessage.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_MULTIGET_REQUEST, ChunkMessages.MultiGetRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_MULTIGET_RESPONSE, ChunkMessages.MultiGetResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_CHUNK_COMMAND_MESSAGE, ChunkMessages.ChunkCommandMessage.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_CHUNK_COMMAND_REQUEST, ChunkMessages.ChunkCommandRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_CHUNK_COMMAND_RESPONSE, ChunkMessages.ChunkCommandResponse.class);

		// Log Messages
		MessageDirectory.register(logType, LogMessages.SUBTYPE_LOG_MESSAGE, LogMessages.LogMessage.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_REMOVE_MESSAGE, LogMessages.RemoveMessage.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_INIT_REQUEST, LogMessages.InitRequest.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_INIT_RESPONSE, LogMessages.InitResponse.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_LOG_COMMAND_REQUEST, LogMessages.LogCommandRequest.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_LOG_COMMAND_RESPONSE, LogMessages.LogCommandResponse.class);

		// Lookup Messages
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_JOIN_REQUEST, LookupMessages.JoinRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_JOIN_RESPONSE, LookupMessages.JoinResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST, LookupMessages.InitRangeRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_INIT_RANGE_RESPONSE, LookupMessages.InitRangeResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_LOOKUP_REQUEST, LookupMessages.LookupRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_LOOKUP_RESPONSE, LookupMessages.LookupResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_BACKUP_RANGES_REQUEST, LookupMessages.GetBackupRangesRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_BACKUP_RANGES_RESPONSE, LookupMessages.GetBackupRangesResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_UPDATE_ALL_MESSAGE, LookupMessages.UpdateAllMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_MIGRATE_REQUEST, LookupMessages.MigrateRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_MIGRATE_RESPONSE, LookupMessages.MigrateResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_MIGRATE_MESSAGE, LookupMessages.MigrateMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST, LookupMessages.MigrateRangeRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_MIGRATE_RANGE_RESPONSE, LookupMessages.MigrateRangeResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_REMOVE_REQUEST, LookupMessages.RemoveRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_REMOVE_RESPONSE, LookupMessages.RemoveResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE, LookupMessages.SendBackupsMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE, LookupMessages.SendSuperpeersMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST, LookupMessages.AskAboutBackupsRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE, LookupMessages.AskAboutBackupsResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST, LookupMessages.AskAboutSuccessorRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE, LookupMessages.AskAboutSuccessorResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE,
				LookupMessages.NotifyAboutNewPredecessorMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE, LookupMessages.NotifyAboutNewSuccessorMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE, LookupMessages.PingSuperpeerMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_REQUEST, LookupMessages.SearchForPeerRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_RESPONSE, LookupMessages.SearchForPeerResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_PROMOTE_PEER_REQUEST, LookupMessages.PromotePeerRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_PROMOTE_PEER_RESPONSE, LookupMessages.PromotePeerResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_DELEGATE_PROMOTE_PEER_MESSAGE, LookupMessages.DelegatePromotePeerMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE, LookupMessages.NotifyAboutFailedPeerMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_START_RECOVERY_MESSAGE, LookupMessages.StartRecoveryMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_INSERT_ID_REQUEST, LookupMessages.InsertIDRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_INSERT_ID_RESPONSE, LookupMessages.InsertIDResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_CHUNKID_REQUEST, LookupMessages.GetChunkIDRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_CHUNKID_RESPONSE, LookupMessages.GetChunkIDResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_MAPPING_COUNT_REQUEST, LookupMessages.GetMappingCountRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_MAPPING_COUNT_RESPONSE, LookupMessages.GetMappingCountResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_REQUEST, LookupMessages.LookupReflectionRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_RESPONSE, LookupMessages.LookupReflectionResponse.class);

		// Recovery Messages
		MessageDirectory.register(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_MESSAGE, RecoveryMessages.RecoverMessage.class);
		MessageDirectory.register(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST, RecoveryMessages.RecoverBackupRangeRequest.class);
		MessageDirectory.register(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE, RecoveryMessages.RecoverBackupRangeResponse.class);
	
	}
}

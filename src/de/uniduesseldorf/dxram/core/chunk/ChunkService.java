package de.uniduesseldorf.dxram.core.chunk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.backup.BackupComponent;
import de.uniduesseldorf.dxram.core.chunk.ChunkStatistic.Operation;
import de.uniduesseldorf.dxram.core.chunk.messages.ChunkMessages;
import de.uniduesseldorf.dxram.core.chunk.messages.GetRequest;
import de.uniduesseldorf.dxram.core.chunk.messages.GetResponse;
import de.uniduesseldorf.dxram.core.chunk.messages.PutRequest;
import de.uniduesseldorf.dxram.core.chunk.messages.PutResponse;
import de.uniduesseldorf.dxram.core.chunk.messages.RemoveRequest;
import de.uniduesseldorf.dxram.core.chunk.messages.RemoveResponse;
import de.uniduesseldorf.dxram.core.data.Chunk;
import de.uniduesseldorf.dxram.core.data.DataStructure;
import de.uniduesseldorf.dxram.core.dxram.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.engine.DXRAMService;
import de.uniduesseldorf.dxram.core.engine.config.DXRAMConfigurationConstants;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodeRole;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.events.IncomingChunkListener;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener.ConnectionLostEvent;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler.ExceptionSource;
import de.uniduesseldorf.dxram.core.lock.DefaultLock;
import de.uniduesseldorf.dxram.core.lock.LockComponent;
import de.uniduesseldorf.dxram.core.log.LogMessages.LogMessage;
import de.uniduesseldorf.dxram.core.log.LogMessages.RemoveMessage;
import de.uniduesseldorf.dxram.core.lookup.Locations;
import de.uniduesseldorf.dxram.core.lookup.LookupComponent;
import de.uniduesseldorf.dxram.core.lookup.LookupException;
import de.uniduesseldorf.dxram.core.mem.MemoryManagerComponent;
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
	private LockComponent m_lock = null;
	private BackupComponent m_backup = null;
	
	private boolean m_statisticsEnabled = false;
	private boolean m_logActive = false;
	private long m_secondaryLogSize = 0;
	private int m_replicationFactor = 0;
	
	private IncomingChunkListener m_listener;
	
	public ChunkService() {
		super(SERVICE_NAME);
	}
	
	public void setListener(final IncomingChunkListener p_listener) {
		m_listener = p_listener;
	}
	
	@Override
	protected boolean startService(final Configuration p_configuration) 
	{
		m_memoryManager = getComponent(MemoryManagerComponent.COMPONENT_IDENTIFIER);
		m_network = getComponent(NetworkComponent.COMPONENT_IDENTIFIER);
		m_lookup = getComponent(LookupComponent.COMPONENT_IDENTIFIER);
		m_lock = getComponent(LockComponent.COMPONENT_IDENTIFIER);
		m_backup = getComponent(BackupComponent.COMPONENT_IDENTIFIER);
		
		m_statisticsEnabled = p_configuration.getBooleanValue(DXRAMConfigurationConstants.STATISTIC_CHUNK);
		m_logActive = p_configuration.getBooleanValue(DXRAMConfigurationConstants.LOG_ACTIVE);
		m_secondaryLogSize = p_configuration.getLongValue(DXRAMConfigurationConstants.SECONDARY_LOG_SIZE);
		m_replicationFactor = p_configuration.getIntValue(DXRAMConfigurationConstants.REPLICATION_FACTOR);

		registerNetworkMessages();

		if (getSystemData().getNodeRole().equals(NodeRole.PEER)) {
			m_backup.registerPeer();
		}

		if (m_statisticsEnabled) {
			StatisticsManager.registerStatistic("Chunk", ChunkStatistic.getInstance());
		}
		
		return true;
	}

	@Override
	protected boolean shutdownService() 
	{
		// TODO
		
		return true;
	}

	public long[] create(final int... p_sizes) {
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
				m_backup.initBackupRange(ChunkID.getLocalID(chunkIDs[i]), p_sizes[i]);
			}
			
			// TODO inform logging about creation?
		}

		if (m_statisticsEnabled) {
			Operation.MULTI_CREATE.leave();
		}
		
		return chunkIDs;
	}

	public int get(final DataStructure... p_dataStructures) {
		return get(false, p_dataStructures);
	}
	
	public int get(final boolean p_aquireLock, DataStructure... p_dataStructures) {
		Map<Short, Vector<DataStructure>> remoteChunksByPeers;
		Vector<DataStructure> remoteChunksOfPeer;
		GetRequest request;
		GetResponse response;
		Vector<DataStructure> localChunks;
		int totalChunksGot = 0;
		
		if (m_statisticsEnabled)
			Operation.MULTI_GET.enter();

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			// sort by local and remote data first
			remoteChunksByPeers = new TreeMap<>();
			localChunks = new Vector<DataStructure>();
			m_memoryManager.lockAccess();
			for (int i = 0; i < p_dataStructures.length; i++) {
				if (m_memoryManager.exists(p_dataStructures[i].getID())) {
					// local
					localChunks.add(p_dataStructures[i]);
				} else {
					// remote, figure out location and sort by peers
					Locations locations;
					try {
						locations = m_lookup.get(p_dataStructures[i].getID());
					} catch (LookupException e) {
						LOGGER.error("Lookup for chunk " + p_dataStructures[i] + " failed: " + e.getMessage());
						locations = null;
					}
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

			// get local chunkIDs
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
					// local get, migrated
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
					request = new GetRequest(peer, p_aquireLock, (DataStructure[]) remoteChunks.toArray());
					try {
						request.sendSync(m_network);
					} catch (final NetworkException e) {
						m_lookup.invalidate(ids);
						if (m_logActive) {
							// TODO: Start Recovery
						}
						continue;
					}
					response = request.getResponse(GetResponse.class);
					if (response != null) {
						// TODO check which get requests failed?
						totalChunksGot += response.getNumberOfChunksGot();
					}
				}
			}
		}

		if (m_statisticsEnabled)
			Operation.MULTI_GET.leave();

		return totalChunksGot;
	}

	public int put(DataStructure... p_dataStrucutre) {
		return put(false, p_dataStrucutre);
	}
	
	public int put(final boolean p_releaseLock, DataStructure... p_dataStructure)
	{
		Locations locations;
		short primaryPeer;
		int numChunksPut = 0;
		PutRequest request;

		if (m_statisticsEnabled)
			Operation.PUT.enter();

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			HashMap<Long, ArrayList<DataStructure>> backupMap = new HashMap<Long, ArrayList<DataStructure>>();
			ArrayList<DataStructure> localChunks = new ArrayList<DataStructure>();
			Map<Short, Vector<DataStructure>> remoteChunksByPeers = new HashMap<Short, Vector<DataStructure>>();
			Vector<DataStructure> remoteChunksOfPeer;
			
			// first loop: sort by local/remote chunks and backup peers
			m_memoryManager.lockAccess();
			for (DataStructure dataStructure : p_dataStructure) {
				if (m_memoryManager.exists(dataStructure.getID())) {
					localChunks.add(dataStructure);

					if (m_logActive) {
						// Send backups for logging (unreliable)
						long backupPeersAsLong = m_backup.getBackupPeersForLocalChunksAsLong(dataStructure.getID());
						if (backupPeersAsLong != -1) {
							ArrayList<DataStructure> list = backupMap.get(backupPeersAsLong);
							if (list == null) {
								list = new ArrayList<DataStructure>();
								backupMap.put(backupPeersAsLong, list);
							}
							list.add(dataStructure);
						}
					}
				} else {
					// remote, figure out location and sort by peers
					try {
						locations = m_lookup.get(dataStructure.getID());
					} catch (LookupException e) {
						LOGGER.error("Lookup for chunk " + dataStructure + " failed: " + e.getMessage());
						locations = null;
					}
					if (locations == null) {
						continue;
					} else {
						short peer = locations.getPrimaryPeer();

						remoteChunksOfPeer = remoteChunksByPeers.get(peer);
						if (remoteChunksOfPeer == null) {
							remoteChunksOfPeer = new Vector<DataStructure>();
							remoteChunksByPeers.put(peer, remoteChunksOfPeer);
						}
						remoteChunksOfPeer.add(dataStructure);
					}
				}
			}
			
			// have local puts first
			for (DataStructure dataStructure : localChunks) {
				if (m_memoryManager.put(dataStructure))
					numChunksPut++;
				else
					LOGGER.error("Putting local chunk " + dataStructure + " failed.");
			}
			m_memoryManager.unlockAccess();
			
			// unlock chunks
			if (p_releaseLock) {
				for (DataStructure dataStructure : localChunks) {
					m_lock.unlock(dataStructure.getID(), getSystemData().getNodeID());
				}
			}
			
			// now handle the remote ones
			// TODO sort by locations for multi put messages
			for (DataStructure dataStructure : remoteChunksOfPeer) {
				locations = m_lookup.get(dataStructure.getID());
				primaryPeer = locations.getPrimaryPeer();
				long backupPeersAsLong = locations.getBackupPeersAsLong();

				if (primaryPeer == getSystemData().getNodeID()) {
					// Local put, migrated
					m_memoryManager.lockManage();
					if (m_memoryManager.put(dataStructure)) {
						numChunksPut++;
					} else {
						LOGGER.error("Putting local (migrated) chunk " + dataStructure + " failed.");
					}
					m_memoryManager.unlockManage();
					
					if (p_releaseLock) {
						m_lock.unlock(dataStructure.getID(), getSystemData().getNodeID());
					}
				} else {
					// Remote put
					request = new PutRequest(primaryPeer, p_releaseLock, dataStructure);
					try {
						request.sendSync(m_network);
					} catch (final NetworkException e) {
						m_lookup.invalidate(dataStructure.getID());
						continue;
					}
					// TODO stefan: have multi put? -> needs sorting by peer
					if (request.getResponse(PutResponse.class).getStatusCodes()[0] == 0) {
						numChunksPut++;
					} else {
						LOGGER.error("Putting remote chunk " + dataStructure + " failed.");
					}
				}
				
				if (m_logActive) {
					// Send backups for logging (unreliable)
					if (backupPeersAsLong != -1) {
						ArrayList<DataStructure>  list = backupMap.get(backupPeersAsLong);
						if (list == null) {
							list = new ArrayList<DataStructure>();
							backupMap.put(backupPeersAsLong, list);
						}
						list.add(dataStructure);
					}
				}
			}
			
			if (m_logActive) {
				short[] backupPeers;
				Chunk[] chunks;
				for (Map.Entry<Long, ArrayList<DataStructure>> entry : backupMap.entrySet()) {
					long backupPeersAsLong = entry.getKey();
					chunks = entry.getValue().toArray(new Chunk[entry.getValue().size()]);

					backupPeers = new short[] {(short) (backupPeersAsLong & 0x000000000000FFFFL),
							(short) ((backupPeersAsLong & 0x00000000FFFF0000L) >> 16), (short) ((backupPeersAsLong & 0x0000FFFF00000000L) >> 32)};
					for (int i = 0; i < backupPeers.length; i++) {
						if (backupPeers[i] != getSystemData().getNodeID() && backupPeers[i] != -1) {
							LOGGER.debug("Logging " + chunks.length + " Chunks to " + backupPeers[i]);
							try {
								new LogMessage(backupPeers[i], chunks).send(m_network);
							} catch (NetworkException e) {
								LOGGER.error("Sending log message for put failed.", e);
							}
						}
					}
				}
			}
		}

		if (m_statisticsEnabled)
			Operation.PUT.leave();		
		
		return numChunksPut;
	}

	// TODO stefan: re-check and re-do
	public int remove(DataStructure p_dataStructure) {
		int chunksRemoved = 0;

		if (m_statisticsEnabled)
			Operation.REMOVE.enter();

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			m_memoryManager.lockManage();
			if (m_memoryManager.exists(p_dataStructure.getID())) {
				if (!m_memoryManager.dataWasMigrated(p_dataStructure.getID())) {
					// Local remove
					m_memoryManager.remove(p_dataStructure.getID());
					m_memoryManager.unlockManage();

					if (m_logActive) {
						// Send backups for logging (unreliable)
						short[] backupPeers = m_backup.getBackupPeersForLocalChunks(p_dataStructure.getID());
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != getSystemData().getNodeID() && backupPeers[i] != -1) {
									new RemoveMessage(backupPeers[i], new long[] {p_dataStructure.getID()}).send(m_network);
								}
							}
						}
					}
					chunksRemoved++;
				} else {
					// Local remove
					m_memoryManager.remove(p_dataStructure.getID());
					m_memoryManager.unlockManage();

					byte rangeID = m_backup.getBackupRange(p_dataStructure.getID());
					m_backup.removeChunk(p_dataStructure.getID());

					// Inform creator about removal
					RemoveRequest request = new RemoveRequest(ChunkID.getCreatorID(p_dataStructure.getID()), p_dataStructure.getID());
					try {
						request.sendSync(m_network);
						request.getResponse(RemoveResponse.class);
					} catch (final NetworkException e) {
						LOGGER.error("Cannot inform creator about removal! Is not available!");
					}

					if (m_logActive) {
						// Send backups for logging (unreliable)
						short[] backupPeers = m_backup.getBackupPeersForLocalChunks(p_dataStructure.getID());
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != getSystemData().getNodeID() && backupPeers[i] != -1) {
									new RemoveMessage(backupPeers[i], new long[] {p_dataStructure.getID()}, rangeID).send(m_network);
								}
							}
						}
					}
					chunksRemoved++;
				}
			} else {
				m_memoryManager.unlockManage();
				
				// remote remove
				Locations locations = m_lookup.get(p_dataStructure.getID());
				if (locations != null) {
					// Remote remove
					RemoveRequest request = new RemoveRequest(locations.getPrimaryPeer(), p_dataStructure.getID());
					try {
						request.sendSync(m_network);
						if (request.getResponse(RemoveResponse.class).getStatus()) {
							chunksRemoved++;
						} else {
							LOGGER.error("Removing remote chunk " + p_dataStructure + " failed.");
						}
					} catch (final NetworkException e) {
						m_lookup.invalidate(p_dataStructure.getID());
					}
				}
			}
			
			m_lookup.remove(p_dataStructure.getID());
		}
		
		if (m_statisticsEnabled)
			Operation.REMOVE.leave();

		return chunksRemoved;
	}

	// TODO delete when remove done
	public int remove(DataStructure[] p_dataStructures) {
		boolean success = false;

		Operation.REMOVE.enter();

		if (p_chunkIDs != null && p_chunkIDs.length > 0) {
			if (NodeID.getRole().equals(Role.SUPERPEER)) {
				LOGGER.error("a superpeer must not use chunks");
			} else {
				success = true;
				for (long chunkID : p_chunkIDs) {
					success = success && deleteChunkData(chunkID);
				}
				m_lookup.remove(p_chunkIDs);
			}
		} else {
			success = true;
		}

		Operation.REMOVE.leave();

		if (!success) {
			throw new DXRAMException("chunks removal failed");
		}
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
				default:
					break;
				}
			}
		}

		LOGGER.trace("Exiting incomingMessage");
	}
	
	@Override
	public void triggerEvent(ConnectionLostEvent p_event) {
		// TODO Auto-generated method stub
		
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
				LOGGER.warn("Getting size of chunk " + chunkIDs[i] + " failed, does not exist.");
				size = 0;
			} else {
				numChunksGot++;
			}
			
			chunks[i] = new Chunk(chunkIDs[i], size);
			m_memoryManager.get(chunks[i]);
		}
		m_memoryManager.unlockAccess();

		try {
			new GetResponse(p_request, numChunksGot, chunks).send(m_network);
		} catch (NetworkException e) {
			LOGGER.error("Sending GetResponse for " + numChunksGot + " chunks failed.", e);
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

		Chunk[] chunks = p_request.getChunks();
		byte[] statusChunks = new byte[chunks.length];
		
		m_memoryManager.lockAccess();
		for (int i = 0; i < chunks.length; i++) {
			if (m_memoryManager.exists(chunks[i].getID())) {
				if (!m_memoryManager.put(chunks[i])) {
					// does not exist (anymore)
					statusChunks[i] = -1;
					LOGGER.warn("Putting chunk " + chunks[i] + " failed, does not exist.");
				}
			} else {
				// got migrated, not responsible anymore
				statusChunks[i] = -2;
				LOGGER.warn("Putting chunk " + chunks[i] + " failed, was migrated.");
			}
		}
		m_memoryManager.unlockAccess();
		
		try {
			new PutResponse(p_request, statusChunks).send(m_network);
		} catch (NetworkException e) {
			LOGGER.error("Sending put response failed.", e);
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
		
		// remove chunks first (local)
		m_memoryManager.lockManage();
		for (int i = 0; i < chunkIDs.length; i++) {
			if (m_memoryManager.exists(chunkIDs[i])) {
				m_memoryManager.remove(chunkIDs[i]);
				chunkStatusCodes[i] = 0;
			} else if (ChunkID.getCreatorID(chunkIDs[i]) == getSystemData().getNodeID()) {
				// chunk data was migrated, "migrate back" id
				m_memoryManager.prepareChunkIDForReuse(chunkIDs[i]);
				chunkStatusCodes[i] = 1;
			} else {
				// remove failed, does not exist
				chunkStatusCodes[i] = -1;
			}
		}
		m_memoryManager.unlockManage();

		for (int i = 0; i < chunkIDs.length; i++) {
			byte rangeID = m_backup.getBackupRange(chunkIDs[i]);
			short[] backupPeers = m_backup.getBackupPeersForLocalChunks(chunkIDs[i]);
			m_backup.removeChunk(chunkIDs[i]);
			
			if (m_memoryManager.dataWasMigrated(chunkIDs[i])) {	
				// Inform peer who got the migrated data about removal
				RemoveRequest request = new RemoveRequest(ChunkID.getCreatorID(chunkIDs[i]), new Chunk(chunkIDs[i], 0));
				try {
					request.sendSync(m_network);
					request.getResponse(RemoveResponse.class);
				} catch (final NetworkException e) {
					LOGGER.error("Informing creator about removal of chunk " + chunkIDs[i] + " failed.", e);
				}
			}
			
			if (m_logActive) {
				// Send backups for logging (unreliable)
				if (backupPeers != null) {
					for (int j = 0; j < backupPeers.length; j++) {
						if (backupPeers[j] != getSystemData().getNodeID() && backupPeers[j] != -1) {
							try {
								new RemoveMessage(backupPeers[i], new long[] {chunkIDs[i]}, rangeID).send(m_network);
							} catch (NetworkException e) {
								LOGGER.error("Sending logging remove message failed.", e);
							}
						}
					}
				}
			}
		}

		// finally, send back response with status codes
		try {
			new RemoveResponse(p_request, chunkStatusCodes).send(m_network);
		} catch (NetworkException e) {
			LOGGER.error("Sending remove response failed.", e);
		}

		if (m_statisticsEnabled)
			Operation.INCOMING_REMOVE.leave();
	}
}

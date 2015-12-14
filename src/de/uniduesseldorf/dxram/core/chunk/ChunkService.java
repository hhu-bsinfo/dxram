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

import de.uniduesseldorf.dxram.core.chunk.ChunkStatistic.Operation;
import de.uniduesseldorf.dxram.core.chunk.messages.ChunkMessages;
import de.uniduesseldorf.dxram.core.chunk.messages.GetRequest;
import de.uniduesseldorf.dxram.core.chunk.messages.GetResponse;
import de.uniduesseldorf.dxram.core.chunk.messages.MultiGetRequest;
import de.uniduesseldorf.dxram.core.chunk.messages.MultiGetResponse;
import de.uniduesseldorf.dxram.core.chunk.messages.PutRequest;
import de.uniduesseldorf.dxram.core.chunk.messages.PutResponse;
import de.uniduesseldorf.dxram.core.chunk.messages.RemoveRequest;
import de.uniduesseldorf.dxram.core.chunk.messages.RemoveResponse;
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
import de.uniduesseldorf.dxram.core.lock.LockComponent;
import de.uniduesseldorf.dxram.core.log.LogMessages.LogMessage;
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
	private LockComponent m_lock = null;
	
	// ChunkID -> migration backup range
	// TODO stefan: move this to migration component?
	private MigrationsTree m_migrationsTree;
	
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

				try {
					locations = m_lookup.get(p_dataStructure.getID());
				} catch (LookupException e1) {
					LOGGER.error("Lookup for chunk " + p_dataStructure + " failed: " + e1.getMessage());
					locations = null;
				}
				
				if (locations != null) {
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
								break;
							}
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
				if (m_memoryManager.isResponsible(p_dataStructures[i].getID())) {
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
					request = new MultiGetRequest(peer, (DataStructure[]) remoteChunks.toArray());
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
						totalChunksGot += remoteChunks.size();
					}
				}
			}
		}

		if (m_statisticsEnabled)
			Operation.MULTI_GET.leave();

		return totalChunksGot;
	}

	public int put(DataStructure p_dataStrucutre) {
		return put(p_dataStrucutre, false);
	}
	
	public int put(final DataStructure p_dataStructure, final boolean p_releaseLock) {
		Locations locations;
		short primaryPeer;
		short[] backupPeers;
		boolean success = false;
		PutRequest request;

		if (m_statisticsEnabled)
			Operation.PUT.enter();

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (m_memoryManager.isResponsible(p_dataStructure.getID())) {
				// Local put
				int bytesWritten;

				m_memoryManager.lockManage();
				success = m_memoryManager.put(p_dataStructure);
				m_memoryManager.unlockManage();

				if (p_releaseLock) {
					m_lock.unlock(p_dataStructure.getID(), getSystemData().getNodeID());
				}

				if (m_logActive) {
					// Send backups for logging (unreliable)
					backupPeers = getBackupPeersForLocalChunks(p_dataStructure.getID());
					if (backupPeers != null) {
						for (int i = 0; i < backupPeers.length; i++) {
							if (backupPeers[i] != getSystemData().getNodeID() && backupPeers[i] != -1) {
								m_memoryManager.lockAccess();
								new LogMessage(backupPeers[i], new Chunk[] {p_chunk}).send(m_network);
								m_memoryManager.unlockAccess();
							}
						}
					}
				}
			} else {
				while (!success) {
					locations = m_lookup.get(p_dataStructure.getID());
					primaryPeer = locations.getPrimaryPeer();
					backupPeers = locations.getBackupPeers();

					if (primaryPeer == getSystemData().getNodeID()) {
						// Local put
						int bytesWritten;

						m_memoryManager.lockManage();
						success = m_memoryManager.put(p_dataStructure);
						m_memoryManager.unlockManage();
						if (p_releaseLock) {
							m_lock.unlock(p_dataStructure.getID(), getSystemData().getNodeID());
						}
					} else {
						// Remote put
						request = new PutRequest(primaryPeer, p_dataStructure, p_releaseLock);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_dataStructure.getID());
							continue;
						}
						success = request.getResponse(PutResponse.class).getStatus();
					}
					if (success && m_logActive) {
						// Send backups for logging (unreliable)
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != getSystemData().getNodeID() && backupPeers[i] != -1) {
									m_memoryManager.lockAccess();
									new LogMessage(backupPeers[i], new Chunk[] {p_chunk}).send(m_network);
									m_memoryManager.unlockAccess();
								}
							}
						}
					}
				}
			}
		}

		if (m_statisticsEnabled)
			Operation.PUT.leave();		
	}	

	public int put(DataStructure[] p_dataStructure) {
		Locations locations;
		short primaryPeer;
		int numChunksPut;
		PutRequest request;

		if (m_statisticsEnabled)
			Operation.PUT.enter();

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			HashMap<Long, ArrayList<DataStructure>> backupMap = new HashMap<Long, ArrayList<DataStructure>>();
			ArrayList<DataStructure> localChunks = new ArrayList<DataStructure>();
			ArrayList<DataStructure> remoteChunks = new ArrayList<DataStructure>();
			
			// first loop: sort by local/remote chunks and backup peers
			for (DataStructure dataStructure : p_dataStructure) {
				m_memoryManager.lockAccess();
				if (m_memoryManager.isResponsible(dataStructure.getID())) {
					localChunks.add(dataStructure);

					if (m_logActive) {
						// Send backups for logging (unreliable)
						long backupPeersAsLong = getBackupPeersForLocalChunksAsLong(dataStructure.getID());
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
					remoteChunks.add(dataStructure);
				}
			}
			
			// have local puts first
			m_memoryManager.lockManage();
			for (DataStructure dataStructure : localChunks) {
				if (m_memoryManager.put(dataStructure))
					numChunksPut++;
				else
					LOGGER.error("Putting local chunk " + dataStructure + " failed.");
			}
			m_memoryManager.unlockManage();
			
			// now handle the remote ones
			for (DataStructure dataStructure : remoteChunks) {
				locations = m_lookup.get(dataStructure.getID());
				primaryPeer = locations.getPrimaryPeer();
				long backupPeersAsLong = locations.getBackupPeersAsLong();

				if (primaryPeer == getSystemData().getNodeID()) {
					// Local put
					m_memoryManager.lockManage();
					if (m_memoryManager.put(dataStructure)) {
						numChunksPut++;
					} else {
						LOGGER.error("Putting local (migrated) chunk " + dataStructure + " failed.");
					}
					m_memoryManager.unlockManage();
				} else {
					// Remote put
					request = new PutRequest(primaryPeer, dataStructure, false);
					try {
						request.sendSync(m_network);
					} catch (final NetworkException e) {
						m_lookup.invalidate(dataStructure.getID());
						continue;
					}
					// TODO stefan: have multi put? -> needs sorting by peer
					if (request.getResponse(PutResponse.class).getStatus()) {
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
							System.out.println("Logging " + chunks.length + " Chunks to " + backupPeers[i]);
							new LogMessage(backupPeers[i], chunks).send(m_network);
						}
					}
				}
			}
		}

		if (m_statisticsEnabled)
			Operation.PUT.leave();
	}

	public int remove(DataStructure p_dataStructure) {
		int chunksRemoved = 0;

		if (m_statisticsEnabled)
			Operation.REMOVE.enter();

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			m_memoryManager.lockManage();
			if (m_memoryManager.isResponsible(p_dataStructure.getID())) {
				if (!m_memoryManager.wasMigrated(p_dataStructure.getID())) {
					// Local remove
					m_memoryManager.remove(p_dataStructure.getID());
					m_memoryManager.unlockManage();

					if (m_logActive) {
						// Send backups for logging (unreliable)
						short[] backupPeers = getBackupPeersForLocalChunks(p_dataStructure.getID());
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

					byte rangeID = m_migrationsTree.getBackupRange(p_dataStructure.getID());
					m_migrationsTree.removeObject(p_dataStructure.getID());

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
						short[] backupPeers = getBackupPeersForLocalChunks(p_dataStructure.getID());
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
				case ChunkMessages.SUBTYPE_MULTIGET_REQUEST:
					incomingMultiGetRequest((MultiGetRequest) p_message);
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
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_GET_REQUEST, GetRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_GET_RESPONSE, GetResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_PUT_REQUEST, PutRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_PUT_RESPONSE, PutResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_REMOVE_REQUEST, RemoveRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_REMOVE_RESPONSE, RemoveResponse.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_MULTIGET_REQUEST, MultiGetRequest.class);
		m_network.registerMessageType(ChunkMessages.TYPE, ChunkMessages.SUBTYPE_MULTIGET_RESPONSE, MultiGetResponse.class);
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

		boolean getSuccess = false;
		Chunk chunk = new Chunk(p_request.getChunkID());
		
		m_memoryManager.lockAccess();
		getSuccess = m_memoryManager.get(chunk);
		m_memoryManager.unlockAccess();
		
		if (getSuccess) {
			try {
				new GetResponse(p_request, chunk).send(m_network);
			} catch (NetworkException e) {
				LOGGER.error("Sending GetResponse for chunk " + chunk + " failed.", e);
			}
		} else {
			// TODO stefan chunk does not exist (concurrently deleted?)
			// send error and log warning?
			LOGGER.error("Serving incoming get request failed, chunk does not exist.");
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
		int numChunksGot = 0;

		if (m_statisticsEnabled)
			Operation.INCOMING_MULTI_GET.enter();

		chunkIDs = p_request.getChunkIDs();
		chunks = new Chunk[chunkIDs.length];
		for (int i = 0; i < chunkIDs.length; i++) {
			chunks[i] = new Chunk(chunkIDs[i]);
		}
		
		// separate loop to keep locked area small
		m_memoryManager.lockAccess();
		for (Chunk chunk : chunks) {
			// TODO stefan: this needs proper handling if getting a single chunk failed
			// further flag required for sending response
			if (m_memoryManager.get(chunk)) {
				numChunksGot++;
			} else {
				LOGGER.error("Serving incoming multi get request, getting chunk " + chunk + " failed, does not exist.");
			}
		}
		m_memoryManager.unlockAccess();

		try {
			new MultiGetResponse(p_request, chunks).send(m_network);
		} catch (NetworkException e) {
			LOGGER.error("Sending multi get response failed.", e);
		}
	
		if (m_statisticsEnabled)
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

		if (m_statisticsEnabled)
			Operation.INCOMING_PUT.enter();

		chunk = p_request.getChunk();

		// TODO stefan: migration lock?
		//m_migrationLock.lock();
		m_memoryManager.lockAccess();
		if (m_memoryManager.isResponsible(chunk.getID())) {
			success = m_memoryManager.put(chunk);
		} else {
			// TODO stefan: what if not responsible (any more?)
		}
		m_memoryManager.unlockAccess();
		// TODO stefan: migration lock?
		//m_migrationLock.unlock();

		if (!success) {
			// TODO stefan: send put response failed message instead
			LOGGER.error("Serving put request, putting chunk " + chunk + " failed, does not exist.");
		}
		
		try {
			new PutResponse(p_request, success).send(m_network);
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

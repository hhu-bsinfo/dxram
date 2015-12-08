
package de.uniduesseldorf.dxram.core.chunk;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.commands.CmdUtils;
import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.ChunkCommandMessage;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.ChunkCommandRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.ChunkCommandResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.DataMessage;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.DataRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.DataResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.GetRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.GetResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.LockRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.LockResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.MultiGetRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.MultiGetResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.PutRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.PutResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.RemoveRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.RemoveResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.UnlockMessage;
import de.uniduesseldorf.dxram.core.chunk.ChunkStatistic.Operation;
import de.uniduesseldorf.dxram.core.chunk.storage.MemoryManager;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.events.IncomingChunkListener;
import de.uniduesseldorf.dxram.core.events.IncomingChunkListener.IncomingChunkEvent;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler.ExceptionSource;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.core.exceptions.NetworkException;
import de.uniduesseldorf.dxram.core.lock.DefaultLock;
import de.uniduesseldorf.dxram.core.lock.LockInterface;
import de.uniduesseldorf.dxram.core.log.LogInterface;
import de.uniduesseldorf.dxram.core.log.LogMessages.LogMessage;
import de.uniduesseldorf.dxram.core.log.LogMessages.RemoveMessage;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;
import de.uniduesseldorf.dxram.core.lookup.LookupInterface;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.AbstractRequest;
import de.uniduesseldorf.dxram.core.net.NetworkInterface;
import de.uniduesseldorf.dxram.core.net.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.NodeID;
import de.uniduesseldorf.dxram.utils.AbstractAction;
import de.uniduesseldorf.dxram.utils.Contract;
import de.uniduesseldorf.dxram.utils.Pair;
import de.uniduesseldorf.dxram.utils.StatisticsManager;
import de.uniduesseldorf.dxram.utils.ZooKeeperHandler;
import de.uniduesseldorf.dxram.utils.ZooKeeperHandler.ZooKeeperException;
import de.uniduesseldorf.dxram.utils.unsafe.AbstractKeyValueList.KeyValuePair;
import de.uniduesseldorf.dxram.utils.unsafe.IntegerLongList;

/**
 * Leads data accesses to the local Chunks or a remote node
 * @author Florian Klein 09.03.2012
 */
public final class ChunkHandler implements ChunkInterface, MessageReceiver, ConnectionLostListener {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(ChunkHandler.class);

	private static final int INDEX_SIZE = 12016;

	private static final boolean LOG_ACTIVE = Core.getConfiguration().getBooleanValue(ConfigurationConstants.LOG_ACTIVE);
	private static final long SECONDARY_LOG_SIZE = Core.getConfiguration().getLongValue(ConfigurationConstants.SECONDARY_LOG_SIZE);
	private static final int REPLICATION_FACTOR = Core.getConfiguration().getIntValue(ConfigurationConstants.REPLICATION_FACTOR);

	// Attributes
	private short m_nodeID;
	private long m_rangeSize;

	private MemoryManager m_memoryManager;

	private BackupRange m_currentBackupRange;
	private ArrayList<BackupRange> m_ownBackupRanges;

	private BackupRange m_currentMigrationBackupRange;
	private ArrayList<BackupRange> m_migrationBackupRanges;
	// ChunkID -> migration backup range
	private MigrationsTree m_migrationsTree;

	private NetworkInterface m_network;
	private LookupInterface m_lookup;
	private LogInterface m_log;
	private LockInterface m_lock;

	private IncomingChunkListener m_listener;

	private Lock m_migrationLock;
	private Lock m_initLock;
	private Lock m_mappingLock;

	// Constructors
	/**
	 * Creates an instance of DataHandler
	 */
	public ChunkHandler() {
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

		m_network = null;
		m_lookup = null;
		m_log = null;
		m_lock = null;

		m_listener = null;

		m_migrationLock = null;
		m_initLock = null;
		m_mappingLock = null;
	}

	// Setters
	@Override
	public void setListener(final IncomingChunkListener p_listener) {
		m_listener = p_listener;
	}

	// Methods
	@Override
	public void initialize() throws DXRAMException {
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

			m_memoryManager = new MemoryManager(NodeID.getLocalNodeID());
			m_memoryManager.initialize(Core.getConfiguration().getLongValue(ConfigurationConstants.RAM_SIZE),
					Core.getConfiguration().getLongValue(ConfigurationConstants.RAM_SEGMENT_SIZE),
					Core.getConfiguration().getBooleanValue(ConfigurationConstants.STATISTIC_MEMORY));

			m_migrationLock = new ReentrantLock(false);
			registerPeer();
			m_initLock = new ReentrantLock(false);
			m_mappingLock = new ReentrantLock(false);
		}

		if (Core.getConfiguration().getBooleanValue(ConfigurationConstants.STATISTIC_CHUNK)) {
			StatisticsManager.registerStatistic("Chunk", ChunkStatistic.getInstance());
		}
	}

	@Override
	public void close() {
		try {
			if (NodeID.getRole().equals(Role.PEER)) {
				m_memoryManager.disengage();
			}
		} catch (final MemoryException e) {}
	}

	@Override
	public Chunk create(final int p_size) throws DXRAMException {
		Chunk ret = null;
		long chunkID;

		// TODO get configuration value on init if we should enable this
		// and have if blocks surrounding them
		Operation.CREATE.enter();

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			m_memoryManager.lockManage();
			chunkID = m_memoryManager.create(p_size);
			if (chunkID != -1) {
				int version = -1;

				version = m_memoryManager.getVersion(chunkID);
				initBackupRange(ChunkID.getLocalID(chunkID), p_size, version);
				m_memoryManager.unlockManage();

				ret = new Chunk(chunkID, p_size);
			} else {
				m_memoryManager.unlockManage();
			}
		}

		Operation.CREATE.leave();

		return ret;
	}

	@Override
	public Chunk[] create(final int[] p_sizes) throws DXRAMException {
		Chunk[] ret = null;
		long[] chunkIDs = null;
		int[] versions = null;
		int itemCount = 0;

		Operation.MULTI_CREATE.enter();

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			chunkIDs = new long[p_sizes.length];
			versions = new int[p_sizes.length];

			m_memoryManager.lockManage();

			// keep first loop tight and execute everything
			// that we don't have to lock outside of this section
			for (int i = 0; i < p_sizes.length; i++)
			{
				chunkIDs[i] = m_memoryManager.create(p_sizes[i]);
				if (chunkIDs[i] != -1)
				{
					versions[i] = m_memoryManager.getVersion(chunkIDs[i]);
				}
				else
				{
					break;
				}
				initBackupRange(ChunkID.getLocalID(chunkIDs[i]), p_sizes[i], versions[i]);

				itemCount++;
			}

			m_memoryManager.unlockManage();

			// "post processing"
			ret = new Chunk[itemCount];
			for (int i = 0; i < ret.length; i++)
			{
				ret[i] = new Chunk(chunkIDs[i], p_sizes[i]);
			}
		}

		Operation.MULTI_CREATE.leave();

		return ret;
	}

	@Override
	public Chunk create(final int p_size, final int p_id) throws DXRAMException {
		Chunk ret = null;
		Chunk mapping;

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			final long chunkID = (long) m_nodeID << 48;
			int size;

			ret = create(p_size);

			m_lookup.insertID(p_id, ret.getChunkID());
			m_mappingLock.lock();

			mapping = null;

			m_memoryManager.lockAccess();
			if (m_memoryManager.exists(chunkID))
			{
				size = m_memoryManager.getSize(chunkID);
				mapping = new Chunk(chunkID, size);
				m_memoryManager.get(chunkID, mapping.getData().array(), 0, size);
			}
			m_memoryManager.unlockAccess();

			if (null == mapping) {
				// Create chunk to store mappings
				mapping = new Chunk((long) m_nodeID << 48, INDEX_SIZE);
				mapping.getData().putInt(4);
				// TODO: Check metadata management regarding chunk zero
			}
			insertMapping(p_id, ret.getChunkID(), mapping);
			m_mappingLock.unlock();
		}

		return ret;
	}

	@Override
	public Chunk[] create(final int[] p_sizes, final int p_id) throws DXRAMException {
		Chunk[] ret = null;
		Chunk mapping;

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			final long chunkID = (long) m_nodeID << 48;
			int size;

			ret = create(p_sizes);

			m_lookup.insertID(p_id, ret[0].getChunkID());

			m_mappingLock.lock();

			mapping = null;

			m_memoryManager.lockAccess();
			if (m_memoryManager.exists(chunkID))
			{
				size = m_memoryManager.getSize(chunkID);
				mapping = new Chunk(chunkID, size);
				m_memoryManager.get(chunkID, mapping.getData().array(), 0, size);
			}
			m_memoryManager.unlockAccess();

			if (null == mapping) {
				// Create chunk to store mappings
				mapping = new Chunk(chunkID, INDEX_SIZE);
				mapping.getData().putInt(4);
				// TODO: Check metadata management regarding chunk zero
			}
			insertMapping(p_id, ret[0].getChunkID(), mapping);
			m_mappingLock.unlock();
		}

		return ret;
	}

	@Override
	public Chunk get(final long p_chunkID) throws DXRAMException {
		Chunk ret = null;
		short primaryPeer;
		GetRequest request;
		GetResponse response;
		Locations locations;

		Operation.GET.enter();

		ChunkID.check(p_chunkID);

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			m_memoryManager.lockAccess();
			if (m_memoryManager.isResponsible(p_chunkID)) {
				// Local get
				int size = -1;
				int bytesRead = -1;

				size = m_memoryManager.getSize(p_chunkID);
				ret = new Chunk(p_chunkID, size);
				bytesRead = m_memoryManager.get(p_chunkID, ret.getData().array(), 0, size);
				m_memoryManager.unlockAccess();
			} else {
				m_memoryManager.unlockAccess();

				while (null == ret) {
					locations = m_lookup.get(p_chunkID);
					if (locations == null) {
						break;
					}

					primaryPeer = locations.getPrimaryPeer();

					if (primaryPeer == m_nodeID) {
						// Local get
						int size = -1;
						int bytesRead = -1;

						m_memoryManager.lockAccess();
						size = m_memoryManager.getSize(p_chunkID);
						ret = new Chunk(p_chunkID, size);
						bytesRead = m_memoryManager.get(p_chunkID, ret.getData().array(), 0, size);
						m_memoryManager.unlockAccess();
					} else {
						// Remote get
						request = new GetRequest(primaryPeer, p_chunkID);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_chunkID);
							if (LOG_ACTIVE) {
								// TODO: Start Recovery
							}
							continue;
						}
						response = request.getResponse(GetResponse.class);
						if (response != null) {
							ret = response.getChunk();
						}
					}
				}
			}
		}

		Operation.GET.leave();

		return ret;
	}

	@Override
	public Chunk[] get(final long[] p_chunkIDs) throws DXRAMException {
		Chunk[] ret = null;
		Map<Short, IntegerLongList> peers;
		short peer;
		IntegerLongList list;
		MultiGetRequest request;
		MultiGetResponse response;
		long[] ids;
		Chunk[] chunks;
		Vector<Pair<Integer, Long>> localChunkIDs;

		Operation.MULTI_GET.enter();

		Contract.checkNotNull(p_chunkIDs, "no IDs given");
		ChunkID.check(p_chunkIDs);

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			ret = new Chunk[p_chunkIDs.length];

			peers = new TreeMap<>();
			localChunkIDs = new Vector<Pair<Integer, Long>>();
			for (int i = 0; i < p_chunkIDs.length; i++) {
				if (m_memoryManager.isResponsible(p_chunkIDs[i])) {
					// Local get
					localChunkIDs.add(new Pair<Integer, Long>(i, p_chunkIDs[i]));
				} else {
					Locations locations;
					locations = m_lookup.get(p_chunkIDs[i]);
					if (locations == null) {
						continue;
					} else {
						peer = locations.getPrimaryPeer();

						list = peers.get(peer);
						if (list == null) {
							list = new IntegerLongList();
							peers.put(peer, list);
						}
						list.add(i, p_chunkIDs[i]);
					}
				}
			}

			// get local chunkIDs in a tight loop
			Iterator<Pair<Integer, Long>> it = localChunkIDs.iterator();
			m_memoryManager.lockAccess();
			while (it.hasNext())
			{
				final Pair<Integer, Long> item = it.next();
				int size;

				size = m_memoryManager.getSize(item.second());
				ret[item.first()] = new Chunk(item.second(), size);
				m_memoryManager.get(item.second(), ret[item.first()].getData().array(), 0, size);
			}
			m_memoryManager.unlockAccess();

			for (final Entry<Short, IntegerLongList> entry : peers.entrySet()) {
				peer = entry.getKey();
				list = entry.getValue();

				if (peer == m_nodeID) {
					// Local get
					int size;
					int bytesRead;

					m_memoryManager.lockAccess();
					for (final KeyValuePair<Integer, Long> pair : list) {
						final long chunkID = pair.getValue();
						size = m_memoryManager.getSize(chunkID);
						ret[pair.getKey()] = new Chunk(chunkID, size);
						bytesRead = m_memoryManager.get(chunkID, ret[pair.getKey()].getData().array(), 0, size);
					}
					m_memoryManager.unlockAccess();
				} else {
					ids = new long[list.size()];
					for (int i = 0; i < list.size(); i++) {
						ids[i] = list.get(i).getValue();
					}

					// Remote get
					request = new MultiGetRequest(peer, ids);
					try {
						request.sendSync(m_network);
					} catch (final NetworkException e) {
						m_lookup.invalidate(ids);
						if (LOG_ACTIVE) {
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

	@Override
	public Chunk get(final int p_id) throws DXRAMException {
		return get(getChunkID(p_id));
	}

	@Override
	public long getChunkID(final int p_id) throws DXRAMException {
		return m_lookup.getChunkID(p_id);
	}

	@Override
	public void getAsync(final long p_chunkID) throws DXRAMException {
		short primaryPeer;
		GetRequest request;

		Operation.GET_ASYNC.enter();

		ChunkID.check(p_chunkID);

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (m_memoryManager.isResponsible(p_chunkID)) {
				// Local get
				int size;
				int bytesRead;
				Chunk chunk;

				m_memoryManager.lockAccess();
				size = m_memoryManager.getSize(p_chunkID);
				chunk = new Chunk(p_chunkID, size);
				bytesRead = m_memoryManager.get(p_chunkID, chunk.getData().array(), 0, size);
				m_memoryManager.unlockAccess();

				fireIncomingChunk(new IncomingChunkEvent(m_nodeID, chunk));
			} else {
				primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();

				if (primaryPeer != m_nodeID) {
					// Remote get
					request = new GetRequest(primaryPeer, p_chunkID);
					request.registerFulfillAction(new GetRequestAction());
					request.send(m_network);
				}
			}
		}

		Operation.GET_ASYNC.enter();
	}

	@Override
	public void put(final Chunk p_chunk) throws DXRAMException {
		put(p_chunk, false);
	}

	@Override
	public void put(final Chunk p_chunk, final boolean p_releaseLock) throws DXRAMException {
		Locations locations;
		short primaryPeer;
		short[] backupPeers;
		boolean success = false;
		PutRequest request;

		Operation.PUT.enter();

		Contract.checkNotNull(p_chunk, "no chunk given");

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (m_memoryManager.isResponsible(p_chunk.getChunkID())) {
				// Local put
				int bytesWritten;

				m_memoryManager.lockManage();
				bytesWritten = m_memoryManager.put(p_chunk.getChunkID(), p_chunk.getData().array(), 0, p_chunk.getData().array().length);
				m_memoryManager.unlockManage();

				if (p_releaseLock) {
					m_lock.unlock(p_chunk.getChunkID(), m_nodeID);
				}

				if (LOG_ACTIVE) {
					// Send backups for logging (unreliable)
					backupPeers = getBackupPeersForLocalChunks(p_chunk.getChunkID());
					if (backupPeers != null) {
						for (int i = 0; i < backupPeers.length; i++) {
							if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
								m_memoryManager.lockAccess();
								new LogMessage(backupPeers[i], new Chunk[] {p_chunk},
										new int[] {m_memoryManager.getVersion(p_chunk.getChunkID())}).send(m_network);
								m_memoryManager.unlockAccess();
							}
						}
					}
				}
			} else {
				while (!success) {
					locations = m_lookup.get(p_chunk.getChunkID());
					primaryPeer = locations.getPrimaryPeer();
					backupPeers = locations.getBackupPeers();

					if (primaryPeer == m_nodeID) {
						// Local put
						int bytesWritten;

						m_memoryManager.lockManage();
						bytesWritten = m_memoryManager.put(p_chunk.getChunkID(), p_chunk.getData().array(), 0, p_chunk.getData().array().length);
						m_memoryManager.unlockManage();
						if (p_releaseLock) {
							m_lock.unlock(p_chunk.getChunkID(), m_nodeID);
						}
						success = true;
					} else {
						// Remote put
						request = new PutRequest(primaryPeer, p_chunk, p_releaseLock);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_chunk.getChunkID());
							continue;
						}
						success = request.getResponse(PutResponse.class).getStatus();
					}
					if (success && LOG_ACTIVE) {
						// Send backups for logging (unreliable)
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
									m_memoryManager.lockAccess();
									new LogMessage(backupPeers[i], new Chunk[] {p_chunk},
											new int[] {m_memoryManager.getVersion(p_chunk.getChunkID())}).send(m_network);
									m_memoryManager.unlockAccess();
								}
							}
						}
					}
				}
			}
		}

		Operation.PUT.leave();
	}

	/*-@Override
	public void put(final Chunk[] p_chunks) throws DXRAMException {
		Locations locations;
		short primaryPeer;
		short[] backupPeers;
		boolean success = false;
		PutRequest request;

		Operation.PUT.enter();

		Contract.checkNotNull(p_chunks, "no chunks given");

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			for (Chunk chunk : p_chunks) {
				if (m_memoryManager.isResponsible(chunk.getChunkID())) {
					// Local put
					// TODO optimize: have this in a separate loop
					int bytesWritten;

					m_memoryManager.lockManage();
					bytesWritten = m_memoryManager.put(chunk.getChunkID(), chunk.getData().array(), 0, chunk.getData().array().length);
					m_memoryManager.unlockManage();

					if (LOG_ACTIVE) {
						// Send backups for logging (unreliable)
						backupPeers = getBackupPeersForLocalChunks(chunk.getChunkID());
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
									new LogMessage(backupPeers[i], new Chunk[] {chunk},
											new int[] {m_memoryManager.getVersion(chunk.getChunkID())}).send(m_network);
								}
							}
						}
					}
				} else {
					while (!success) {
						locations = m_lookup.get(chunk.getChunkID());
						primaryPeer = locations.getPrimaryPeer();
						backupPeers = locations.getBackupPeers();

						if (primaryPeer == m_nodeID) {
							// Local put
							int bytesWritten;
							// TODO optimize: have this in a separate loop
							m_memoryManager.lockManage();
							bytesWritten = m_memoryManager.put(chunk.getChunkID(), chunk.getData().array(), 0, chunk.getData().array().length);
							m_memoryManager.unlockManage();
							success = true;
						} else {
							// Remote put
							request = new PutRequest(primaryPeer, chunk, false);
							try {
								request.sendSync(m_network);
							} catch (final NetworkException e) {
								m_lookup.invalidate(chunk.getChunkID());
								continue;
							}
							success = request.getResponse(PutResponse.class).getStatus();
						}
						if (success && LOG_ACTIVE) {
							// Send backups for logging (unreliable)
							if (backupPeers != null) {
								for (int i = 0; i < backupPeers.length; i++) {
									if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
										new LogMessage(backupPeers[i], new Chunk[] {chunk},
												new int[] {m_memoryManager.getVersion(chunk.getChunkID())}).send(m_network);
									}
								}
							}
						}
					}
				}
			}
		}

		Operation.PUT.leave();
	}*/

	@Override
	public void put(final Chunk[] p_chunks) throws DXRAMException {
		Locations locations;
		short primaryPeer;
		long backupPeersAsLong;
		boolean success = false;
		PutRequest request;

		HashMap<Long, ArrayList<Chunk>> map = null;
		ArrayList<Chunk> list;

		Operation.PUT.enter();

		Contract.checkNotNull(p_chunks, "no chunks given");

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			map = new HashMap<Long, ArrayList<Chunk>>();
			for (Chunk chunk : p_chunks) {
				if (m_memoryManager.isResponsible(chunk.getChunkID())) {
					// Local put
					// TODO optimize: have this in a separate loop
					int bytesWritten;

					m_memoryManager.lockManage();
					bytesWritten = m_memoryManager.put(chunk.getChunkID(), chunk.getData().array(), 0, chunk.getData().array().length);
					m_memoryManager.unlockManage();

					if (LOG_ACTIVE) {
						// Send backups for logging (unreliable)
						backupPeersAsLong = getBackupPeersForLocalChunksAsLong(chunk.getChunkID());
						if (backupPeersAsLong != -1) {
							list = map.get(backupPeersAsLong);
							if (list == null) {
								list = new ArrayList<Chunk>();
								map.put(backupPeersAsLong, list);
							}
							list.add(chunk);
						}
					}
				} else {
					while (!success) {
						locations = m_lookup.get(chunk.getChunkID());
						primaryPeer = locations.getPrimaryPeer();
						backupPeersAsLong = locations.getBackupPeersAsLong();

						if (primaryPeer == m_nodeID) {
							// Local put
							int bytesWritten;
							// TODO optimize: have this in a separate loop
							m_memoryManager.lockManage();
							bytesWritten = m_memoryManager.put(chunk.getChunkID(), chunk.getData().array(), 0, chunk.getData().array().length);
							m_memoryManager.unlockManage();
							success = true;
						} else {
							// Remote put
							request = new PutRequest(primaryPeer, chunk, false);
							try {
								request.sendSync(m_network);
							} catch (final NetworkException e) {
								m_lookup.invalidate(chunk.getChunkID());
								continue;
							}
							success = request.getResponse(PutResponse.class).getStatus();
						}
						if (success && LOG_ACTIVE) {
							// Send backups for logging (unreliable)
							if (backupPeersAsLong != -1) {
								list = map.get(backupPeersAsLong);
								if (list == null) {
									list = new ArrayList<Chunk>();
									map.put(backupPeersAsLong, list);
								}
								list.add(chunk);
							}
						}
					}
				}
			}
		}

		if (LOG_ACTIVE) {
			short[] backupPeers;
			Chunk[] chunks;
			int[] versions;
			for (Map.Entry<Long, ArrayList<Chunk>> entry : map.entrySet()) {
				backupPeersAsLong = entry.getKey();
				chunks = entry.getValue().toArray(new Chunk[entry.getValue().size()]);

				m_memoryManager.lockAccess();
				versions = new int[chunks.length];
				for (int i = 0; i < chunks.length; i++) {
					versions[i] = m_memoryManager.getVersion(chunks[i].getChunkID());
				}
				m_memoryManager.unlockAccess();

				backupPeers = new short[] {(short) (backupPeersAsLong & 0x000000000000FFFFL),
						(short) ((backupPeersAsLong & 0x00000000FFFF0000L) >> 16), (short) ((backupPeersAsLong & 0x0000FFFF00000000L) >> 32)};
				for (int i = 0; i < backupPeers.length; i++) {
					if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
						System.out.println("Logging " + chunks.length + " Chunks to " + backupPeers[i]);
						new LogMessage(backupPeers[i], chunks, versions).send(m_network);
					}
				}
			}
		}

		Operation.PUT.leave();
	}

	@Override
	public void remove(final long p_chunkID) throws DXRAMException {
		boolean success = false;

		Operation.REMOVE.enter();

		ChunkID.check(p_chunkID);

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			success = deleteChunkData(p_chunkID);
			m_lookup.remove(p_chunkID);
		}

		Operation.REMOVE.leave();

		if (!success) {
			throw new DXRAMException("chunk removal failed");
		}

	}

	@Override
	public void remove(final long[] p_chunkIDs) throws DXRAMException {
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

	/**
	 * Deletes the data (Chunk + Log) of one Chunk
	 * @param p_chunkID
	 *            the ChunkID
	 * @return whether this operation was successful or not
	 * @throws DXRAMException
	 *             if the Chunk could not be deleted
	 */
	private boolean deleteChunkData(final long p_chunkID) throws DXRAMException {
		Locations locations;
		byte rangeID;
		int version;
		boolean ret = false;
		short[] backupPeers;
		RemoveRequest request;

		while (!ret) {
			if (m_memoryManager.isResponsible(p_chunkID)) {
				if (!m_memoryManager.wasMigrated(p_chunkID)) {
					version = -1;
					m_memoryManager.lockManage();
					version = m_memoryManager.getVersion(p_chunkID);

					// Local remove
					m_memoryManager.remove(p_chunkID);
					m_memoryManager.unlockManage();

					if (LOG_ACTIVE) {
						// Send backups for logging (unreliable)
						backupPeers = getBackupPeersForLocalChunks(p_chunkID);
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
									new RemoveMessage(backupPeers[i], new long[] {p_chunkID}, new int[] {version}).send(m_network);
								}
							}
						}
					}
					ret = true;
				} else {
					version = -1;

					m_memoryManager.lockManage();
					version = m_memoryManager.getVersion(p_chunkID);

					// Local remove
					m_memoryManager.remove(p_chunkID);
					m_memoryManager.unlockManage();

					rangeID = m_migrationsTree.getBackupRange(p_chunkID);
					m_migrationsTree.removeObject(p_chunkID);

					// Inform creator about removal
					request = new RemoveRequest(ChunkID.getCreatorID(p_chunkID), p_chunkID, version);
					try {
						request.sendSync(m_network);
						request.getResponse(RemoveResponse.class);
					} catch (final NetworkException e) {
						System.out.println("Cannot inform creator about removal! Is not available!");
					}

					if (LOG_ACTIVE) {
						// Send backups for logging (unreliable)
						backupPeers = getBackupPeersForLocalChunks(p_chunkID);
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
									new RemoveMessage(backupPeers[i], new long[] {p_chunkID}, new int[] {version}, rangeID).send(m_network);
								}
							}
						}
					}
					ret = true;
				}
			} else {
				locations = m_lookup.get(p_chunkID);
				if (locations != null) {
					// Remote remove
					request = new RemoveRequest(locations.getPrimaryPeer(), p_chunkID);
					try {
						request.sendSync(m_network);
					} catch (final NetworkException e) {
						m_lookup.invalidate(p_chunkID);
						continue;
					}
					ret = request.getResponse(RemoveResponse.class).getStatus();
				}
			}
		}

		return ret;
	}

	@Override
	public Chunk lock(final long p_chunkID) throws DXRAMException {
		return lock(p_chunkID, false);
	}

	@Override
	public Chunk lock(final long p_chunkID, final boolean p_readLock) throws DXRAMException {
		Chunk ret = null;
		DefaultLock lock;
		short primaryPeer;
		LockRequest request;
		LockResponse response;

		Operation.LOCK.enter();

		ChunkID.check(p_chunkID);

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (m_memoryManager.isResponsible(p_chunkID)) {
				int size;
				int bytesRead;

				// Local lock
				lock = new DefaultLock(p_chunkID, m_nodeID, p_readLock);
				m_lock.lock(lock);

				m_memoryManager.lockAccess();
				size = m_memoryManager.getSize(p_chunkID);
				ret = new Chunk(p_chunkID, size);
				bytesRead = m_memoryManager.get(p_chunkID, ret.getData().array(), 0, size);
				m_memoryManager.unlockAccess();
			} else {
				while (null == ret || -1 == ret.getChunkID()) {
					primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();
					if (primaryPeer == m_nodeID) {
						int size;
						int bytesRead;

						// Local lock
						lock = new DefaultLock(p_chunkID, m_nodeID, p_readLock);
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

		Operation.LOCK.leave();

		return ret;
	}

	@Override
	public void unlock(final long p_chunkID) throws DXRAMException {
		short primaryPeer;
		UnlockMessage request;

		Operation.UNLOCK.enter();

		ChunkID.check(p_chunkID);

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (m_memoryManager.isResponsible(p_chunkID)) {
				// Local release
				m_lock.unlock(p_chunkID, m_nodeID);
			} else {
				while (true) {
					primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();
					if (primaryPeer == m_nodeID) {
						// Local release
						m_lock.unlock(p_chunkID, m_nodeID);
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

		Operation.UNLOCK.leave();
	}

	@Override
	public void putRecoveredChunks(final Chunk[] p_chunks) throws DXRAMException {
		Contract.checkNotNull(p_chunks, "no chunks given");

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			putForeignChunks(p_chunks, null);
		}
	}

	/**
	 * Puts migrated or recovered Chunks
	 * @param p_chunks
	 *            the Chunks
	 * @param p_versions
	 *            the versions of the Chunks (for migrations, recovered Chunks get version 1)
	 * @throws LookupException
	 *             if the backup range could not be initialized on superpeers
	 * @throws NetworkException
	 *             if the put Chunks could not be logged properly
	 * @throws MemoryException
	 *             if the Chunks could not be put properly
	 */
	public void putForeignChunks(final Chunk[] p_chunks, final int[] p_versions) throws LookupException, NetworkException, MemoryException {
		int logEntrySize;
		int version = 1;
		long size = 0;
		long cutChunkID = -1;
		short[] backupPeers = null;
		Chunk chunk = null;

		// multi put
		/*-m_memoryManager.lockManage();
		for (Chunk chunk : p_chunks) {
			int bytesWritten;

			m_memoryManager.create(chunk.getChunkID());
			bytesWritten = m_memoryManager.put(chunk.getChunkID(), chunk.getData().array(), 0, chunk.getData().array().length);
		}
		m_memoryManager.unlockManage();*/

		for (int i = 0; i < p_chunks.length; i++) {
			chunk = p_chunks[i];
			if (p_versions == null) {
				m_memoryManager.lockAccess();
				version = m_memoryManager.getVersion(chunk.getChunkID());
				m_memoryManager.unlockAccess();
			} else {
				version = p_versions[i];
			}

			if (LOG_ACTIVE) {
				logEntrySize = chunk.getSize() + m_log.getHeaderSize(ChunkID.getCreatorID(chunk.getChunkID()), ChunkID.getLocalID(chunk.getChunkID()),
						chunk.getSize(), version);
				if (m_migrationsTree.fits(size + logEntrySize) && (m_migrationsTree.size() != 0 || size > 0)) {
					// Chunk fits in current migration backup range
					size += logEntrySize;
				} else {
					// Chunk does not fit -> initialize new migration backup range and remember cut
					size = logEntrySize;
					cutChunkID = chunk.getChunkID();

					determineBackupPeers(-1);
					m_migrationsTree.initNewBackupRange();

					m_lookup.initRange(((long) -1 << 48) + m_currentMigrationBackupRange.getRangeID(), new Locations(m_nodeID,
							m_currentMigrationBackupRange.getBackupPeers(), null));
					m_log.initBackupRange(((long) -1 << 48) + m_currentMigrationBackupRange.getRangeID(), m_currentMigrationBackupRange.getBackupPeers());
				}
			}
		}

		if (LOG_ACTIVE) {
			for (int i = 0; i < p_chunks.length; i++) {
				chunk = p_chunks[i];
				if (p_versions == null) {
					m_memoryManager.lockAccess();
					version = m_memoryManager.getVersion(chunk.getChunkID());
					m_memoryManager.unlockAccess();
				} else {
					version = p_versions[i];
				}

				if (chunk.getChunkID() == cutChunkID) {
					// All following chunks are in the new migration backup range
					backupPeers = m_currentMigrationBackupRange.getBackupPeers();
				}

				m_migrationsTree.putObject(chunk.getChunkID(), (byte) m_currentMigrationBackupRange.getRangeID(), chunk.getSize());

				if (backupPeers != null) {
					for (int j = 0; j < backupPeers.length; j++) {
						if (backupPeers[j] != m_nodeID && backupPeers[j] != -1) {
							new LogMessage(backupPeers[j], (byte) m_currentMigrationBackupRange.getRangeID(),
									new Chunk[] {chunk}, new int[] {version}).send(m_network);
						}
					}
				}
			}
		}
	}

	@Override
	public boolean migrate(final long p_chunkID, final short p_target) throws DXRAMException, NetworkException {
		short[] backupPeers;
		int version;
		Chunk chunk;
		boolean ret = false;

		ChunkID.check(p_chunkID);
		NodeID.check(p_target);

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not store chunks");
		} else {
			m_migrationLock.lock();
			if (p_target != m_nodeID && m_memoryManager.isResponsible(p_chunkID)) {
				int size;
				int bytesRead;

				chunk = null;

				m_memoryManager.lockAccess();
				size = m_memoryManager.getSize(p_chunkID);
				chunk = new Chunk(p_chunkID, size);
				bytesRead = m_memoryManager.get(p_chunkID, chunk.getData().array(), 0, size);
				version = m_memoryManager.getVersion(p_chunkID);
				m_memoryManager.unlockAccess();

				if (chunk != null) {
					LOGGER.trace("Send request to " + p_target);

					new DataMessage(p_target, new Chunk[] {chunk}, new int[] {version}).send(m_network);

					// Update superpeers
					m_lookup.migrate(p_chunkID, p_target);
					// Remove all locks
					m_lock.unlockAll(p_chunkID);
					// Update local memory management
					m_memoryManager.remove(p_chunkID);
					if (LOG_ACTIVE) {
						// Update logging
						backupPeers = getBackupPeersForLocalChunks(p_chunkID);
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
									new RemoveMessage(backupPeers[i], new long[] {p_chunkID},
											new int[] {version}).send(m_network);
								}
							}
						}
					}
					ret = true;
				}
			} else {
				System.out.println("Chunk with ChunkID " + p_chunkID + " could not be migrated!");
				ret = false;
			}
			m_migrationLock.unlock();
		}
		return ret;
	}

	@Override
	public boolean migrateRange(final long p_startChunkID, final long p_endChunkID, final short p_target) throws DXRAMException {
		long[] chunkIDs = null;
		int[] versions = null;
		short[] backupPeers;
		int counter = 0;
		long iter;
		long size;
		Chunk chunk;
		Chunk[] chunks;
		boolean ret = false;

		ChunkID.check(p_startChunkID);
		ChunkID.check(p_endChunkID);
		NodeID.check(p_target);

		// TODO: Handle range properly

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not store chunks");
		} else {
			if (p_startChunkID <= p_endChunkID) {
				chunkIDs = new long[(int) (p_endChunkID - p_startChunkID + 1)];
				versions = new int[(int) (p_endChunkID - p_startChunkID + 1)];
				m_migrationLock.lock();
				if (p_target != m_nodeID) {
					iter = p_startChunkID;
					while (true) {
						// Send chunks to p_target
						chunks = new Chunk[(int) (p_endChunkID - iter + 1)];
						counter = 0;
						size = 0;
						m_memoryManager.lockAccess();
						while (iter <= p_endChunkID) {
							if (m_memoryManager.isResponsible(iter)) {
								int bytesRead;
								int sizeChunk;

								chunk = null;

								sizeChunk = m_memoryManager.getSize(iter);
								chunk = new Chunk(iter, sizeChunk);
								bytesRead = m_memoryManager.get(iter, chunk.getData().array(), 0, sizeChunk);

								chunks[counter] = chunk;
								chunkIDs[counter] = chunk.getChunkID();
								versions[counter++] = m_memoryManager.getVersion(iter);
								size += chunk.getSize();
							} else {
								System.out.println("Chunk with ChunkID " + iter + " could not be migrated!");
							}
							iter++;
						}
						m_memoryManager.unlockAccess();

						System.out.println("Sending " + counter + " Chunks (" + size + " Bytes) to " + p_target);
						new DataMessage(p_target, Arrays.copyOf(chunks, counter), Arrays.copyOf(versions, counter)).send(m_network);

						if (iter > p_endChunkID) {
							break;
						}
					}

					// Update superpeers
					m_lookup.migrateRange(p_startChunkID, p_endChunkID, p_target);

					if (LOG_ACTIVE) {
						// Update logging
						backupPeers = getBackupPeersForLocalChunks(iter);
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
									new RemoveMessage(backupPeers[i], chunkIDs, versions).send(m_network);
								}
							}
						}
					}

					iter = p_startChunkID;
					while (iter <= p_endChunkID) {
						// Remove all locks
						m_lock.unlockAll(iter);
						// Update local memory management
						m_memoryManager.remove(iter);
						iter++;
					}
					ret = true;
				} else {
					System.out.println("Chunks could not be migrated because end of range is before start of range!");
					ret = false;
				}
			} else {
				System.out.println("Chunks could not be migrated!");
				ret = false;
			}
			m_migrationLock.unlock();
			System.out.println("All chunks migrated!");
		}
		return ret;
	}

	/**
	 * Migrates a Chunk that was not created on this node; is called during promotion
	 * @param p_chunkID
	 *            the ID
	 * @param p_target
	 *            the Node where to migrate the Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be migrated
	 */
	private void migrateNotCreatedChunk(final long p_chunkID, final short p_target) throws DXRAMException {
		Chunk chunk;
		short creator;
		short target;
		short[] backupPeers;
		int version;

		ChunkID.check(p_chunkID);
		NodeID.check(p_target);
		creator = ChunkID.getCreatorID(p_chunkID);

		m_migrationLock.lock();
		if (p_target != m_nodeID && m_memoryManager.isResponsible(p_chunkID) && m_memoryManager.wasMigrated(p_chunkID)) {
			chunk = null;
			// TODO: enable
			m_memoryManager.lockAccess();
			// chunk = m_memoryManager.get(p_chunkID);
			version = m_memoryManager.getVersion(p_chunkID);
			m_memoryManager.unlockAccess();
			LOGGER.trace("Send request to " + p_target);

			if (m_lookup.creatorAvailable(creator)) {
				// Migrate chunk back to owner
				System.out.println("** Migrating " + p_chunkID + " back to " + creator);
				target = creator;
			} else {
				// Migrate chunk to p_target, if owner is not available anymore
				System.out.println("** Migrating " + p_chunkID + " to " + p_target);
				target = p_target;
			}

			// This is not safe, but there is no other possibility unless
			// the number of network threads is increased
			new DataMessage(target, new Chunk[] {chunk}, new int[] {version}).send(m_network);

			// Update superpeers
			m_lookup.migrateNotCreatedChunk(p_chunkID, target);
			// Remove all locks
			m_lock.unlockAll(p_chunkID);
			// Update local memory management
			m_memoryManager.remove(p_chunkID);
			if (LOG_ACTIVE) {
				// Update logging
				backupPeers = getBackupPeersForLocalChunks(p_chunkID);
				if (backupPeers != null) {
					for (int i = 0; i < backupPeers.length; i++) {
						if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
							new RemoveMessage(backupPeers[i], new long[] {p_chunkID}, new int[] {version}).send(m_network);
						}
					}
				}
			}
		}
		m_migrationLock.unlock();
	}

	/**
	 * Migrates a Chunk that was created on this node; is called during promotion
	 * @param p_chunkID
	 *            the ID
	 * @param p_target
	 *            the Node where to migrate the Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be migrated
	 */
	private void migrateOwnChunk(final long p_chunkID, final short p_target) throws DXRAMException {
		short[] backupPeers;
		int version;
		Chunk chunk;

		ChunkID.check(p_chunkID);
		NodeID.check(p_target);

		m_migrationLock.lock();
		if (p_target != m_nodeID) {
			int sizeChunk;
			int bytesRead;

			chunk = null;

			m_memoryManager.lockAccess();
			sizeChunk = m_memoryManager.getSize(p_chunkID);
			chunk = new Chunk(p_chunkID, sizeChunk);
			bytesRead = m_memoryManager.get(p_chunkID, chunk.getData().array(), 0, sizeChunk);
			version = m_memoryManager.getVersion(p_chunkID);
			m_memoryManager.unlockAccess();

			if (chunk != null) {
				LOGGER.trace("Send request to " + p_target);

				// This is not safe, but there is no other possibility unless
				// the number of network threads is increased
				System.out.println("** Migrating own chunk " + p_chunkID + " to " + p_target);
				new DataMessage(p_target, new Chunk[] {chunk}, new int[] {version}).send(m_network);

				// Update superpeers
				m_lookup.migrateOwnChunk(p_chunkID, p_target);
				// Remove all locks
				m_lock.unlockAll(p_chunkID);
				// Update local memory management
				m_memoryManager.remove(p_chunkID);
				if (LOG_ACTIVE) {
					// Update logging
					backupPeers = getBackupPeersForLocalChunks(p_chunkID);
					if (backupPeers != null) {
						for (int i = 0; i < backupPeers.length; i++) {
							if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
								new RemoveMessage(backupPeers[i], new long[] {p_chunkID},
										new int[] {version}).send(m_network);
							}
						}
					}
				}
			}
		}
		m_migrationLock.unlock();
	}

	@Override
	public void migrateAll(final short p_target) throws DXRAMException {
		long localID;
		long chunkID;
		Iterator<Long> iter;

		// TODO
		localID = -1;
		// localID = m_memoryManager.getCurrentLocalID();

		// Migrate own chunks to p_target
		if (1 != localID) {
			for (int i = 1; i <= localID; i++) {
				chunkID = ((long) m_nodeID << 48) + i;
				if (m_memoryManager.isResponsible(chunkID)) {
					migrateOwnChunk(chunkID, p_target);
				}
			}
		}

		// TODO
		iter = null;
		// iter = m_memoryManager.getCIDOfAllMigratedChunks().iterator();
		while (iter.hasNext()) {
			chunkID = iter.next();
			migrateNotCreatedChunk(chunkID, p_target);
		}
	}

	/**
	 * Registers peer in superpeer overlay
	 * @throws LookupException
	 *             if range could not be initialized
	 */
	private void registerPeer() throws LookupException {
		m_lookup.initRange(0, new Locations(m_nodeID, null, null));
	}

	/**
	 * Returns the corresponding backup peers
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the backup peers
	 */
	private short[] getBackupPeersForLocalChunks(final long p_chunkID) {
		short[] ret = null;

		if (ChunkID.getCreatorID(p_chunkID) == NodeID.getLocalNodeID()) {
			for (int i = m_ownBackupRanges.size() - 1; i >= 0; i--) {
				if (m_ownBackupRanges.get(i).getRangeID() <= ChunkID.getLocalID(p_chunkID)) {
					ret = m_ownBackupRanges.get(i).getBackupPeers();
					break;
				}
			}
		} else {
			ret = m_migrationBackupRanges.get(m_migrationsTree.getBackupRange(p_chunkID)).getBackupPeers();
		}

		return ret;
	}

	/**
	 * Returns the corresponding backup peers
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the backup peers
	 */
	private long getBackupPeersForLocalChunksAsLong(final long p_chunkID) {
		long ret = -1;

		if (ChunkID.getCreatorID(p_chunkID) == NodeID.getLocalNodeID()) {
			for (int i = m_ownBackupRanges.size() - 1; i >= 0; i--) {
				if (m_ownBackupRanges.get(i).getRangeID() <= ChunkID.getLocalID(p_chunkID)) {
					ret = m_ownBackupRanges.get(i).getBackupPeersAsLong();
					break;
				}
			}
		} else {
			ret = m_migrationBackupRanges.get(m_migrationsTree.getBackupRange(p_chunkID)).getBackupPeersAsLong();
		}

		return ret;
	}



	/**
	 * Determines backup peers
	 * @param p_localID
	 *            the current LocalID
	 */
	private void determineBackupPeers(final long p_localID) {
		boolean ready = false;
		boolean insufficientPeers = false;
		short index = 0;
		short peer;
		short[] oldBackupPeers = null;
		short[] newBackupPeers = null;
		short[] allPeers;
		short numberOfPeers = 0;
		List<String> peers = null;

		// Get all other online peers
		try {
			peers = ZooKeeperHandler.getChildren("nodes/peers");
		} catch (final ZooKeeperException e) {
			System.out.println("Could not access ZooKeeper!");
		}
		allPeers = new short[peers.size() - 1];
		for (int i = 0; i < peers.size(); i++) {
			peer = Short.parseShort(peers.get(i));
			if (peer != NodeID.getLocalNodeID()) {
				allPeers[numberOfPeers++] = peer;
			}
		}

		if (3 > numberOfPeers) {
			LOGGER.warn("Less than three peers for backup available. Replication will be incomplete!");

			newBackupPeers = new short[numberOfPeers];
			Arrays.fill(newBackupPeers, (short) -1);

			insufficientPeers = true;
		} else if (6 > numberOfPeers) {
			LOGGER.warn("Less than six peers for backup available. Some peers may store more" + " than one backup range of a node!");

			oldBackupPeers = new short[REPLICATION_FACTOR];
			Arrays.fill(oldBackupPeers, (short) -1);

			newBackupPeers = new short[REPLICATION_FACTOR];
			Arrays.fill(newBackupPeers, (short) -1);
		} else if (null != m_currentBackupRange.getBackupPeers()) {
			oldBackupPeers = new short[REPLICATION_FACTOR];
			for (int i = 0; i < REPLICATION_FACTOR; i++) {
				if (p_localID > -1) {
					oldBackupPeers[i] = m_currentBackupRange.getBackupPeers()[i];
				} else {
					oldBackupPeers[i] = m_currentMigrationBackupRange.getBackupPeers()[i];
				}
			}

			newBackupPeers = new short[REPLICATION_FACTOR];
			Arrays.fill(newBackupPeers, (short) -1);
		}

		if (insufficientPeers) {
			if (numberOfPeers > 0) {
				// Determine backup peers
				for (int i = 0; i < numberOfPeers; i++) {
					while (!ready) {
						index = (short) (Math.random() * numberOfPeers);
						ready = true;
						for (int j = 0; j < i; j++) {
							if (allPeers[index] == newBackupPeers[j]) {
								ready = false;
								break;
							}
						}
					}
					System.out.println(i + 1 + ". backup peer: " + allPeers[index]);
					newBackupPeers[i] = allPeers[index];
					ready = false;
				}
				if (p_localID > -1) {
					m_currentBackupRange = new BackupRange(p_localID, newBackupPeers);
				} else {
					m_currentMigrationBackupRange = new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, newBackupPeers);
				}
			} else {
				if (p_localID > -1) {
					m_currentBackupRange = new BackupRange(p_localID, null);
				} else {
					m_currentMigrationBackupRange = new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, null);
				}
			}
		} else {
			// Determine backup peers
			for (int i = 0; i < 3; i++) {
				while (!ready) {
					index = (short) (Math.random() * numberOfPeers);
					ready = true;
					for (int j = 0; j < i; j++) {
						if (allPeers[index] == oldBackupPeers[j] || allPeers[index] == newBackupPeers[j]) {
							ready = false;
							break;
						}
					}
				}
				System.out.println(i + 1 + ". backup peer: " + allPeers[index]);
				newBackupPeers[i] = allPeers[index];
				ready = false;
			}
			if (p_localID > -1) {
				m_currentBackupRange = new BackupRange(p_localID, newBackupPeers);
			} else {
				m_currentMigrationBackupRange = new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, newBackupPeers);
			}
		}

		if (numberOfPeers > 0) {
			if (p_localID > -1) {
				m_ownBackupRanges.add(m_currentBackupRange);
			} else {
				m_migrationBackupRanges.add(m_currentMigrationBackupRange);
			}
		}
	}

	/**
	 * Inserts given ChunkID with key in index chunk
	 * @param p_key
	 *            the key to be inserted
	 * @param p_chunkID
	 *            the ChunkID to be inserted
	 * @param p_chunk
	 *            the index chunk
	 * @throws DXRAMException
	 *             if there is no fitting chunk
	 */
	private void insertMapping(final int p_key, final long p_chunkID, final Chunk p_chunk) throws DXRAMException {
		int size;
		ByteBuffer indexData;
		ByteBuffer appendixData;
		Chunk indexChunk = p_chunk;
		Chunk appendix;

		// Iterate over index chunks to get to the last one
		while (true) {
			// Get data and number of written bytes of current index chunk
			indexData = indexChunk.getData();
			size = indexData.getInt(0);

			if (-1 != indexData.getInt(indexData.capacity() - 12)) {
				// This is the last index chunk
				if (24 <= indexData.capacity() - size) {
					// If there is at least 24 Bytes (= two entries; the last one of every index file is needed
					// to address the next index chunk) left in this index chunk, the new entry will be appended

					// Set position on first unwritten byte and add <ID, ChunkID>
					indexData.position(size);
					indexData.putInt(p_key);
					indexData.putLong(p_chunkID);
					indexData.putInt(0, size + 12);
					put(indexChunk);
				} else {
					// The last index chunk is full -> create new chunk and add its address to the old one
					appendix = create(INDEX_SIZE);
					appendixData = appendix.getData();
					appendixData.putInt(4 + 12);
					appendixData.putInt(p_key);
					appendixData.putLong(p_chunkID);
					put(appendix);

					indexData.position(indexData.capacity() - 12);
					indexData.putInt(-1);
					indexData.putLong(appendix.getChunkID());
					put(indexChunk);
				}
				break;
			}
			// Get next index file and repeat
			indexChunk = get(indexData.getLong(indexData.capacity() - 8));
		}
	}

	/**
	 * Removes given key in index chunk
	 * @param p_key
	 *            the key to be removed
	 * @param p_chunk
	 *            the index chunk
	 * @throws DXRAMException
	 *             if there is no fitting chunk
	 */
	@SuppressWarnings("unused")
	private void removeMapping(final int p_key, final Chunk p_chunk) throws DXRAMException {
		int j = 0;
		int id;
		int size;
		ByteBuffer indexData;
		ByteBuffer lastIndexData;
		Chunk indexChunk;
		Chunk lastIndexChunk;
		Chunk predecessorChunk = null;

		indexChunk = p_chunk;
		indexData = indexChunk.getData();

		while (true) {
			// Get j-th ID from index chunk
			id = indexData.getInt(j * 12 + 4);
			if (id == p_key) {
				// ID is found -> remove entry
				if (-1 != indexData.getInt(indexData.capacity() - 12)) {
					// Deletion in last index chunk
					deleteEntryInLastIndexFile(indexChunk, predecessorChunk, j);
				} else {
					// Deletion in index file with successor -> replace entry with last entry in whole list
					// Find last index chunk
					lastIndexChunk = indexChunk;
					lastIndexData = indexData;
					while (-1 == lastIndexData.getInt(lastIndexData.capacity() - 12)) {
						predecessorChunk = lastIndexChunk;
						lastIndexChunk = get(lastIndexData.getLong(lastIndexData.capacity() - 8));
						lastIndexData = lastIndexChunk.getData();
					}
					// Replace entry that should be removed with last entry from last index file
					size = lastIndexData.getInt(0);
					indexData.putInt(j * 12 + 4, lastIndexData.getInt(size - 12));
					indexData.putLong(j * 12 + 4 + 4, lastIndexData.getLong(size - 8));
					put(indexChunk);
					// Remove last entry from last index file
					deleteEntryInLastIndexFile(lastIndexChunk, predecessorChunk, (size - 4) / 12 - 1);
				}
				break;
			} else if (id == -1) {
				// Get next user index and remember current chunk (may be needed for deletion)
				predecessorChunk = indexChunk;
				indexChunk = get(indexData.getLong(indexData.capacity() - 8));
				indexData = indexChunk.getData();
				j = 0;
				continue;
			}
			j++;
		}
	}

	/**
	 * Deletes the entry with given index in index chunk
	 * @param p_indexChunk
	 *            the index chunk
	 * @param p_predecessorChunk
	 *            the parent index chunk
	 * @param p_index
	 *            the index
	 * @throws DXRAMException
	 *             if there is no fitting chunk
	 */
	private void deleteEntryInLastIndexFile(final Chunk p_indexChunk, final Chunk p_predecessorChunk, final int p_index) throws DXRAMException {
		int size;
		byte[] data1;
		byte[] data2;
		byte[] allData;
		ByteBuffer indexData;

		// Get data and size of last index file
		indexData = p_indexChunk.getData();
		size = indexData.getInt(0);

		if (size > 16) {
			// If there is more than one entry -> shift all entries 12 Bytes to the left beginning
			// at p_index to overwrite entry
			data1 = new byte[p_index * 12 + 4];
			data2 = new byte[size - (p_index * 12 + 4 + 12)];
			allData = new byte[size];

			indexData.get(data1, 0, p_index * 12 + 4);
			indexData.position(p_index * 12 + 4 + 12);
			indexData.get(data2, 0, size - (p_index * 12 + 4 + 12));

			System.arraycopy(data1, 0, allData, 0, data1.length);
			System.arraycopy(data2, 0, allData, data1.length, data2.length);

			indexData.position(0);
			indexData.put(allData);
			indexData.putInt(0, size - 12);
			put(p_indexChunk);
		} else {
			// There is only one entry in index file
			if (null != p_predecessorChunk) {
				// If there is a predecessor, remove current index file and update predecessor
				remove(p_indexChunk.getChunkID());

				indexData = p_predecessorChunk.getData();
				indexData.position(indexData.getInt(0));
				// Overwrite the addressing to predecessor's successor with zeros
				for (int i = 0; i < 12; i++) {
					indexData.put((byte) 0);
				}
				put(p_predecessorChunk);
			} else {
				// If there is no predecessor, the entry to remove is the last entry in list
				// -> overwrite <ID, ChunkID> with zeros and update size
				indexData.position(0);
				indexData.putInt(4);
				for (int i = 0; i < 12; i++) {
					indexData.put((byte) 0);
				}
				put(p_indexChunk);
			}
		}
	}

	/**
	 * Handles an incoming GetRequest
	 * @param p_request
	 *            the GetRequest
	 */
	private void incomingGetRequest(final GetRequest p_request) {
		Chunk chunk;

		Operation.INCOMING_GET.enter();

		try {
			// TODO
			chunk = null;
			// chunk = m_memoryManager.get(p_request.getChunkID());

			new GetResponse(p_request, chunk).send(m_network);
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle request", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}

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
		int version;
		long chunkID;
		short[] backupPeers = null;
		RemoveRequest request;

		Operation.INCOMING_REMOVE.enter();

		chunkID = p_request.getChunkID();
		version = p_request.getVersion();

		try {

			m_migrationLock.lock();
			if (version != -1) {
				m_memoryManager.prepareChunkIDForReuse(chunkID, version);
				success = true;
			} else if (m_memoryManager.isResponsible(chunkID)) {
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
					version = m_memoryManager.getVersion(chunkID);
					m_memoryManager.remove(chunkID);
					m_memoryManager.unlockManage();

					m_migrationsTree.removeObject(chunkID);

					// Inform creator about removal
					request = new RemoveRequest(ChunkID.getCreatorID(chunkID), chunkID, version);
					try {
						request.sendSync(m_network);
						request.getResponse(RemoveResponse.class);
					} catch (final NetworkException e) {
						System.out.println("Cannot inform creator about removal! Is not available!");
					}
				}

				replicate = true;
				success = true;
			}
			m_migrationLock.unlock();

			new RemoveResponse(p_request, success).send(m_network);

			if (LOG_ACTIVE && replicate) {
				// Send backups for logging (unreliable)
				if (backupPeers != null) {
					for (int i = 0; i < backupPeers.length; i++) {
						if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
							new RemoveMessage(backupPeers[i], new long[] {chunkID}, new int[] {version}, rangeID).send(m_network);
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
			putForeignChunks(p_request.getChunks(), p_request.getVersions());

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
			putForeignChunks(p_message.getChunks(), p_message.getVersions());
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle message", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_message);
		}
	}

	/**
	 * Handles an incoming CommandMessage
	 * @param p_message
	 *            the CommandMessage
	 */
	private void incomingCommandMessage(final ChunkCommandMessage p_message) {
		String cmd;

		Operation.INCOMING_COMMAND.enter();

		cmd = p_message.getCommand();

		if (Core.getCommandListener() != null) {
			Core.getCommandListener().processCmd(cmd, false);
		} else {
			System.out.println("error: command message received but no command listener registered");
		}

		Operation.INCOMING_COMMAND.leave();
	}

	/**
	 * Handles 'chunkinfo' command. Called by incomingCommandRequest
	 * @param p_command
	 *            the CommandMessage
	 * @return the result string
	 */
	private String cmdReqChunkinfo(final String p_command) {
		short nodeID;
		short primaryPeer;
		short[] backupPeers;
		long localID;
		long chunkID;
		String ret = null;
		String[] arguments;

		arguments = p_command.split(" ");

		nodeID = CmdUtils.getNIDfromTuple(arguments[1]);
		localID = CmdUtils.getLIDfromTuple(arguments[1]);
		chunkID = CmdUtils.getCIDfromTuple(arguments[1]);
		System.out.println("   cmdReqChunkinfo for " + nodeID + "," + localID);

		try {

			if (m_memoryManager.isResponsible(chunkID)) {
				backupPeers = getBackupPeersForLocalChunks(chunkID);
				ret = "  Stored on peer=" + m_nodeID + ", backup_peers=" + Arrays.toString(backupPeers);
			} else {
				primaryPeer = m_lookup.get(chunkID).getPrimaryPeer();
				ret = "  Chunk not stored on this peer. Contact peer " + primaryPeer + " or a superpeer";
			}
		} catch (final DXRAMException de) {
			ret = "error: " + de.toString();
		}

		return ret;
	}

	/**
	 * Handles 'chunkinfo' command. Called by incomingCommandRequest
	 * @param p_command
	 *            the CommandMessage
	 * @return the result string
	 */
	private String cmdReqCIDT(final String p_command) {
		String ret;
		ArrayList<Long> chunkIDLocalArray;
		ArrayList<Long> chunkIDMigratedArray;

		try {
			// de.uniduesseldorf.dxram.core.chunk.storage.CIDTable.printDebugInfos();
			// System.out.println("cmdReqCIDT 0");
			// TODO
			chunkIDLocalArray = null;
			// chunkIDLocalArray = m_memoryManager.getCIDrangesOfAllLocalChunks();
			// System.out.println("cmdReqCIDT 1");
			ret = "  Local (ranges): ";
			if (chunkIDLocalArray == null) {
				ret = ret + "empty.\n";
			} else if (chunkIDLocalArray.size() == 0) {
				ret = ret + "empty.\n";
			} else {
				boolean first = true;
				for (long l : chunkIDLocalArray) {
					if (first) {
						first = false;
					} else {
						ret = ret + ",";
					}
					ret += CmdUtils.getLIDfromCID(l);
				}
				ret = ret + "\n";
			}
			// System.out.println("cmdReqCIDT 2");

			// TODO
			chunkIDMigratedArray = null;
			// chunkIDMigratedArray = m_memoryManager.getCIDOfAllMigratedChunks();
			// System.out.println("cmdReqCIDT 3");

			ret = ret + "  Migrated (NodeID,LocalID): ";
			if (chunkIDMigratedArray == null) {
				ret = ret + "empty.";
			} else if (chunkIDMigratedArray.size() == 0) {
				ret = ret + "empty.";
			} else {
				boolean first = true;
				for (long l : chunkIDMigratedArray) {
					if (first) {
						first = false;
					} else {
						ret = ret + "; ";
					}
					ret += CmdUtils.getTupleFromCID(l);
				}
			}
		} finally {}
		// TODO take back in when done fixing stuff above
		// } catch (final MemoryException me) {
		// System.out.println("cmdReqCIDT: MemoryException");
		// ret = "error: internal error";
		// }
		return ret;
	}

	/**
	 * Handles 'backups' command. Called by incomingCommandRequest
	 * @param p_command
	 *            the CommandMessage
	 * @return the result string
	 */
	private String cmdReqBackups(final String p_command) {
		String ret = "";

		// System.out.println("ChunkHandler.cmdReqBackups");
		ret = ret + "  Backup ranges for locally created chunks\n";
		if (m_ownBackupRanges != null) {
			// System.out.println("   m_ownBackupRanges");
			for (int i = 0; i < m_ownBackupRanges.size(); i++) {
				final BackupRange br = m_ownBackupRanges.get(i);
				ret = ret + "    BR" + Integer.toString(i) + ": ";

				if (br != null) {
					// System.out.println("   BackupRange: "+i+", m_firstChunkIDORRangeID="+br.m_firstChunkIDORRangeID);
					ret = ret + Long.toString(br.m_firstChunkIDORRangeID) + " (";

					if (m_ownBackupRanges.size() == 0) {
						ret = ret + "    None.";
					} else {
						for (int j = 0; j < br.m_backupPeers.length; j++) {
							// System.out.println("      backup peer: "+j+": "+br.m_backupPeers[j]);
							ret = ret + Short.toString(br.m_backupPeers[j]);
							if (j < br.m_backupPeers.length - 1) {
								ret = ret + ",";
							}
						}
					}
					ret = ret + ")\n";
				}
			}
			if (m_ownBackupRanges.size() == 0) {
				ret = ret + "    None.\n";
			}
		} else {
			ret = ret + "    None.\n";
		}

		ret = ret + "  Backup ranges for migrated chunks\n";
		if (m_migrationBackupRanges != null) {
			// System.out.println("   m_migrationBackupRanges");
			for (int i = 0; i < m_migrationBackupRanges.size(); i++) {
				final BackupRange br = m_migrationBackupRanges.get(i);
				ret = ret + "    BR" + Integer.toString(i) + ": ";

				if (br != null) {
					// System.out.println("   BackupRange: "+i+", m_firstChunkIDORRangeID="+br.m_firstChunkIDORRangeID);
					ret = ret + Long.toString(br.m_firstChunkIDORRangeID) + " (";

					for (int j = 0; j < br.m_backupPeers.length; j++) {
						// System.out.println("      backup peer: "+j+": "+br.m_backupPeers[j]);
						ret = ret + Short.toString(br.m_backupPeers[j]);
						if (j < br.m_backupPeers.length - 1) {
							ret = ret + ",";
						}
					}
					ret = ret + ")\n";
				}
				if (m_migrationBackupRanges.size() == 0) {
					ret = ret + "    None.\n";
				}
			}
			if (m_migrationBackupRanges.size() == 0) {
				ret = ret + "    None.\n";
			}
		} else {
			ret = ret + "    None.\n";
		}

		return ret;
	}

	/**
	 * Handles an incoming CommandRequest
	 * @param p_request
	 *            the CommandRequest
	 */
	private void incomingCommandRequest(final ChunkCommandRequest p_request) {
		String cmd;
		String res = null;

		Operation.INCOMING_COMMAND.enter();

		cmd = p_request.getArgument();

		if (cmd.indexOf("chunkinfo") >= 0) {
			res = cmdReqChunkinfo(cmd);
		} else if (cmd.indexOf("backups") >= 0) {
			res = cmdReqBackups(cmd);
		} else if (cmd.indexOf("cidt") >= 0) {
			res = cmdReqCIDT(cmd);
		} else if (cmd.indexOf("stats") >= 0) {
			// stats command?
			res = StatisticsManager.getStatistics();
		} else {
			// command handled in callback?
			if (Core.getCommandListener() != null) {
				res = Core.getCommandListener().processCmd(cmd, true);
			} else {
				res = "error: no command listener registered";
				System.out.println("error: command request received but no command listener registered");
			}
		}

		// send back result
		try {
			new ChunkCommandResponse(p_request, res).send(m_network);
		} catch (final NetworkException e) {
			e.printStackTrace();
		}
		Operation.INCOMING_COMMAND.leave();
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

	@Override
	public void triggerEvent(final ConnectionLostEvent p_event) {
		Contract.checkNotNull(p_event, "no event given");

		try {
			m_lock.unlockAll(p_event.getSource());
		} catch (final DXRAMException e) {}
	}

	/**
	 * Triggers an IncomingChunkEvent at the IncomingChunkListener
	 * @param p_event
	 *            the IncomingChunkEvent
	 */
	private void fireIncomingChunk(final IncomingChunkEvent p_event) {
		if (m_listener != null) {
			m_listener.triggerEvent(p_event);
		}
	}

	// Classes
	/**
	 * Action, that will be executed, if a GetRequest is fullfilled
	 * @author Florian Klein 13.04.2012
	 */
	private class GetRequestAction extends AbstractAction<AbstractRequest> {

		// Constructors
		/**
		 * Creates an instance of GetRequestAction
		 */
		GetRequestAction() {}

		// Methods
		/**
		 * Executes the Action
		 * @param p_request
		 *            the corresponding Request
		 */
		@Override
		public void execute(final AbstractRequest p_request) {
			GetResponse response;

			if (p_request != null) {
				LOGGER.trace("Request fulfilled: " + p_request);

				response = p_request.getResponse(GetResponse.class);
				fireIncomingChunk(new IncomingChunkEvent(response.getSource(), response.getChunk()));
			}
		}

	}

	/**
	 * Stores a backup range
	 * @author Kevin Beineke 10.06.2015
	 */
	public static final class BackupRange {

		// Attributes
		private long m_firstChunkIDORRangeID;
		private short[] m_backupPeers;

		// Constructors
		/**
		 * Creates an instance of Locations
		 * @param p_firstChunkIDORRangeID
		 *            the RangeID or the first ChunkID
		 * @param p_backupPeers
		 *            the backup peers
		 */
		public BackupRange(final long p_firstChunkIDORRangeID, final short[] p_backupPeers) {
			super();

			m_firstChunkIDORRangeID = p_firstChunkIDORRangeID;
			m_backupPeers = p_backupPeers;
		}

		/**
		 * Creates an instance of Locations
		 * @param p_firstChunkIDORRangeID
		 *            the RangeID or the first ChunkID
		 * @param p_backupPeers
		 *            the locations in long representation
		 */
		public BackupRange(final long p_firstChunkIDORRangeID, final long p_backupPeers) {
			this(p_firstChunkIDORRangeID, new short[] {(short) (p_backupPeers & 0x000000000000FFFFL),
					(short) ((p_backupPeers & 0x00000000FFFF0000L) >> 16), (short) ((p_backupPeers & 0x0000FFFF00000000L) >> 32)});
		}

		// Getter
		/**
		 * Returns RangeID or first ChunkID
		 * @return RangeID or first ChunkID
		 */
		public long getRangeID() {
			return m_firstChunkIDORRangeID;
		}

		/**
		 * Get backup peers
		 * @return the backup peers
		 */
		public short[] getBackupPeers() {
			return m_backupPeers;
		}

		/**
		 * Get backup peers as long
		 * @return the backup peers
		 */
		public long getBackupPeersAsLong() {
			long ret = -1;
			if (null != m_backupPeers) {
				if (m_backupPeers.length == 3) {
					ret =
							((m_backupPeers[2] & 0x000000000000FFFFL) << 32) + ((m_backupPeers[1] & 0x000000000000FFFFL) << 16)
									+ (m_backupPeers[0] & 0x000000000000FFFFL);
				} else if (m_backupPeers.length == 2) {
					ret = ((-1 & 0x000000000000FFFFL) << 32) + ((m_backupPeers[1] & 0x000000000000FFFFL) << 16) + (m_backupPeers[0] & 0x000000000000FFFFL);
				} else {
					ret = ((-1 & 0x000000000000FFFFL) << 32) + ((-1 & 0x000000000000FFFFL) << 16) + (m_backupPeers[0] & 0x000000000000FFFFL);
				}
			}

			return ret;
		}

		// Methods
		/**
		 * Prints the locations
		 * @return String interpretation of locations
		 */
		@Override
		public String toString() {
			String ret;

			ret = "" + m_firstChunkIDORRangeID;
			if (null != m_backupPeers) {
				if (m_backupPeers.length == 3) {
					ret = "[" + m_backupPeers[0] + ", " + m_backupPeers[1] + ", " + m_backupPeers[2] + "]";
				} else if (m_backupPeers.length == 2) {
					ret = "[" + m_backupPeers[0] + ", " + m_backupPeers[1] + "]";
				} else {
					ret = "[" + m_backupPeers[0] + "]";
				}
			} else {
				ret = "no backup peers";
			}

			return ret;
		}
	}

}

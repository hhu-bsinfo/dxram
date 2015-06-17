
package de.uniduesseldorf.dxram.core.chunk;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
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
import de.uniduesseldorf.dxram.utils.AbstractAction;
import de.uniduesseldorf.dxram.utils.Contract;
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

	private static final boolean LOG_ACTIVE = Core.getConfiguration().getBooleanValue(
			ConfigurationConstants.LOG_ACTIVE);
	private static final long SECONDARY_LOG_SIZE = Core.getConfiguration().getLongValue(
			ConfigurationConstants.SECONDARY_LOG_SIZE);
	private static final int REPLICATION_FACTOR = Core.getConfiguration().getIntValue(
			ConfigurationConstants.REPLICATION_FACTOR);

	// Attributes
	private short m_nodeID;
	private long m_rangeSize;

	private BackupRange m_currentBackupRange;
	private ArrayList<BackupRange> m_ownBackupRanges;

	private BackupRange m_currentMigrationBackupRange;
	private ArrayList<BackupRange> m_migrationBackupRanges;
	private MigrationsTree m_migrationsTree;

	private NetworkInterface m_network;
	private LookupInterface m_lookup;
	private LogInterface m_log;
	private LockInterface m_lock;

	private IncomingChunkListener m_listener;

	private Lock m_migrationLock;

	private Lock m_mappingLock;

	// Constructors
	/**
	 * Creates an instance of DataHandler
	 */
	public ChunkHandler() {
		m_nodeID = NodeID.INVALID_ID;
		m_ownBackupRanges = new ArrayList<BackupRange>();
		m_migrationBackupRanges = new ArrayList<BackupRange>();
		m_migrationsTree = new MigrationsTree((short) 10);
		m_currentBackupRange = null;
		m_currentMigrationBackupRange = new BackupRange(-1, null);
		m_rangeSize = 0;

		m_network = null;
		m_lookup = null;
		m_log = null;
		m_lock = null;

		m_listener = null;

		m_migrationLock = null;

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

		m_lookup = CoreComponentFactory.getLookupInterface();
		m_lookup.initChunkHandler();
		if (LOG_ACTIVE && !NodeID.isSuperpeer()) {
			m_log = CoreComponentFactory.getLogInterface();
		}
		m_lock = CoreComponentFactory.getLockInterface();

		MemoryManager.initialize(Core.getConfiguration().getLongValue(ConfigurationConstants.RAM_SIZE));

		if (!NodeID.isSuperpeer()) {
			m_migrationLock = new ReentrantLock(false);
			registerPeer();

			m_mappingLock = new ReentrantLock(false);
		}

		if (Core.getConfiguration().getBooleanValue(ConfigurationConstants.STATISTIC_CHUNK)) {
			StatisticsManager.registerStatistic("Chunk", ChunkStatistic.getInstance());
		}
	}

	@Override
	public void close() {
		try {
			MemoryManager.disengage();
		} catch (final MemoryException e) {}
	}

	@Override
	public Chunk create(final int p_size) throws DXRAMException {
		Chunk ret = null;
		final long lid;

		Operation.CREATE.enter();

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			lid = MemoryManager.getNextLocalID();
			ret = new Chunk(m_nodeID, lid, p_size);
			MemoryManager.put(ret);
			initBackupRange(lid, p_size);
		}

		Operation.CREATE.leave();

		return ret;
	}

	@Override
	public Chunk[] create(final int[] p_sizes) throws DXRAMException {
		Chunk[] ret = null;
		final long[] lids;

		Operation.MULTI_CREATE.enter();

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			lids = MemoryManager.getNextLocalIDs(p_sizes.length);
			if (lids != null) {
				ret = new Chunk[p_sizes.length];
				for (int i = 0; i < p_sizes.length; i++) {
					ret[i] = new Chunk(m_nodeID, lids[i], p_sizes[i]);
					MemoryManager.put(ret[i]);
					initBackupRange(lids[i], p_sizes[i]);
				}
			}
		}

		Operation.MULTI_CREATE.leave();

		return ret;
	}

	@Override
	public Chunk create(final int p_size, final int p_id) throws DXRAMException {
		Chunk ret = null;
		Chunk mapping;

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			ret = create(p_size);

			m_lookup.insertID(p_id, ret.getChunkID());

			m_mappingLock.lock();
			mapping = MemoryManager.get((long) m_nodeID << 48);
			if (null == mapping) {
				// Create chunk to store mappings
				mapping = new Chunk(m_nodeID, 0, INDEX_SIZE);
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

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			ret = create(p_sizes);

			m_lookup.insertID(p_id, ret[0].getChunkID());

			m_mappingLock.lock();
			mapping = MemoryManager.get((long) m_nodeID << 48);
			if (null == mapping) {
				// Create chunk to store mappings
				mapping = new Chunk(m_nodeID, 0, INDEX_SIZE);
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

		Operation.GET.enter();

		ChunkID.check(p_chunkID);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (MemoryManager.isResponsible(p_chunkID)) {
				// Local get
				ret = MemoryManager.get(p_chunkID);
			} else {
				while (null == ret) {
					primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();
					if (primaryPeer == m_nodeID) {
						// Local get
						ret = MemoryManager.get(p_chunkID);
					} else {
						// Remote get
						request = new GetRequest(primaryPeer, p_chunkID);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_chunkID);
							// TODO: Start Recovery
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

		Operation.MULTI_GET.enter();

		Contract.checkNotNull(p_chunkIDs, "no IDs given");
		ChunkID.check(p_chunkIDs);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			ret = new Chunk[p_chunkIDs.length];

			peers = new TreeMap<>();
			for (int i = 0; i < p_chunkIDs.length; i++) {
				if (MemoryManager.isResponsible(p_chunkIDs[i])) {
					// Local get
					ret[i] = MemoryManager.get(p_chunkIDs[i]);
				} else {
					peer = m_lookup.get(p_chunkIDs[i]).getPrimaryPeer();

					list = peers.get(peer);
					if (list == null) {
						list = new IntegerLongList();
						peers.put(peer, list);
					}
					list.add(i, p_chunkIDs[i]);
				}
			}

			for (final Entry<Short, IntegerLongList> entry : peers.entrySet()) {
				peer = entry.getKey();
				list = entry.getValue();

				if (peer == m_nodeID) {
					// Local get
					for (final KeyValuePair<Integer, Long> pair : list) {
						ret[pair.getKey()] = MemoryManager.get(pair.getValue());
					}
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
						// TODO: Start Recovery
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

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (MemoryManager.isResponsible(p_chunkID)) {
				// Local get
				fireIncomingChunk(new IncomingChunkEvent(m_nodeID, MemoryManager.get(p_chunkID)));
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

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (MemoryManager.isResponsible(p_chunk.getChunkID())) {
				// Local put
				MemoryManager.put(p_chunk);

				if (p_releaseLock) {
					m_lock.unlock(p_chunk.getChunkID(), m_nodeID);
				}

				if (LOG_ACTIVE) {
					// Send backups for logging (unreliable)
					backupPeers = getBackupPeers(p_chunk.getChunkID());
					if (backupPeers != null) {
						for (int i = 0; i < backupPeers.length; i++) {
							if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
								new LogMessage(backupPeers[i], p_chunk).send(m_network);
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
						MemoryManager.put(p_chunk);
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
								if (backupPeers[i] != m_nodeID) {
									new LogMessage(backupPeers[i], p_chunk).send(m_network);
								}
							}
						}
					}
				}
			}
		}

		Operation.PUT.leave();
	}

	@Override
	public void remove(final long p_chunkID) throws DXRAMException {
		Locations locations;
		short primaryPeer;
		short[] backupPeers;
		boolean success = false;
		RemoveRequest request;

		Operation.REMOVE.enter();

		ChunkID.check(p_chunkID);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (MemoryManager.isResponsible(p_chunkID)) {
				if (!MemoryManager.wasMigrated(p_chunkID)) {
					// Local remove
					MemoryManager.remove(p_chunkID);

					if (LOG_ACTIVE) {
						// Send backups for logging (unreliable)
						backupPeers = getBackupPeers(p_chunkID);
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
									new RemoveMessage(backupPeers[i], p_chunkID).send(m_network);
								}
							}
						}
					}
				} else {
					// TODO: Migrate back and remove
				}
			} else {
				while (!success) {
					locations = m_lookup.get(p_chunkID);
					primaryPeer = locations.getPrimaryPeer();
					backupPeers = locations.getBackupPeers();

					if (primaryPeer == m_nodeID) {
						// Local remove
						MemoryManager.remove(p_chunkID);
						success = true;
					} else {
						// Remote remove
						request = new RemoveRequest(primaryPeer, p_chunkID);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_chunkID);
							continue;
						}
						success = request.getResponse(RemoveResponse.class).getStatus();
					}
					if (success && LOG_ACTIVE) {
						// Send backups for logging (unreliable)
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != m_nodeID) {
									new RemoveMessage(backupPeers[i], p_chunkID).send(m_network);
								}
							}
						}
					}
				}
			}
			m_lookup.remove(p_chunkID);
		}

		Operation.REMOVE.leave();
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

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (MemoryManager.isResponsible(p_chunkID)) {
				// Local lock
				lock = new DefaultLock(p_chunkID, m_nodeID, p_readLock);
				m_lock.lock(lock);
				ret = lock.getChunk();
			} else {
				while (null == ret || -1 == ret.getChunkID()) {
					primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();
					if (primaryPeer == m_nodeID) {
						// Local lock
						lock = new DefaultLock(p_chunkID, m_nodeID, p_readLock);
						m_lock.lock(lock);
						ret = lock.getChunk();
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

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (MemoryManager.isResponsible(p_chunkID)) {
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
	public void migrate(final long p_chunkID, final short p_target) throws DXRAMException, NetworkException {
		short[] backupPeers;
		Chunk chunk;
		DataRequest request;

		ChunkID.check(p_chunkID);
		NodeID.check(p_target);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not store chunks");
		} else {
			m_migrationLock.lock();
			if (p_target != m_nodeID && MemoryManager.isResponsible(p_chunkID)) {
				chunk = MemoryManager.get(p_chunkID);

				if (chunk != null) {
					LOGGER.trace("Send request to " + p_target);

					// Send request instead of message to guarantee delivery
					request = new DataRequest(p_target, chunk);
					request.send(m_network);
					request.getResponse(DataResponse.class);

					// Update superpeers
					m_lookup.migrate(p_chunkID, p_target);
					// Remove all locks
					m_lock.unlockAll(p_chunkID);
					// Update local memory management
					MemoryManager.remove(p_chunkID);
					// Update logging
					backupPeers = getBackupPeers(p_chunkID);
					if (backupPeers != null) {
						for (int i = 0; i < backupPeers.length; i++) {
							if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
								new RemoveMessage(backupPeers[i], p_chunkID).send(m_network);
							}
						}
					}
				}
			}
			m_migrationLock.unlock();
		}
	}

	@Override
	public void migrateRange(final long p_startChunkID, final long p_endChunkID, final short p_target)
			throws DXRAMException {
		short[] backupPeers;
		long iter;
		Chunk chunk;
		DataRequest request;

		ChunkID.check(p_startChunkID);
		ChunkID.check(p_endChunkID);
		NodeID.check(p_target);

		// TODO: Handle range properly

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not store chunks");
		} else {
			m_migrationLock.lock();
			if (p_target != m_nodeID) {
				iter = p_startChunkID;
				while (iter <= p_endChunkID) {
					if (MemoryManager.isResponsible(iter)) {
						chunk = MemoryManager.get(iter);
						if (chunk != null) {
							request = new DataRequest(p_target, chunk);
							request.send(m_network);
							request.getResponse(DataResponse.class);
						}
					}
					iter++;
				}
				// Update superpeers
				m_lookup.migrateRange(p_startChunkID, p_endChunkID, p_target);

				iter = p_startChunkID;
				while (iter <= p_endChunkID) {
					// Remove all locks
					m_lock.unlockAll(iter);
					// Update local memory management
					MemoryManager.remove(iter);
					// Update logging
					backupPeers = getBackupPeers(iter);
					if (backupPeers != null) {
						for (int i = 0; i < backupPeers.length; i++) {
							if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
								new RemoveMessage(backupPeers[i], iter).send(m_network);
							}
						}
					}
					iter++;
				}
			}
			m_migrationLock.unlock();
		}
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
	public void migrateNotCreatedChunk(final long p_chunkID, final short p_target) throws DXRAMException {
		Chunk chunk;
		short creator;
		short target;
		short[] backupPeers;

		ChunkID.check(p_chunkID);
		NodeID.check(p_target);
		creator = ChunkID.getCreatorID(p_chunkID);

		m_migrationLock.lock();
		if (p_target != m_nodeID && MemoryManager.isResponsible(p_chunkID) && MemoryManager.wasMigrated(p_chunkID)) {
			chunk = MemoryManager.get(p_chunkID);
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
			new DataMessage(target, chunk).send(m_network);

			// Update superpeers
			m_lookup.migrateNotCreatedChunk(p_chunkID, target);
			// Remove all locks
			m_lock.unlockAll(p_chunkID);
			// Update local memory management
			MemoryManager.remove(p_chunkID);
			// Update logging
			backupPeers = getBackupPeers(p_chunkID);
			if (backupPeers != null) {
				for (int i = 0; i < backupPeers.length; i++) {
					if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
						new RemoveMessage(backupPeers[i], p_chunkID).send(m_network);
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
	public void migrateOwnChunk(final long p_chunkID, final short p_target) throws DXRAMException {
		short[] backupPeers;
		Chunk chunk;

		ChunkID.check(p_chunkID);
		NodeID.check(p_target);

		m_migrationLock.lock();
		if (p_target != m_nodeID) {
			chunk = MemoryManager.get(p_chunkID);
			if (chunk != null) {
				LOGGER.trace("Send request to " + p_target);

				// This is not safe, but there is no other possibility unless
				// the number of network threads is increased
				System.out.println("** Migrating own chunk " + p_chunkID + " to " + p_target);
				new DataMessage(p_target, chunk).send(m_network);

				// Update superpeers
				m_lookup.migrateOwnChunk(p_chunkID, p_target);
				// Remove all locks
				m_lock.unlockAll(p_chunkID);
				// Update local memory management
				MemoryManager.remove(p_chunkID);
				// Update logging
				backupPeers = getBackupPeers(p_chunkID);
				if (backupPeers != null) {
					for (int i = 0; i < backupPeers.length; i++) {
						if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
							new RemoveMessage(backupPeers[i], p_chunkID).send(m_network);
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

		localID = MemoryManager.getCurrentLocalID();

		// Migrate own chunks to p_target
		if (1 != localID) {
			for (int i = 1; i <= localID; i++) {
				chunkID = ((long) m_nodeID << 48) + i;
				if (MemoryManager.isResponsible(chunkID)) {
					migrateOwnChunk(chunkID, p_target);
				}
			}
		}

		iter = MemoryManager.getCIDOfAllMigratedChunks().iterator();
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
	private short[] getBackupPeers(final long p_chunkID) {
		short[] ret = null;

		if (ChunkID.getCreatorID(p_chunkID) == NodeID.getLocalNodeID()) {
			for (int i = m_ownBackupRanges.size() - 1; i >= 0; i--) {
				if (m_ownBackupRanges.get(i).getBackupPeers()[0] <= ChunkID.getLocalID(ChunkID.getLocalID(p_chunkID))) {
					ret = m_ownBackupRanges.get(i).getBackupPeers();
					break;
				}
			}
		} else {
			ret = m_migrationBackupRanges.get((int) m_migrationsTree.getBackupRange(p_chunkID)).getBackupPeers();
		}

		return ret;
	}

	/**
	 * Initializes the backup range for current locations
	 * and determines new backup peers if necessary
	 * @param p_lid
	 *            the current localID
	 * @param p_size
	 *            the size of the new created chunk
	 * @throws LookupException
	 *             if range could not be initialized
	 */
	private void initBackupRange(final long p_lid, final long p_size) throws LookupException {
		if (LOG_ACTIVE) {
			m_rangeSize += p_size + m_log.getHeaderSize();
			if (1 == p_lid || m_rangeSize > SECONDARY_LOG_SIZE / 2) {
				determineBackupPeers(p_lid);
				m_lookup.initRange(((long) m_nodeID << 48) + p_lid,
						new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_log.initBackupRange(((long) m_nodeID << 48) + p_lid, m_currentBackupRange.getBackupPeers());
				m_rangeSize = 0;
			}
		} else if (1 == p_lid) {
			m_lookup.initRange(((long) m_nodeID << 48) + 0xFFFFFFFFFFFFL,
					new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
		}
	}

	/**
	 * Determines backup peers
	 * @param p_lid
	 *            the current localID
	 */
	private void determineBackupPeers(final long p_lid) {
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
			LOGGER.warn("Less than six peers for backup available. Some peers may store more"
					+ " than one backup range of a node!");

			oldBackupPeers = new short[REPLICATION_FACTOR];
			Arrays.fill(oldBackupPeers, (short) -1);

			newBackupPeers = new short[REPLICATION_FACTOR];
			Arrays.fill(newBackupPeers, (short) -1);
		} else if (null != m_currentBackupRange.getBackupPeers()) {
			oldBackupPeers = new short[REPLICATION_FACTOR];
			for (int i = 0; i < REPLICATION_FACTOR; i++) {
				if (p_lid > -1) {
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
				if (p_lid > -1) {
					m_currentBackupRange = new BackupRange(p_lid, newBackupPeers);
				} else {
					m_currentMigrationBackupRange =
							new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, newBackupPeers);
				}
			}
		} else {
			// Determine backup peers
			for (int i = 0; i < 3; i++) {
				while (!ready) {
					index = (short) (Math.random() * numberOfPeers);
					ready = true;
					for (int j = 0; j < i; j++) {
						if (allPeers[index] == oldBackupPeers[j]
								|| allPeers[index] == newBackupPeers[j]) {
							ready = false;
							break;
						}
					}
				}
				System.out.println(i + 1 + ". backup peer: " + allPeers[index]);
				newBackupPeers[i] = allPeers[index];
				ready = false;
			}
			if (p_lid > -1) {
				m_currentBackupRange = new BackupRange(p_lid, newBackupPeers);
			} else {
				m_currentMigrationBackupRange =
						new BackupRange(m_currentMigrationBackupRange.getRangeID() + 1, newBackupPeers);
			}
		}

		if (p_lid > -1) {
			m_ownBackupRanges.add(m_currentBackupRange);
		} else {
			m_migrationBackupRanges.add(m_currentMigrationBackupRange);
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
					Core.put(indexChunk);
				} else {
					// The last index chunk is full -> create new chunk and add its address to the old one
					appendix = create(INDEX_SIZE);
					appendixData = appendix.getData();
					appendixData.putInt(4 + 12);
					appendixData.putInt(p_key);
					appendixData.putLong(p_chunkID);
					Core.put(appendix);

					indexData.position(indexData.capacity() - 12);
					indexData.putInt(-1);
					indexData.putLong(appendix.getChunkID());
					Core.put(indexChunk);
				}
				break;
			}
			// Get next index file and repeat
			indexChunk = Core.get(indexData.getLong(indexData.capacity() - 8));
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
	private void
	deleteEntryInLastIndexFile(final Chunk p_indexChunk, final Chunk p_predecessorChunk, final int p_index)
			throws DXRAMException {
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

	@Override
	public void recoverFromLog() throws DXRAMException {
		// TODO: recoverFromLog
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
			chunk = MemoryManager.get(p_request.getChunkID());
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
				chunks[i] = MemoryManager.get(chunkIDs[i]);
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
			if (MemoryManager.isResponsible(chunk.getChunkID())) {
				MemoryManager.put(chunk);
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
		long chunkID;

		Operation.INCOMING_REMOVE.enter();

		chunkID = p_request.getChunkID();
		try {
			m_migrationLock.lock();
			if (MemoryManager.isResponsible(chunkID)) {
				MemoryManager.remove(chunkID);
				success = true;
			}
			m_migrationLock.unlock();
			new RemoveResponse(p_request, success).send(m_network);
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
			new LockResponse(p_request, lock.getChunk()).send(m_network);
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle message", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}

		Operation.INCOMING_LOCK.leave();
	}

	/**
	 * Handles an incoming UnlockMessage
	 * @param p_request
	 *            the UnlockMessage
	 */
	private void incomingUnlockMessage(final UnlockMessage p_request) {
		Operation.INCOMING_UNLOCK.enter();

		try {
			m_lock.unlock(p_request.getChunkID(), p_request.getSource());
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle message", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}

		Operation.INCOMING_UNLOCK.leave();
	}

	/**
	 * Handles an incoming DataRequest
	 * @param p_request
	 *            the DataRequest
	 */
	private void incomingDataRequest(final DataRequest p_request) {
		long size;
		Chunk chunk;
		short[] backupPeers;

		chunk = p_request.getChunk();
		size = chunk.getSize() + m_log.getHeaderSize();

		try {
			MemoryManager.put(chunk);
			if (!m_migrationsTree.fits(size)) {
				determineBackupPeers(-1);

				m_lookup.initRange(((long) -1 << 48) + m_currentBackupRange.getRangeID(),
						new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_log.initBackupRange(((long) -1 << 48) + m_currentBackupRange.getRangeID(),
						m_currentBackupRange.getBackupPeers());
			}
			m_migrationsTree.putObject(chunk.getChunkID(), (int) m_currentMigrationBackupRange.getRangeID(), size);

			backupPeers = m_currentMigrationBackupRange.getBackupPeers();
			if (backupPeers != null) {
				for (int i = 0; i < backupPeers.length; i++) {
					if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
						new LogMessage(backupPeers[i], chunk, (int) m_currentMigrationBackupRange.getRangeID())
						.send(m_network);
					}
				}
			}

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
		long size;
		Chunk chunk;
		short[] backupPeers;

		chunk = p_message.getChunk();
		size = chunk.getSize() + m_log.getHeaderSize();

		try {
			MemoryManager.put(chunk);
			if (!m_migrationsTree.fits(size)) {
				determineBackupPeers(-1);

				m_lookup.initRange(((long) -1 << 48) + m_currentBackupRange.getRangeID(),
						new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_log.initBackupRange(((long) -1 << 48) + m_currentBackupRange.getRangeID(),
						m_currentBackupRange.getBackupPeers());
			}
			m_migrationsTree.putObject(chunk.getChunkID(), (int) m_currentMigrationBackupRange.getRangeID(), size);

			backupPeers = m_currentMigrationBackupRange.getBackupPeers();
			if (backupPeers != null) {
				for (int i = 0; i < backupPeers.length; i++) {
					if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
						new LogMessage(backupPeers[i], chunk, (int) m_currentMigrationBackupRange.getRangeID())
						.send(m_network);
					}
				}
			}
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle request", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_message);
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
		public GetRequestAction() {}

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
		private long m_rangeID;
		private short[] m_backupPeers;

		// Constructors
		/**
		 * Creates an instance of Locations
		 * @param p_rangeID
		 *            the RangeID or the first ChunkID
		 * @param p_backupPeers
		 *            the backup peers
		 */
		public BackupRange(final long p_rangeID, final short[] p_backupPeers) {
			super();

			m_rangeID = p_rangeID;
			m_backupPeers = p_backupPeers;
		}

		/**
		 * Creates an instance of Locations
		 * @param p_rangeID
		 *            the RangeID or the first ChunkID
		 * @param p_backupPeers
		 *            the locations in long representation
		 */
		public BackupRange(final long p_rangeID, final long p_backupPeers) {
			this(p_rangeID, new short[] {
					(short) ((p_backupPeers & 0x00000000FFFF0000L) >> 16),
					(short) ((p_backupPeers & 0x0000FFFF00000000L) >> 32),
					(short) ((p_backupPeers & 0xFFFF000000000000L) >> 48)});
		}

		// Getter
		/**
		 * Returns RangeID or first ChunkID
		 * @return RangeID or first ChunkID
		 */
		public long getRangeID() {
			return m_rangeID;
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
					ret = ((m_backupPeers[2] & 0x000000000000FFFFL) << 32)
							+ ((m_backupPeers[1] & 0x000000000000FFFFL) << 16)
							+ (m_backupPeers[0] & 0x000000000000FFFFL);
				} else if (m_backupPeers.length == 2) {
					ret = ((-1 & 0x000000000000FFFFL) << 32)
							+ ((m_backupPeers[1] & 0x000000000000FFFFL) << 16)
							+ (m_backupPeers[0] & 0x000000000000FFFFL);
				} else {
					ret = ((-1 & 0x000000000000FFFFL) << 32)
							+ ((-1 & 0x000000000000FFFFL) << 16)
							+ (m_backupPeers[0] & 0x000000000000FFFFL);
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

			ret = "" + m_rangeID;
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

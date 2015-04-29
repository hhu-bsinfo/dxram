
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
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.LogMessage;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.LogRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.MultiGetRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.MultiGetResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.PutRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.PutResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.RemoveRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.RemoveResponse;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.UnlockRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.UnlockResponse;
import de.uniduesseldorf.dxram.core.chunk.storage.MemoryManager;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.events.IncomingChunkListener;
import de.uniduesseldorf.dxram.core.events.IncomingChunkListener.IncomingChunkEvent;
import de.uniduesseldorf.dxram.core.exceptions.ChunkException;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler.ExceptionSource;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.core.exceptions.NetworkException;
import de.uniduesseldorf.dxram.core.lock.LockInterface;
import de.uniduesseldorf.dxram.core.lock.LockInterface.AbstractLock;
import de.uniduesseldorf.dxram.core.log.LogInterface;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;
import de.uniduesseldorf.dxram.core.lookup.LookupInterface;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.AbstractRequest;
import de.uniduesseldorf.dxram.core.net.NetworkInterface;
import de.uniduesseldorf.dxram.core.net.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.dxram.utils.AbstractAction;
import de.uniduesseldorf.dxram.utils.Contract;
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

	private static final int RANGE_SIZE = Core.getConfiguration().getIntValue(ConfigurationConstants.LOOKUP_INIT_RANGE);
	private static final int INDEX_SIZE = 12016;

	// Attributes
	private short m_nodeID;
	private Locations m_locations;

	private NetworkInterface m_network;
	private LookupInterface m_lookup;
	private LogInterface m_log;
	private LockInterface m_lock;

	private IncomingChunkListener m_listener;

	private ArrayList<Long> m_migrations;
	private Lock m_migrationLock;

	private Lock m_mappingLock;

	// Constructors
	/**
	 * Creates an instance of DataHandler
	 */
	public ChunkHandler() {
		m_nodeID = NodeID.INVALID_ID;
		m_locations = null;

		m_network = null;
		m_lookup = null;
		m_log = null;
		m_lock = null;

		m_listener = null;

		m_migrations = null;
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
		LOGGER.trace("Entering initialize");

		m_nodeID = NodeID.getLocalNodeID();

		m_network = CoreComponentFactory.getNetworkInterface();
		m_network.register(GetRequest.class, this);
		m_network.register(PutRequest.class, this);
		m_network.register(RemoveRequest.class, this);
		m_network.register(LockRequest.class, this);
		m_network.register(UnlockRequest.class, this);
		m_network.register(LogRequest.class, this);
		m_network.register(LogMessage.class, this);
		m_network.register(DataRequest.class, this);
		m_network.register(DataMessage.class, this);
		m_network.register(MultiGetRequest.class, this);

		m_lookup = CoreComponentFactory.getLookupInterface();
		m_lookup.initChunkHandler();
		m_log = CoreComponentFactory.getLogInterface();
		m_lock = CoreComponentFactory.getLockInterface();

		MemoryManager.initialize(Core.getConfiguration().getLongValue(ConfigurationConstants.RAM_SIZE));

		if (!NodeID.isSuperpeer()) {
			m_migrations = new ArrayList<Long>();
			m_migrationLock = new ReentrantLock(false);
			m_locations = new Locations(m_nodeID, null, null);
			registerPeer();

			m_mappingLock = new ReentrantLock(false);
		}

		LOGGER.trace("Exiting initialize");
	}

	@Override
	public void close() {
		LOGGER.trace("Entering close");

		try {
			MemoryManager.disengage();
		} catch (final MemoryException e) {}

		LOGGER.trace("Exiting close");
	}

	/**
	 * Checks if the node is responsible for the Chunk
	 * @param p_chunkID
	 *            the ChunkID of the Chunk
	 * @return true if the node is responsible, false otherwise
	 */
	private boolean isResponsible(final long p_chunkID) {
		boolean ret;

		ret = ChunkID.getCreatorID(p_chunkID) == m_nodeID;
		if (!ret) {
			// This could be a bottleneck
			ret = m_migrations.contains(p_chunkID);
		}

		return ret;
	}

	@Override
	public Chunk create(final int p_size) throws DXRAMException {
		Chunk ret = null;
		final long lid;

		LOGGER.trace("Entering create with: p_size=" + p_size);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			lid = MemoryManager.getNextLocalID();
			ret = new Chunk(m_nodeID, lid, p_size);
			MemoryManager.put(ret);
			increaseObjectCounter(lid);
		}

		LOGGER.trace("Exiting create with: ret=" + ret);

		return ret;
	}

	@Override
	public Chunk[] create(final int[] p_sizes) throws DXRAMException {
		Chunk[] ret = null;
		final long[] lids;

		Contract.checkNotNull(p_sizes, "no sizes given");

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not create chunks");
		} else {
			lids = MemoryManager.getNextLocalIDs(p_sizes.length);
			if (lids != null) {
				ret = new Chunk[p_sizes.length];
				for (int i = 0; i < p_sizes.length; i++) {
					ret[i] = new Chunk(m_nodeID, lids[i], p_sizes[i]);
					MemoryManager.put(ret[i]);
					increaseObjectCounter(lids[i]);
				}
			}
		}

		return ret;
	}

	@Override
	public Chunk create(final int p_size, final int p_id) throws DXRAMException {
		Chunk ret = null;
		Chunk mapping;

		LOGGER.trace("Entering create with: p_size=" + p_size + ", p_id=" + p_id);

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
		LOGGER.trace("Exiting create with: ret=" + ret);

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

		LOGGER.trace("Entering get with: p_chunkID=" + p_chunkID);

		ChunkID.check(p_chunkID);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (isResponsible(p_chunkID)) {
				// Local get
				ret = MemoryManager.get(p_chunkID);
			} else {
				while (null == ret) {
					primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();
					if (primaryPeer == m_nodeID) {
						// Local get
						ret = MemoryManager.get(p_chunkID);
					} else {
						LOGGER.trace("Send request to " + primaryPeer);
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

		LOGGER.trace("Exiting get with: ret=" + ret);

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

		LOGGER.trace("Entering get with: p_chunkID=" + Arrays.toString(p_chunkIDs));

		Contract.checkNotNull(p_chunkIDs, "no IDs given");
		ChunkID.check(p_chunkIDs);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			ret = new Chunk[p_chunkIDs.length];

			peers = new TreeMap<>();
			for (int i = 0; i < p_chunkIDs.length; i++) {
				if (isResponsible(p_chunkIDs[i])) {
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
					LOGGER.trace("Send request to " + peer);

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

		LOGGER.trace("Exiting get with: ret=" + Arrays.toString(ret));

		return ret;
	}

	@Override
	public Chunk get(final int p_id) throws DXRAMException {
		return get(m_lookup.getChunkID(p_id));
	}

	@Override
	public long getChunkID(final int p_id) throws DXRAMException {
		return m_lookup.getChunkID(p_id);
	}

	@Override
	public void getAsync(final long p_chunkID) throws DXRAMException {
		short primaryPeer;
		GetRequest request;

		LOGGER.trace("Entering getAsync with: p_chunkID=" + p_chunkID);

		ChunkID.check(p_chunkID);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (isResponsible(p_chunkID)) {
				// Local get
				fireIncomingChunk(new IncomingChunkEvent(m_nodeID, MemoryManager.get(p_chunkID)));
			} else {
				primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();

				if (primaryPeer != m_nodeID) {
					LOGGER.trace("Send request to " + primaryPeer);

					// Remote get
					request = new GetRequest(primaryPeer, p_chunkID);
					request.registerFulfillAction(new GetRequestAction());
					request.send(m_network);
				}
			}
		}

		LOGGER.trace("Exiting getAsync");
	}

	@Override
	public void put(final Chunk p_chunk) throws DXRAMException {
		Locations locations;
		short primaryPeer;
		short[] backupPeers;
		boolean success = false;

		PutRequest request;

		LOGGER.trace("Entering put with: p_chunk=" + p_chunk);

		Contract.checkNotNull(p_chunk, "no chunk given");

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (isResponsible(p_chunk.getChunkID())) {
				// Local put
				MemoryManager.put(p_chunk);

				// Send backups for logging (unreliable)
				backupPeers = m_locations.getBackupPeers();
				for (int i = 0; i < 3; i++) {
					if (backupPeers[i] != m_nodeID) {
						new LogMessage(backupPeers[i], p_chunk).send(m_network);
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
						success = true;
					} else {
						LOGGER.trace("Send message to " + primaryPeer);

						// Remote put
						request = new PutRequest(primaryPeer, p_chunk);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_chunk.getChunkID());
							continue;
						}
						success = request.getResponse(PutResponse.class).getStatus();
					}
					if (success) {
						// Send backups for logging (unreliable)
						for (int i = 0; i < 3; i++) {
							if (backupPeers[i] != m_nodeID) {
								new LogMessage(backupPeers[i], p_chunk).send(m_network);
							}
						}
					}
				}
			}
		}

		LOGGER.trace("Exiting put");
	}

	@Override
	public void remove(final long p_chunkID) throws DXRAMException {
		short primaryPeer;
		boolean success = false;

		RemoveRequest request;

		LOGGER.trace("Entering remove with: p_chunkID=" + p_chunkID);

		ChunkID.check(p_chunkID);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (isResponsible(p_chunkID)) {
				// Local remove
				MemoryManager.remove(p_chunkID);
			} else {
				while (!success) {
					primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();
					if (primaryPeer == m_nodeID) {
						// Local remove
						MemoryManager.remove(p_chunkID);
						success = true;
					} else {
						LOGGER.trace("Send message to " + primaryPeer);

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
					// TODO
					/*
					 * if (success) {
					 * // Send backups for logging (unreliable)
					 * for (int i = 0; i < 3; i++) {
					 * if (backupPeers[i] != m_nodeID) {
					 * new LogMessage(backupPeers[i], null).send(m_network);
					 * }
					 * }
					 * }
					 */
				}
			}
			m_lookup.remove(p_chunkID);
		}

		LOGGER.trace("Exiting remove");
	}

	@Override
	public Chunk lock(final long p_chunkID) throws DXRAMException {
		Chunk ret = null;
		LocalLock lock;
		short primaryPeer;
		LockRequest request;
		LockResponse response;

		LOGGER.trace("Entering lock with: p_chunkID=" + p_chunkID);

		ChunkID.check(p_chunkID);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (isResponsible(p_chunkID)) {
				// Local lock
				lock = new LocalLock(p_chunkID, m_nodeID);
				m_lock.lock(lock);
				ret = lock.m_chunk;
			} else {
				while (null == ret || -1 == ret.getChunkID()) {
					primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();
					if (primaryPeer == m_nodeID) {
						// Local lock
						lock = new LocalLock(p_chunkID, m_nodeID);
						m_lock.lock(lock);
						ret = lock.m_chunk;
					} else {
						LOGGER.trace("Send request to " + primaryPeer);
						// Remote lock
						request = new LockRequest(primaryPeer, p_chunkID);
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
								Thread.sleep(100);
							} catch (final InterruptedException e) {}
						}
					}
				}
			}
		}

		LOGGER.trace("Exiting lock with: ret=" + ret);

		return ret;
	}

	@Override
	public void unlock(final long p_chunkID) throws DXRAMException {
		short primaryPeer;
		UnlockRequest request;

		LOGGER.trace("Entering unlock with: p_chunkID=" + p_chunkID);

		ChunkID.check(p_chunkID);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			if (isResponsible(p_chunkID)) {
				// Local lock
				m_lock.release(p_chunkID, m_nodeID);
			} else {
				while (true) {
					primaryPeer = m_lookup.get(p_chunkID).getPrimaryPeer();
					if (primaryPeer == m_nodeID) {
						// Local release
						m_lock.release(p_chunkID, m_nodeID);
						break;
					} else {
						LOGGER.trace("Send message to " + primaryPeer);
						try {
							// Remote release
							request = new UnlockRequest(primaryPeer, p_chunkID);
							request.sendSync(m_network);
							request.getResponse(UnlockResponse.class);
						} catch (final NetworkException e) {
							m_lookup.invalidate(p_chunkID);
							continue;
						}
						break;
					}
				}
			}
		}

		LOGGER.trace("Exiting unlock");
	}

	@Override
	public void migrate(final long p_chunkID, final short p_target) throws DXRAMException, NetworkException {
		Chunk chunk;
		DataRequest request;

		LOGGER.trace("Entering migrate with: p_chunkID=" + p_chunkID + ", p_target=" + p_target);

		ChunkID.check(p_chunkID);
		NodeID.check(p_target);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not store chunks");
		} else {
			m_migrationLock.lock();
			if (p_target != m_nodeID && isResponsible(p_chunkID)) {
				chunk = MemoryManager.get(p_chunkID);
				if (chunk != null) {
					LOGGER.trace("Send request to " + p_target);

					// Send request instead of message to guarantee delivery
					request = new DataRequest(p_target, chunk);
					request.send(m_network);
					request.getResponse(DataResponse.class);

					m_lookup.migrate(p_chunkID, p_target);
					m_lock.releaseAllByChunkID(p_chunkID);
					MemoryManager.remove(p_chunkID);
					m_migrations.remove(p_chunkID);
				}
			}
			m_migrationLock.unlock();
		}

		LOGGER.trace("Exiting migrate");
	}

	@Override
	public void migrateRange(final long p_startChunkID, final long p_endChunkID, final short p_target)
			throws DXRAMException {
		long iter;
		Chunk chunk;
		// DataRequest request;

		LOGGER.trace("Entering migrateRange with: p_startChunkID=" + p_startChunkID + ", p_endChunkID=" + p_endChunkID
				+ ", p_target=" + p_target);

		ChunkID.check(p_startChunkID);
		ChunkID.check(p_endChunkID);
		NodeID.check(p_target);

		if (NodeID.isSuperpeer()) {
			LOGGER.error("a superpeer must not store chunks");
		} else {
			m_migrationLock.lock();
			if (p_target != m_nodeID) {
				iter = p_startChunkID;
				while (iter <= p_endChunkID) {
					if (isResponsible(iter)) {
						chunk = MemoryManager.get(iter);
						if (chunk != null) {
							// TODO: Send request instead of message to guarantee delivery
							// request = new DataRequest(p_target, chunk);
							// request.send(m_network);
							// request.getResponse(DataResponse.class);
						}
					}
					iter++;
				}
				m_lookup.migrateRange(p_startChunkID, p_endChunkID, p_target);

				iter = p_startChunkID;
				while (iter <= p_endChunkID) {
					m_lock.releaseAllByChunkID(iter);
					MemoryManager.remove(iter);
					m_migrations.remove(iter++);
				}
			}
			m_migrationLock.unlock();
		}

		LOGGER.trace("Exiting migrateRange");
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

		LOGGER.trace("Entering migrateNotCreatedChunk with: p_chunkID=" + p_chunkID + ", p_target=" + p_target);

		ChunkID.check(p_chunkID);
		NodeID.check(p_target);
		creator = ChunkID.getCreatorID(p_chunkID);

		m_migrationLock.lock();
		if (p_target != m_nodeID) {
			chunk = MemoryManager.get(p_chunkID);
			if (chunk != null) {
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

				m_lookup.migrateNotCreatedChunk(p_chunkID, target);
				m_lock.releaseAllByChunkID(p_chunkID);
				MemoryManager.remove(p_chunkID);
			}
		}
		m_migrationLock.unlock();

		LOGGER.trace("Exiting migrateNotCreatedChunk");
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
		Chunk chunk;

		LOGGER.trace("Entering migrateOwnChunk with: p_chunkID=" + p_chunkID + ", p_target=" + p_target);

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

				m_lookup.migrateOwnChunk(p_chunkID, p_target);
				m_lock.releaseAllByChunkID(p_chunkID);
				MemoryManager.remove(p_chunkID);
			}
		}
		m_migrationLock.unlock();

		LOGGER.trace("Exiting migrateOwnChunk");
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
				if (isResponsible(chunkID)) {
					migrateOwnChunk(chunkID, p_target);
				}
			}
		}

		// Migrate previously migrated chunks
		iter = m_migrations.iterator();
		while (iter.hasNext()) {
			chunkID = iter.next();
			migrateNotCreatedChunk(chunkID, p_target);
		}
		m_migrations.clear();
	}

	/**
	 * Registers peer in superpeer overlay
	 * @throws LookupException
	 *             if range could not be initialized
	 */
	private void registerPeer() throws LookupException {
		m_lookup.initRange(0, m_locations);
	}

	/**
	 * Increases object counter for current locations
	 * @param p_lid
	 *            the current localID
	 * @throws LookupException
	 *             if range could not be initialized
	 */
	private void increaseObjectCounter(final long p_lid) throws LookupException {
		if (0 == p_lid % RANGE_SIZE) {
			determineBackupPeers();
			m_lookup.initRange(((long) m_nodeID << 48) + p_lid + RANGE_SIZE - 1, m_locations);
		} else if (1 == p_lid) {
			determineBackupPeers();
			m_lookup.initRange(((long) m_nodeID << 48) + RANGE_SIZE - 1, m_locations);
		}
	}

	/**
	 * Determines backup peers
	 */
	private void determineBackupPeers() {
		boolean ready = false;
		boolean error = false;
		short index = 0;
		short peer;
		short[] backupPeers;
		short[] allPeers;
		short numberOfPeers = 0;
		List<String> peers = null;

		backupPeers = new short[6];

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
			LOGGER.error("not enough peers online to backup");
			error = true;
		} else if (null == m_locations.getBackupPeers()) {
			backupPeers[0] = (short) -1;
			backupPeers[1] = (short) -1;
			backupPeers[2] = (short) -1;
		} else if (6 > numberOfPeers) {
			LOGGER.warn("less than six peers online. Within the new backup peers will be old ones");
			backupPeers[0] = (short) -1;
			backupPeers[1] = (short) -1;
			backupPeers[2] = (short) -1;
		} else {
			backupPeers[0] = m_locations.getBackupPeers()[0];
			backupPeers[1] = m_locations.getBackupPeers()[1];
			backupPeers[2] = m_locations.getBackupPeers()[2];
		}
		if (!error) {
			// Determine backup peers
			for (int i = 3; i < 6; i++) {
				while (!ready) {
					index = (short) (Math.random() * numberOfPeers);
					ready = true;
					for (int j = 0; j < i; j++) {
						if (allPeers[index] == backupPeers[j]) {
							ready = false;
							break;
						}
					}
				}
				System.out.println(i + ". backup peer: " + allPeers[index]);
				backupPeers[i] = allPeers[index];
				ready = false;
			}
			m_locations.setBackupPeers(Arrays.copyOfRange(backupPeers, 3, 6));
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
		LOGGER.trace("Entering recoverFromLog");

		// TODO: recoverFromLog

		LOGGER.trace("Exiting recoverFromLog");
	}

	/**
	 * Handles an incoming GetRequest
	 * @param p_request
	 *            the GetRequest
	 */
	private void incomingGetRequest(final GetRequest p_request) {
		Chunk chunk;
		try {
			// System.out.println("GetRequest: " + Long.toHexString(p_request.getChunkID()));
			chunk = MemoryManager.get(p_request.getChunkID());
			if (null != chunk) {
				new GetResponse(p_request, chunk).send(m_network);
			} else {
				System.out.println("null");
			}
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle request", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}
	}

	/**
	 * Handles an incoming PutRequest
	 * @param p_request
	 *            the PutRequest
	 */
	private void incomingPutRequest(final PutRequest p_request) {
		boolean success = false;
		Chunk chunk;

		chunk = p_request.getChunk();

		try {
			m_migrationLock.lock();
			if (isResponsible(chunk.getChunkID())) {
				MemoryManager.put(chunk);
				success = true;
			}
			m_migrationLock.unlock();
			new PutResponse(p_request, success).send(m_network);
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle message", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}
	}

	/**
	 * Handles an incoming RemoveRequest
	 * @param p_request
	 *            the RemoveRequest
	 */
	private void incomingRemoveRequest(final RemoveRequest p_request) {
		boolean success = false;
		long chunkID;

		chunkID = p_request.getChunkID();
		try {
			m_migrationLock.lock();
			if (isResponsible(chunkID)) {
				MemoryManager.remove(chunkID);
				success = true;
			}
			m_migrationLock.unlock();
			new RemoveResponse(p_request, success).send(m_network);
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle message", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}
	}

	/**
	 * Handles an incoming LockRequest
	 * @param p_request
	 *            the LockRequest
	 */
	private void incomingLockRequest(final LockRequest p_request) {
		long chunkID;
		Chunk chunk = null;
		LocalLock lock = null;

		chunkID = p_request.getChunkID();

		try {
			if (!m_lock.isLocked(chunkID)) {
				chunk = MemoryManager.get(chunkID);
				if (null != chunk) {
					// Local lock
					lock = new LocalLock(chunkID, p_request.getSource());
					m_lock.lock(lock);
					new LockResponse(p_request, chunk).send(m_network);
				}
			} else {
				chunk = new Chunk(-1, 1);
				new LockResponse(p_request, chunk).send(m_network);
			}
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle request", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}
	}

	/**
	 * Handles an incoming UnlockRequest
	 * @param p_request
	 *            the UnlockRequest
	 */
	private void incomingUnlockRequest(final UnlockRequest p_request) {
		try {
			// Local release
			m_lock.release(p_request.getChunkID(), p_request.getSource());

			new UnlockResponse(p_request).send(m_network);
		} catch (final ChunkException | NetworkException e) {
			LOGGER.error("ERR::Could not handle request", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}
	}

	/**
	 * Handles an incoming LogRequest
	 * @param p_request
	 *            the LogRequest
	 */
	private void incomingLogRequest(final LogRequest p_request) {
		// TODO
	}

	/**
	 * Handles an incoming LogMessage
	 * @param p_message
	 *            the LogMessage
	 */
	private void incomingLogMessage(final LogMessage p_message) {

		try {
			m_log.logChunk(p_message.getChunk());
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle request", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_message);
		}
	}

	/**
	 * Handles an incoming DataRequest
	 * @param p_request
	 *            the DataRequest
	 */
	private void incomingDataRequest(final DataRequest p_request) {
		Chunk chunk;
		long chunkID;

		chunk = p_request.getChunk();
		chunkID = chunk.getChunkID();

		try {
			if (m_nodeID != ChunkID.getCreatorID(chunkID)) {
				m_migrations.add(m_migrations.size(), chunkID);
			}
			MemoryManager.put(chunk);
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
		Chunk chunk;
		long chunkID;

		chunk = p_message.getChunk();
		chunkID = chunk.getChunkID();
		System.out.println("--------------------------- " + chunkID + " ------------------------");

		try {
			if (m_nodeID != ChunkID.getCreatorID(chunkID)) {
				m_migrations.add(m_migrations.size(), chunkID);
			}
			MemoryManager.put(chunk);
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle request", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_message);
		}
	}

	/**
	 * Handles an incoming MultiGetRequest
	 * @param p_request
	 *            the MultiGetRequest
	 */
	private void incomingMultiGetRequest(final MultiGetRequest p_request) {
		long[] chunkIDs;
		Chunk[] chunks;

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
				case ChunkMessages.SUBTYPE_UNLOCK_REQUEST:
					incomingUnlockRequest((UnlockRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_LOG_REQUEST:
					incomingLogRequest((LogRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_LOG_MESSAGE:
					incomingLogMessage((LogMessage) p_message);
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
		LOGGER.trace("Entering trigger with: p_event=" + p_event);

		Contract.checkNotNull(p_event, "no nodeID given");

		try {
			m_lock.releaseAllByNodeID(p_event.getSource());
		} catch (final ChunkException e) {
			LOGGER.error("ERR::Could not handle lost connection", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_event);
		}

		LOGGER.trace("Exiting onConnectionLost");
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
	 * Represents a Lock from the local node
	 * @author Florian Klein 09.03.2012
	 */
	private class LocalLock extends AbstractLock {

		// Attributes
		private Chunk m_chunk;

		private boolean m_released;

		// Constructors
		/**
		 * Creates an instance of LocalLock
		 * @param p_chunkID
		 *            the ID of the Chunk to lock
		 * @param p_nodeID
		 *            the ID of the node that requested this lock
		 */
		public LocalLock(final long p_chunkID, final short p_nodeID) {
			super(p_chunkID, p_nodeID);

			m_released = false;
		}

		// Methods
		/**
		 * Lock has been enqueued
		 */
		@Override
		public synchronized void enqueued() {
			while (!m_released) {
				try {
					wait();
				} catch (final InterruptedException e) {}
			}
		}

		/**
		 * Lock has been released
		 * @param p_chunk
		 *            the corresponding Chunk of the Lock
		 */
		@Override
		public synchronized void release(final Chunk p_chunk) {
			m_chunk = p_chunk;

			m_released = true;

			notify();
		}

	}

	// /**
	// * Represents a Lock from a remote node
	// * @author Florian Klein 09.03.2012
	// */
	// private class RemoteLock extends AbstractLock {
	//
	// // Attributes
	// private LockRequest m_request;
	//
	// // Constructors
	// /**
	// * Creates an instance of RemoteLock
	// * @param p_request
	// * the corresponding LcokRequest
	// */
	// public RemoteLock(final LockRequest p_request) {
	// super(p_request.getChunkID(), p_request.getSource());
	//
	// m_request = p_request;
	// }
	//
	// // Methods
	// /**
	// * Lock has been released
	// * @param p_chunk
	// * the corresponding Chunk of the Lock
	// */
	// @Override
	// public void release(final Chunk p_chunk) {
	// try {
	// new LockResponse(m_request, p_chunk).send(m_network);
	// } catch (final DXRAMException e) {
	// LOGGER.error("ERR::Could not handle request", e);
	//
	// Core.handleException(e, ExceptionSource.DATA_INTERFACE, m_request);
	// }
	// }
	//
	// }

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

}

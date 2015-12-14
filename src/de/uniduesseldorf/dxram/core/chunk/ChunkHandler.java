
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
import de.uniduesseldorf.dxram.core.dxram.Core;
import de.uniduesseldorf.dxram.core.dxram.nodeconfig.NodeID;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.events.IncomingChunkListener;
import de.uniduesseldorf.dxram.core.events.IncomingChunkListener.IncomingChunkEvent;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler.ExceptionSource;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.core.lock.DefaultLock;
import de.uniduesseldorf.dxram.core.lock.LockInterface;
import de.uniduesseldorf.dxram.core.log.LogInterface;
import de.uniduesseldorf.dxram.core.log.LogMessages.LogMessage;
import de.uniduesseldorf.dxram.core.log.LogMessages.RemoveMessage;
import de.uniduesseldorf.dxram.core.lookup.LookupHandler.Locations;
import de.uniduesseldorf.dxram.core.mem.Chunk;
import de.uniduesseldorf.dxram.core.mem.MigrationsTree;
import de.uniduesseldorf.dxram.core.mem.storage.MemoryManagerComponent;
import de.uniduesseldorf.dxram.core.lookup.LookupException;
import de.uniduesseldorf.dxram.core.lookup.LookupInterface;
import de.uniduesseldorf.dxram.core.util.ChunkID;

import de.uniduesseldorf.menet.AbstractAction;
import de.uniduesseldorf.menet.AbstractMessage;
import de.uniduesseldorf.menet.AbstractRequest;
import de.uniduesseldorf.menet.NetworkException;
import de.uniduesseldorf.menet.NetworkInterface;
import de.uniduesseldorf.menet.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.utils.Contract;
import de.uniduesseldorf.utils.Pair;
import de.uniduesseldorf.utils.StatisticsManager;
import de.uniduesseldorf.utils.ZooKeeperHandler;
import de.uniduesseldorf.utils.ZooKeeperHandler.ZooKeeperException;
import de.uniduesseldorf.utils.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.utils.unsafe.IntegerLongList;
import de.uniduesseldorf.utils.unsafe.AbstractKeyValueList.KeyValuePair;

/**
 * Leads data accesses to the local Chunks or a remote node
 * @author Florian Klein 09.03.2012
 */
public final class ChunkHandler implements ChunkInterface, MessageReceiver, ConnectionLostListener {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(ChunkHandler.class);

	private static final int INDEX_SIZE = 12016;

	private static final boolean LOG_ACTIVE = Core.getConfiguration().getBooleanValue(DXRAMConfigurationConstants.LOG_ACTIVE);
	private static final long SECONDARY_LOG_SIZE = Core.getConfiguration().getLongValue(DXRAMConfigurationConstants.SECONDARY_LOG_SIZE);
	private static final int REPLICATION_FACTOR = Core.getConfiguration().getIntValue(DXRAMConfigurationConstants.REPLICATION_FACTOR);

	// Attributes
	private short m_nodeID;
	private long m_rangeSize;
	private boolean m_firstRangeInitialized;

	private MemoryManagerComponent m_memoryManager;

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
		m_firstRangeInitialized = false;

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
			if (m_memoryManager.exists(chunkID)) {
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
			if (m_memoryManager.exists(chunkID)) {
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
	public void putRecoveredChunks(final Chunk[] p_chunks) throws DXRAMException {
		Contract.checkNotNull(p_chunks, "no chunks given");

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not use chunks");
		} else {
			putForeignChunks(p_chunks);
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
	 * Initializes the backup range for current locations
	 * and determines new backup peers if necessary
	 * @param p_localID
	 *            the current LocalID
	 * @param p_size
	 *            the size of the new created chunk
	 * @throws LookupException
	 *             if range could not be initialized
	 */
	private void initBackupRange(final long p_localID, final int p_size) throws LookupException {
		if (LOG_ACTIVE) {
			m_rangeSize += p_size + m_log.getAproxHeaderSize(m_nodeID, p_localID, p_size);
			if (!m_firstRangeInitialized && p_localID == 1) {
				// First Chunk has LocalID 1, but there is a Chunk with LocalID 0 for hosting the name service
				// This is the first put and p_localID is not reused
				determineBackupPeers(0);
				m_lookup.initRange((long) m_nodeID << 48, new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_log.initBackupRange((long) m_nodeID << 48, m_currentBackupRange.getBackupPeers());
				m_rangeSize = 0;
				m_firstRangeInitialized = true;
			} else if (m_rangeSize > SECONDARY_LOG_SIZE / 2) {
				determineBackupPeers(p_localID);
				m_lookup.initRange(((long) m_nodeID << 48) + p_localID, new Locations(m_nodeID, m_currentBackupRange.getBackupPeers(), null));
				m_log.initBackupRange(((long) m_nodeID << 48) + p_localID, m_currentBackupRange.getBackupPeers());
				m_rangeSize = 0;
			}
		} else if (!m_firstRangeInitialized && p_localID == 1) {
			m_lookup.initRange(((long) m_nodeID << 48) + 0xFFFFFFFFFFFFL, new Locations(m_nodeID, new short[] {-1, -1, -1}, null));
			m_firstRangeInitialized = true;
		}
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
}

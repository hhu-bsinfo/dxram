
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
import de.uniduesseldorf.dxram.core.backup.MigratedBackupsTree;
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
import de.uniduesseldorf.dxram.core.data.Chunk;
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
import de.uniduesseldorf.dxram.core.mem.MemoryManagerComponent;
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


	private MemoryManagerComponent m_memoryManager;

	private NetworkInterface m_network;
	private LookupInterface m_lookup;
	private LogInterface m_log;
	private LockInterface m_lock;



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
			m_migrationsTree = new MigratedBackupsTree((short) 10);
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
			if (m_memoryManager.exists(p_chunkID)) {
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

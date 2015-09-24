
package de.uniduesseldorf.dxram.core.lookup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import de.uniduesseldorf.dxram.commands.CmdUtils;
import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.chunk.ChunkInterface;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.exceptions.NetworkException;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.AskAboutBackupsRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.AskAboutBackupsResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.AskAboutSuccessorRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.AskAboutSuccessorResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.DelegatePromotePeerMessage;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.GetChunkIDRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.GetChunkIDResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.GetMappingCountRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.GetMappingCountResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.InitRangeRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.InitRangeResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.InsertIDRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.InsertIDResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.JoinRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.JoinResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.LookupReflectionRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.LookupReflectionResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.LookupRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.LookupResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.MigrateMessage;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.MigrateRangeRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.MigrateRangeResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.MigrateRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.MigrateResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.NotifyAboutFailedPeerMessage;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.NotifyAboutNewPredecessorMessage;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.NotifyAboutNewSuccessorMessage;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.PingSuperpeerMessage;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.PromotePeerRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.PromotePeerResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.RemoveRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.RemoveResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.SearchForPeerRequest;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.SearchForPeerResponse;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.SendBackupsMessage;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.SendSuperpeersMessage;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages.StartRecoveryMessage;
import de.uniduesseldorf.dxram.core.lookup.storage.AIDTableOptimized;
import de.uniduesseldorf.dxram.core.lookup.storage.CIDTreeOptimized;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.NetworkInterface;
import de.uniduesseldorf.dxram.core.net.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.dxram.utils.CRC16;
import de.uniduesseldorf.dxram.utils.Contract;
import de.uniduesseldorf.dxram.utils.ZooKeeperHandler;
import de.uniduesseldorf.dxram.utils.ZooKeeperHandler.ZooKeeperException;

/**
 * Lookup for meta-data:
 * 1. Find responsible superpeer in superpeer overlay
 * 2. Get location from corresponding CIDTree
 * @author Kevin Beineke
 *         03.06.2013
 */
public final class LookupHandler implements LookupInterface, MessageReceiver, ConnectionLostListener {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(LookupHandler.class);

	private static final short ORDER = 10;

	private static final short CLOSED_INTERVAL = 0;
	private static final short UPPER_CLOSED_INTERVAL = 1;
	private static final short OPEN_INTERVAL = 2;
	private static final boolean IS_SUPERPEER = true;
	private static final boolean IS_NOT_SUPERPEER = false;
	private static final boolean NO_CHECK = false;
	private static final boolean BACKUP = true;
	private static final boolean NO_BACKUP = false;
	private static final short DUMMY = -1;

	// Attributes
	private NetworkInterface m_network;
	private ChunkInterface m_chunk;

	private short m_me;
	private short m_predecessor;
	private short m_successor;
	private short m_mySuperpeer;
	private short m_bootstrap;
	private int m_numberOfSuperpeers;
	private ArrayList<Short> m_superpeers;
	private ArrayList<Short> m_peers;

	private int m_sleepInterval;
	private CIDTreeOptimized[] m_nodeTable;
	private ArrayList<Short> m_nodeList;

	private CRC16 m_hashGenerator;
	private AIDTableOptimized m_idTable;

	private Thread m_stabilizationThread;
	private SOWorker m_worker;

	private Lock m_overlayLock;
	private Lock m_dataLock;
	private Lock m_mappingLock;
	private Lock m_failureLock;
	private Lock m_promoteLock;

	// Constructors
	/**
	 * Creates an instance of LookupHandler
	 */
	public LookupHandler() {
		m_network = null;
		m_chunk = null;

		m_me = -1;
		m_predecessor = -1;
		m_successor = -1;
		m_mySuperpeer = -1;
		m_bootstrap = -1;
		m_numberOfSuperpeers = 0;
		m_superpeers = null;
		m_peers = null;

		m_sleepInterval = 0;
		m_nodeTable = null;
		m_nodeList = null;

		m_hashGenerator = new CRC16();
		m_idTable = null;

		m_stabilizationThread = null;
		m_worker = null;

		m_overlayLock = null;
		m_dataLock = null;
		m_mappingLock = null;
		m_failureLock = null;
		m_promoteLock = null;
	}

	// Methods
	@Override
	public void initialize() throws DXRAMException {
		LOGGER.trace("Entering initialize");

		try {
			m_bootstrap = Short.parseShort(new String(ZooKeeperHandler.getData("nodes/bootstrap")));
			m_numberOfSuperpeers = Integer.parseInt(new String(ZooKeeperHandler.getData("nodes/superpeers")));
		} catch (final ZooKeeperException e) {
			throw new LookupException("Could not access ZooKeeper", e);
		}

		m_sleepInterval = Core.getConfiguration().getIntValue(ConfigurationConstants.LOOKUP_SLEEP);

		m_network = CoreComponentFactory.getNetworkInterface();
		m_network.register(JoinRequest.class, this);
		m_network.register(LookupRequest.class, this);
		m_network.register(MigrateRequest.class, this);
		m_network.register(MigrateMessage.class, this);
		m_network.register(MigrateRangeRequest.class, this);
		m_network.register(InitRangeRequest.class, this);
		m_network.register(RemoveRequest.class, this);
		m_network.register(SendBackupsMessage.class, this);
		m_network.register(SendSuperpeersMessage.class, this);
		m_network.register(AskAboutBackupsRequest.class, this);
		m_network.register(AskAboutSuccessorRequest.class, this);
		m_network.register(NotifyAboutNewPredecessorMessage.class, this);
		m_network.register(NotifyAboutNewSuccessorMessage.class, this);
		m_network.register(PingSuperpeerMessage.class, this);
		m_network.register(SearchForPeerRequest.class, this);
		m_network.register(PromotePeerRequest.class, this);
		m_network.register(DelegatePromotePeerMessage.class, this);
		m_network.register(NotifyAboutFailedPeerMessage.class, this);
		m_network.register(StartRecoveryMessage.class, this);
		m_network.register(InsertIDRequest.class, this);
		m_network.register(GetChunkIDRequest.class, this);
		m_network.register(LookupReflectionRequest.class, this);

		m_me = NodeID.getLocalNodeID();
		Contract.check(-1 != m_me);

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			m_nodeTable = new CIDTreeOptimized[65536];
			m_nodeList = new ArrayList<Short>();
			m_idTable = new AIDTableOptimized(1000, 0.9f);

			m_numberOfSuperpeers--;
		}

		m_superpeers = new ArrayList<Short>(m_numberOfSuperpeers);
		Contract.checkNotNull(m_superpeers);
		m_peers = new ArrayList<Short>();
		Contract.checkNotNull(m_peers);

		m_overlayLock = new ReentrantLock(false);
		Contract.checkNotNull(m_overlayLock);
		m_dataLock = new ReentrantLock(false);
		Contract.checkNotNull(m_dataLock);
		m_mappingLock = new ReentrantLock(false);
		Contract.checkNotNull(m_mappingLock);
		m_failureLock = new ReentrantLock(false);
		Contract.checkNotNull(m_failureLock);
		m_promoteLock = new ReentrantLock(false);
		Contract.checkNotNull(m_promoteLock);

		createOrJoinSuperpeerOverlay(m_bootstrap);

		LOGGER.trace("Exiting initialize");
	}

	@Override
	public void initChunkHandler() throws DXRAMException {
		m_chunk = CoreComponentFactory.getChunkInterface();
	}

	@Override
	public void close() {
		LOGGER.trace("Entering close");

		if (null != m_stabilizationThread) {
			m_stabilizationThread.interrupt();
		}

		LOGGER.trace("Exiting close");
	}

	@Override
	public Locations get(final long p_chunkID) throws LookupException {
		Locations ret = null;
		short nid;
		short responsibleSuperpeer;
		boolean check = false;

		LookupRequest request;
		LookupResponse response;

		LOGGER.trace("Entering get with: p_chunkID=" + p_chunkID);

		if (!overlayIsStable()) {
			check = true;
		}
		nid = ChunkID.getCreatorID(p_chunkID);
		while (null == ret) {
			responsibleSuperpeer = getResponsibleSuperpeer(nid, check);

			if (-1 != responsibleSuperpeer) {
				request = new LookupRequest(responsibleSuperpeer, p_chunkID);
				Contract.checkNotNull(request);
				try {
					request.sendSync(m_network);
				} catch (final NetworkException e) {
					// Responsible superpeer is not available, try again and check responsible superpeer
					check = true;
					continue;
				}
				response = request.getResponse(LookupResponse.class);

				ret = response.getLocations();
			}
		}

		LOGGER.trace("Exiting get");
		return ret;
	}

	@Override
	public void initRange(final long p_firstChunkIDOrRangeID, final Locations p_primaryAndBackupPeers) throws LookupException {
		short responsibleSuperpeer;
		boolean finished = false;

		InitRangeRequest request;

		LOGGER.trace("Entering initRange with: p_endChunkID=" + p_firstChunkIDOrRangeID + ", p_locations=" + p_primaryAndBackupPeers);

		Contract.check(!NodeID.getRole().equals(Role.SUPERPEER));

		while (!finished) {
			responsibleSuperpeer = m_mySuperpeer;

			request = new InitRangeRequest(responsibleSuperpeer, p_firstChunkIDOrRangeID, p_primaryAndBackupPeers.convertToLong(), NO_BACKUP);
			Contract.checkNotNull(request);
			try {
				request.sendSync(m_network);
			} catch (final NetworkException e) {
				// Responsible superpeer is not available, try again (superpeers will be updated
				// automatically by network thread)
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e1) {}
				continue;
			}
			finished = request.getResponse(InitRangeResponse.class).getStatus();
		}

		LOGGER.trace("Exiting initRange");
	}

	@Override
	public void migrate(final long p_chunkID, final short p_nodeID) throws LookupException {
		short responsibleSuperpeer;
		boolean finished = false;

		MigrateRequest request;

		LOGGER.trace("Entering migrate with: p_chunkID=" + p_chunkID + ", p_nodeID=" + p_nodeID);

		while (!finished) {
			responsibleSuperpeer = m_mySuperpeer;

			request = new MigrateRequest(responsibleSuperpeer, p_chunkID, p_nodeID, NO_BACKUP);
			Contract.checkNotNull(request);
			try {
				request.sendSync(m_network);
			} catch (final NetworkException e) {
				// Responsible superpeer is not available, try again (superpeers will be updated
				// automatically by network thread)
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e1) {}
				continue;
			}
			finished = request.getResponse(MigrateResponse.class).getStatus();
		}

		LOGGER.trace("Exiting migrate");
	}

	@Override
	public void migrateRange(final long p_startCID, final long p_endCID, final short p_nodeID) throws LookupException {
		short creator;
		short responsibleSuperpeer;
		boolean finished = false;

		MigrateRangeRequest request;

		LOGGER.trace("Entering migrateRange with: p_startChunkID=" + p_startCID + ", p_endChunkID=" + p_endCID + ", p_nodeID=" + p_nodeID);

		creator = ChunkID.getCreatorID(p_startCID);
		if (creator != ChunkID.getCreatorID(p_endCID)) {
			LOGGER.error("Start and end objects creators not equal");
		} else {
			while (!finished) {
				responsibleSuperpeer = m_mySuperpeer;

				request = new MigrateRangeRequest(responsibleSuperpeer, p_startCID, p_endCID, p_nodeID, NO_BACKUP);
				Contract.checkNotNull(request);
				try {
					request.sendSync(m_network);
				} catch (final NetworkException e) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException e1) {}
					continue;
				}
				finished = request.getResponse(MigrateRangeResponse.class).getStatus();
			}
		}

		LOGGER.trace("Exiting migrateRange");
	}

	@Override
	public void migrateNotCreatedChunk(final long p_chunkID, final short p_nodeID) throws LookupException {
		short responsibleSuperpeer;
		short[] backupSuperpeers;

		CIDTreeOptimized tree;

		LOGGER.trace("Entering migrateNotCreatedChunk with: p_chunkID=" + p_chunkID + ", p_nodeID=" + p_nodeID);

		responsibleSuperpeer = getResponsibleSuperpeer(ChunkID.getCreatorID(p_chunkID), NO_CHECK);
		try {
			new MigrateMessage(responsibleSuperpeer, p_chunkID, p_nodeID, NO_BACKUP).send(m_network);
		} catch (final NetworkException e) {
			// Ignore superpeer failure, System will fix this
		}

		m_overlayLock.lock();
		backupSuperpeers = getBackupSuperpeers(responsibleSuperpeer);
		m_overlayLock.unlock();

		if (null != backupSuperpeers) {
			if (-1 != backupSuperpeers[0]) {
				if (isNodeInRange(m_me, responsibleSuperpeer, backupSuperpeers[backupSuperpeers.length - 1], OPEN_INTERVAL)) {
					m_dataLock.lock();
					tree = getCIDTree(ChunkID.getCreatorID(p_chunkID));
					tree.migrateObject(p_chunkID, p_nodeID);
					m_dataLock.unlock();
					// Send backups
					for (int i = 0; i < backupSuperpeers.length - 1; i++) {
						try {
							new MigrateMessage(backupSuperpeers[i], p_chunkID, p_nodeID, BACKUP).send(m_network);
						} catch (final NetworkException e) {
							// Ignore superpeer failure, System will fix this
							continue;
						}
					}
				} else {
					// Send backups
					for (int i = 0; i < backupSuperpeers.length; i++) {
						try {
							new MigrateMessage(backupSuperpeers[i], p_chunkID, p_nodeID, BACKUP).send(m_network);
						} catch (final NetworkException e) {
							// Ignore superpeer failure, System will fix this
							continue;
						}
					}
				}
			}
		}
		LOGGER.trace("Exiting migrateNotCreatedChunk");
	}

	@Override
	public void migrateOwnChunk(final long p_chunkID, final short p_nodeID) throws LookupException {
		short[] backupSuperpeers;

		CIDTreeOptimized tree;

		LOGGER.trace("Entering migrateOwnChunk with: p_chunkID=" + p_chunkID + ", p_nodeID=" + p_nodeID);

		m_dataLock.lock();
		tree = getCIDTree(m_me);
		tree.migrateObject(p_chunkID, p_nodeID);
		m_dataLock.unlock();

		m_overlayLock.lock();
		backupSuperpeers = getBackupSuperpeers(m_me);
		m_overlayLock.unlock();

		if (null != backupSuperpeers) {
			if (-1 != backupSuperpeers[0]) {
				// Send backups
				for (int i = 0; i < backupSuperpeers.length; i++) {
					try {
						new MigrateMessage(backupSuperpeers[i], p_chunkID, p_nodeID, BACKUP).send(m_network);
					} catch (final NetworkException e) {
						// Ignore superpeer failure, will fix this later
						continue;
					}
				}
			}
		}
		LOGGER.trace("Exiting migrateOwnChunk");
	}

	@Override
	public void insertID(final int p_id, final long p_chunkID) throws LookupException {
		short responsibleSuperpeer;
		short[] backupSuperpeers;
		boolean check = false;
		InsertIDRequest request;
		InsertIDResponse response;

		// Insert ChunkID <-> ApplicationID mapping
		LOGGER.trace("Entering insertID with: p_id=" + p_id + ", p_chunkID=" + p_chunkID);
		Contract.check(p_id < Math.pow(2, 31) && p_id >= 0);

		if (!overlayIsStable()) {
			check = true;
		}
		while (true) {
			responsibleSuperpeer = getResponsibleSuperpeer(m_hashGenerator.hash(p_id), check);

			if (-1 != responsibleSuperpeer) {
				request = new InsertIDRequest(responsibleSuperpeer, p_id, p_chunkID, NO_BACKUP);
				Contract.checkNotNull(request);
				try {
					request.sendSync(m_network);
				} catch (final NetworkException e) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException e1) {}
					continue;
				}
				response = request.getResponse(InsertIDResponse.class);

				backupSuperpeers = response.getBackupSuperpeers();
				if (null != backupSuperpeers) {
					if (-1 != backupSuperpeers[0]) {
						// Send backups
						for (int i = 0; i < backupSuperpeers.length; i++) {
							request = new InsertIDRequest(backupSuperpeers[i], p_id, p_chunkID, BACKUP);
							Contract.checkNotNull(request);
							try {
								request.sendSync(m_network);
							} catch (final NetworkException e) {
								// Ignore superpeer failure, own superpeer will fix this
								continue;
							}
						}
					}
					break;
				}
			}
		}

		LOGGER.trace("Exiting insertID");
	}

	@Override
	public long getChunkID(final int p_id) throws LookupException {
		long ret = -1;
		short responsibleSuperpeer;
		boolean check = false;
		GetChunkIDRequest request;

		// Resolve ChunkID <-> ApplicationID mapping to return corresponding ChunkID
		LOGGER.trace("Entering getChunkID with: p_id=" + p_id);

		if (!overlayIsStable()) {
			check = true;
		}
		while (-1 == ret) {
			responsibleSuperpeer = getResponsibleSuperpeer(m_hashGenerator.hash(p_id), check);

			if (-1 != responsibleSuperpeer) {
				request = new GetChunkIDRequest(responsibleSuperpeer, p_id);
				Contract.checkNotNull(request);
				try {
					request.sendSync(m_network);
				} catch (final NetworkException e) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException e1) {}
					continue;
				}
				ret = request.getResponse(GetChunkIDResponse.class).getChunkID();
			}
		}

		LOGGER.trace("Exiting getChunkID");

		return ret;
	}

	@Override
	public long getMappingCount() throws LookupException {
		long ret = 0;
		Short[] superpeers;
		GetMappingCountRequest request;
		GetMappingCountResponse response;

		m_overlayLock.lock();
		superpeers = m_superpeers.toArray(new Short[m_superpeers.size()]);
		m_overlayLock.unlock();

		for (short superpeer : superpeers) {
			request = new GetMappingCountRequest(superpeer);
			try {
				request.sendSync(m_network);
				response = request.getResponse(GetMappingCountResponse.class);
				ret += response.getCount();
			} catch (final NetworkException e) {
				e.printStackTrace();
			}
		}

		return ret;
	}

	@Override
	public void remove(final long p_chunkID) throws LookupException {
		short responsibleSuperpeer;
		short[] backupSuperpeers;

		RemoveRequest request;
		RemoveResponse response;

		LOGGER.trace("Entering remove with: p_chunkID=" + p_chunkID);

		Contract.check(!NodeID.getRole().equals(Role.SUPERPEER));

		while (true) {
			responsibleSuperpeer = m_mySuperpeer;

			request = new RemoveRequest(responsibleSuperpeer, new long[] {p_chunkID}, NO_BACKUP);
			Contract.checkNotNull(request);
			try {
				request.sendSync(m_network);
			} catch (final NetworkException e) {
				// Responsible superpeer is not available, try again (superpeers will be updated
				// automatically by network thread)
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e1) {}
				continue;
			}
			response = request.getResponse(RemoveResponse.class);

			backupSuperpeers = response.getBackupSuperpeers();
			if (null != backupSuperpeers) {
				if (-1 != backupSuperpeers[0]) {
					// Send backups
					for (int i = 0; i < backupSuperpeers.length; i++) {
						request = new RemoveRequest(backupSuperpeers[i], new long[] {p_chunkID}, BACKUP);
						Contract.checkNotNull(request);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							// Ignore superpeer failure, own superpeer will fix this
							continue;
						}
					}
				}
				break;
			}
		}

		LOGGER.trace("Exiting remove");
	}

	@Override
	public void remove(final long[] p_chunkIDs) throws LookupException {
		short responsibleSuperpeer;
		short[] backupSuperpeers;

		RemoveRequest request;
		RemoveResponse response;

		LOGGER.trace("Entering remove with: p_chunkIDs=" + p_chunkIDs);

		Contract.check(!NodeID.getRole().equals(Role.SUPERPEER));

		while (true) {
			responsibleSuperpeer = m_mySuperpeer;

			request = new RemoveRequest(responsibleSuperpeer, p_chunkIDs, NO_BACKUP);
			Contract.checkNotNull(request);
			try {
				request.sendSync(m_network);
			} catch (final NetworkException e) {
				// Responsible superpeer is not available, try again (superpeers will be updated
				// automatically by network thread)
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e1) {}
				continue;
			}
			response = request.getResponse(RemoveResponse.class);

			backupSuperpeers = response.getBackupSuperpeers();
			if (null != backupSuperpeers) {
				if (-1 != backupSuperpeers[0]) {
					// Send backups
					for (int i = 0; i < backupSuperpeers.length; i++) {
						request = new RemoveRequest(backupSuperpeers[i], p_chunkIDs, BACKUP);
						Contract.checkNotNull(request);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							// Ignore superpeer failure, own superpeer will fix this
							continue;
						}
					}
				}
				break;
			}
		}

		LOGGER.trace("Exiting remove");
	}

	@Override
	public void invalidate(final long... p_chunkIDs) {
		// Not supported in no-cache mode
	}

	@Override
	public void invalidate(final long p_startCID, final long p_endCID) {
		// Not supported in no-cache mode
	}

	@Override
	public boolean creatorAvailable(final short p_creator) {
		boolean ret = false;

		if (0 > Collections.binarySearch(m_superpeers, p_creator)) {
			// Deactivate connection manager temporarily to accelerate creator checking
			m_network.deactivateConnectionManager();
			try {
				new SendSuperpeersMessage(p_creator, m_superpeers).send(m_network);
				ret = true;
			} catch (final NetworkException e) {
				// Peer is not available anymore
			}
			m_network.activateConnectionManager();
		}

		return ret;
	}

	@Override
	public boolean isLastSuperpeer() {
		boolean ret = false;
		short superpeer;
		int i = 0;

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			ret = true;
			if (!m_superpeers.isEmpty()) {
				while (i < m_superpeers.size()) {
					superpeer = m_superpeers.get(i++);
					try {
						new PingSuperpeerMessage(superpeer).send(m_network);
					} catch (final NetworkException e) {
						continue;
					}
					ret = false;
					break;
				}
			}
		}

		return ret;
	}

	@Override
	public List<Short> getSuperpeers() {
		return m_superpeers;
	}

	@Override
	public boolean overlayIsStable() {
		return m_numberOfSuperpeers == m_superpeers.size();
	}

	/**
	 * Checks if there are other known superpeers
	 * @return true if there are other known superpeers, false otherwise
	 */
	public boolean isOnlySuperpeer() {
		return NodeID.getRole().equals(Role.SUPERPEER) && m_superpeers.isEmpty();
	}

	/**
	 * Joins the superpeer overlay through contactSuperpeer
	 * @param p_contactSuperpeer
	 *            NodeID of a known superpeer
	 * @note if contactSuperpeer is (-1), a new superpeer overlay will be created
	 * @throws DXRAMException
	 *             if the node could not join the superpeer overlay
	 */
	private void createOrJoinSuperpeerOverlay(final short p_contactSuperpeer) throws DXRAMException {
		short contactSuperpeer;
		JoinRequest joinRequest = null;
		JoinResponse joinResponse = null;
		ArrayList<CIDTreeOptimized> trees;
		CIDTreeOptimized tree;

		LOGGER.trace("Entering createOrJoinSuperpeerOverlay with: p_contactSuperpeer=" + p_contactSuperpeer);

		contactSuperpeer = p_contactSuperpeer;

		if (m_me == contactSuperpeer) {
			if (NodeID.getRole().equals(Role.SUPERPEER)) {
				LOGGER.trace("Setting up new ring, I am " + m_me);
				setSuccessor(m_me);
			} else {
				LOGGER.error("Bootstrap has to be a superpeer, exiting now.");
				Core.close();
				System.exit(-1);
			}
		} else {
			if (NodeID.getRole().equals(Role.SUPERPEER)) {
				while (-1 != contactSuperpeer) {
					LOGGER.trace("Contacting " + contactSuperpeer + " to join the ring, I am " + m_me);

					joinRequest = new JoinRequest(contactSuperpeer, m_me, IS_SUPERPEER);
					Contract.checkNotNull(joinRequest);
					try {
						joinRequest.sendSync(m_network);
					} catch (final NetworkException e) {
						// Contact superpeer is not available, get a new contact superpeer
						contactSuperpeer = getCurrentBootstrap();
						continue;
					}
					joinResponse = joinRequest.getResponse(JoinResponse.class);
					contactSuperpeer = joinResponse.getNewContactSuperpeer();
				}

				m_superpeers = joinResponse.getSuperpeers();

				m_peers = joinResponse.getPeers();

				trees = joinResponse.getCIDTrees();
				for (int i = 0; i < trees.size(); i++) {
					tree = trees.get(i);
					addCIDTree(tree.getCreator(), trees.get(i));
				}
				m_idTable.putAll(joinResponse.getMappings());

				setSuccessor(joinResponse.getSuccessor());
				setPredecessor(joinResponse.getPredecessor());
			} else {
				while (-1 != contactSuperpeer) {
					LOGGER.trace("Contacting " + contactSuperpeer + " to get the responsible superpeer, I am " + m_me);

					joinRequest = new JoinRequest(contactSuperpeer, m_me, IS_NOT_SUPERPEER);
					Contract.checkNotNull(joinRequest);
					try {
						joinRequest.sendSync(m_network);
					} catch (final NetworkException e) {
						// Contact superpeer is not available, get a new contact superpeer
						contactSuperpeer = getCurrentBootstrap();
						continue;
					}
					joinResponse = joinRequest.getResponse(JoinResponse.class);
					contactSuperpeer = joinResponse.getNewContactSuperpeer();
				}
				m_superpeers = joinResponse.getSuperpeers();
				m_mySuperpeer = joinResponse.getSource();
				insertSuperpeer(m_mySuperpeer);

				LOGGER.trace("Exiting createOrJoinSuperpeerOverlay");
				return;
			}
		}

		LOGGER.trace("Starting stabilization thread");
		m_worker = new SOWorker();

		m_stabilizationThread = new Thread(m_worker);
		Contract.checkNotNull(m_stabilizationThread);
		m_stabilizationThread.setName(SOWorker.class.getSimpleName() + " for " + LookupHandler.class.getSimpleName());
		m_stabilizationThread.setDaemon(true);
		m_stabilizationThread.start();

		LOGGER.trace("Exiting createOrJoinSuperpeerOverlay");
	}

	/* helper methods: superpeers */

	/**
	 * Sets the successor for the current superpeer
	 * @param p_nodeID
	 *            NodeID of the successor
	 * @note assumes m_overlayLock has been locked
	 */
	private void setSuccessor(final short p_nodeID) {
		m_successor = p_nodeID;
		if (-1 != m_successor && m_me != m_successor) {
			insertSuperpeer(m_successor);
		}
	}

	/**
	 * Sets the predecessor for the current superpeer
	 * @param p_nodeID
	 *            NodeID of the predecessor
	 * @note assumes m_overlayLock has been locked
	 */
	private void setPredecessor(final short p_nodeID) {
		m_predecessor = p_nodeID;
		if (m_predecessor != m_successor) {
			insertSuperpeer(m_predecessor);
		}
	}

	/**
	 * Determines the responsible superpeer for ChunkID
	 * @param p_nodeID
	 *            NodeID from chunk whose location is searched
	 * @param p_check
	 *            whether the result has to be checked (in case of incomplete superpeer overlay) or not
	 * @return the responsible superpeer for given ChunkID
	 */
	private short getResponsibleSuperpeer(final short p_nodeID, final boolean p_check) {
		short responsibleSuperpeer = -1;
		short predecessor;
		short hisSuccessor;
		int index;
		AskAboutSuccessorRequest request = null;
		AskAboutSuccessorResponse response = null;

		LOGGER.trace("Entering getResponsibleSuperpeer with: p_chunkID=" + p_nodeID);

		m_overlayLock.lock();
		if (!m_superpeers.isEmpty()) {
			index = Collections.binarySearch(m_superpeers, p_nodeID);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_superpeers.size()) {
					index = 0;
				}
			}
			responsibleSuperpeer = m_superpeers.get(index);

			if (p_check && 1 < m_superpeers.size()) {
				if (0 == index) {
					index = m_superpeers.size() - 1;
				} else {
					index--;
				}
				predecessor = m_superpeers.get(index);
				m_overlayLock.unlock();

				while (true) {
					request = new AskAboutSuccessorRequest(predecessor);
					Contract.checkNotNull(request);
					try {
						request.sendSync(m_network);
					} catch (final DXRAMException e) {
						// Predecessor is not available, try responsibleSuperpeer without checking
						break;
					}
					response = request.getResponse(AskAboutSuccessorResponse.class);
					Contract.checkNotNull(response);
					hisSuccessor = response.getSuccessor();
					if (responsibleSuperpeer == hisSuccessor) {
						break;
					} else if (isNodeInRange(p_nodeID, predecessor, hisSuccessor, OPEN_INTERVAL)) {
						responsibleSuperpeer = hisSuccessor;
						break;
					} else {
						predecessor = hisSuccessor;
					}
				}
			} else {
				m_overlayLock.unlock();
			}
		} else {
			LOGGER.warn("do not know any superpeer");
			m_overlayLock.unlock();
		}
		LOGGER.trace("Exiting getResponsibleSuperpeer");

		return responsibleSuperpeer;
	}

	/* helper methods: CIDTree */

	/**
	 * Returns the requested CIDTree
	 * @param p_nodeID
	 *            NodeID for that the CIDTree is requested
	 * @return the CIDTree for given NodeID
	 * @note assumes m_dataLock has been locked
	 */
	private CIDTreeOptimized getCIDTree(final short p_nodeID) {
		return m_nodeTable[p_nodeID & 0xFFFF];
	}

	/**
	 * Adds the given CIDTree to NIDTable
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_tree
	 *            the CIDTree to add
	 * @note assumes m_dataLock has been locked
	 */
	private void addCIDTree(final short p_nodeID, final CIDTreeOptimized p_tree) {
		int index;

		index = Collections.binarySearch(m_nodeList, p_nodeID);
		if (0 > index) {
			index = index * -1 - 1;
			m_nodeList.add(index, p_nodeID);
		}
		m_nodeTable[p_nodeID & 0xFFFF] = p_tree;
	}

	/**
	 * Deletes the given CIDTree
	 * @param p_nodeID
	 *            the NodeID
	 * @note assumes m_dataLock has been locked
	 */
	private void deleteCIDTree(final short p_nodeID) {
		int index;

		if (0 != m_nodeList.size()) {
			index = Collections.binarySearch(m_nodeList, p_nodeID);
			if (0 <= index) {
				m_nodeList.remove(index);
				m_nodeTable[p_nodeID & 0xFFFF] = null;
			}
		}
	}

	/* helper methods: keys */

	/**
	 * Verifies if node is between start and end
	 * @param p_nodeID
	 *            NodeID to compare
	 * @param p_startID
	 *            first NodeID
	 * @param p_endID
	 *            last NodeID
	 * @param p_type
	 *            the type of the interval (open, half-closed, closed)
	 * @return true if p_key is between p_start and p_end (including p_end or not), false otherwise
	 */
	public static boolean isNodeInRange(final short p_nodeID, final short p_startID, final short p_endID, final short p_type) {
		boolean ret = false;

		if (CLOSED_INTERVAL == p_type) {
			if (p_startID < p_endID) {
				// Example: m = 8, start = 2, end = 6 -> true: 2, 3, 4, 5, 6; false: 0, 1, 7
				if (p_nodeID >= p_startID && p_nodeID <= p_endID) {
					ret = true;
				}
			} else {
				// Example: m = 8, start = 6, end = 2 -> true: 6, 7, 0, 1, 2; false: 3, 4, 5
				if (p_nodeID >= p_startID || p_nodeID <= p_endID) {
					ret = true;
				}
			}
		} else if (UPPER_CLOSED_INTERVAL == p_type) {
			if (p_startID < p_endID) {
				// Example: m = 8, start = 2, end = 6 -> true: 3, 4, 5, 6; false: 0, 1, 2, 7
				if (p_nodeID > p_startID && p_nodeID <= p_endID) {
					ret = true;
				}
			} else {
				// Example: m = 8, start = 6, end = 2 -> true: 7, 0, 1, 2; false: 3, 4, 5, 6
				if (p_nodeID > p_startID || p_nodeID <= p_endID) {
					ret = true;
				}
			}
		} else {
			if (p_startID < p_endID) {
				// Example: m = 8, start = 2, end = 6 -> true: 3, 4, 5; false: 0, 1, 2, 6, 7
				if (p_nodeID > p_startID && p_nodeID < p_endID) {
					ret = true;
				}
			} else {
				// Example: m = 8, start = 6, end = 2 -> true: 7, 0, 1; false: 2, 3, 4, 5, 6
				if (p_nodeID > p_startID || p_nodeID < p_endID) {
					ret = true;
				}
			}
		}

		return ret;
	}

	/* helper methods: Superpeer operations */

	/**
	 * Inserts the superpeer at given position in the superpeer array
	 * @param p_superpeer
	 *            NodeID of the new superpeer
	 * @note assumes m_overlayLock has been locked
	 */
	private void insertSuperpeer(final short p_superpeer) {
		int index;

		Contract.check(-1 != p_superpeer);

		index = Collections.binarySearch(m_superpeers, p_superpeer);
		if (0 > index) {
			m_superpeers.add(index * -1 - 1, p_superpeer);
		}
	}

	/**
	 * Removes superpeer
	 * @param p_superpeer
	 *            NodeID of the superpeer that has to be removed
	 * @return true if p_superpeer was found and deleted, false otherwise
	 * @note assumes m_overlayLock has been locked
	 */
	private boolean removeSuperpeer(final short p_superpeer) {
		boolean ret = false;
		int index;

		index = Collections.binarySearch(m_superpeers, p_superpeer);
		if (0 <= index) {
			m_superpeers.remove(index);
			if (p_superpeer == m_successor) {
				if (0 != m_superpeers.size()) {
					if (index < m_superpeers.size()) {
						m_successor = m_superpeers.get(index);
					} else {
						m_successor = m_superpeers.get(0);
					}
				} else {
					m_successor = (short) -1;
				}
			}
			if (p_superpeer == m_predecessor) {
				if (0 != m_superpeers.size()) {
					if (0 < index) {
						m_predecessor = m_superpeers.get(index - 1);
					} else {
						m_predecessor = m_superpeers.get(m_superpeers.size() - 1);
					}
				} else {
					m_predecessor = (short) -1;
				}
			}
			ret = true;
		}

		return ret;
	}

	/**
	 * Inserts the peer at given position in the peer array
	 * @param p_peer
	 *            NodeID of the new peer
	 * @note assumes m_overlayLock has been locked
	 */
	private void insertPeer(final short p_peer) {
		int index;

		Contract.check(-1 != p_peer);

		index = Collections.binarySearch(m_peers, p_peer);
		if (0 > index) {
			m_peers.add(index * -1 - 1, p_peer);
		}
	}

	/**
	 * Removes peer
	 * @param p_peer
	 *            NodeID of the peer that has to be removed
	 * @return true if p_peer was found and deleted, false otherwise
	 * @note assumes m_overlayLock has been locked
	 */
	private boolean removePeer(final short p_peer) {
		boolean ret = false;
		int index;

		index = Collections.binarySearch(m_peers, p_peer);
		if (0 <= index) {
			m_peers.remove(index);
			ret = true;
		}

		return ret;
	}

	/* Message/Request handler */

	/**
	 * Handles an incoming JoinRequest
	 * @param p_joinRequest
	 *            the JoinRequest
	 */
	private void incomingJoinRequest(final JoinRequest p_joinRequest) {
		short joiningNode;
		short currentPeer;
		int index;
		int startIndex;
		Iterator<Short> iter;
		ArrayList<Short> peers;
		ArrayList<CIDTreeOptimized> trees;

		byte[] mappings;
		short joiningNodesPredecessor;
		short superpeer;
		short[] responsibleArea;

		boolean newNodeisSuperpeer;

		LOGGER.trace("Got request: JOIN_REQUEST from " + p_joinRequest.getSource());

		joiningNode = p_joinRequest.getNewNode();
		newNodeisSuperpeer = p_joinRequest.nodeIsSuperpeer();

		if (isOnlySuperpeer() || isNodeInRange(joiningNode, m_predecessor, m_me, OPEN_INTERVAL)) {
			if (newNodeisSuperpeer) {
				m_overlayLock.lock();
				// Send the joining node not only the successor, but the predecessor, superpeers
				// and all relevant CIDTrees
				if (isOnlySuperpeer()) {
					joiningNodesPredecessor = m_me;
				} else {
					joiningNodesPredecessor = m_predecessor;
				}

				iter = m_peers.iterator();
				peers = new ArrayList<Short>();
				while (iter.hasNext()) {
					currentPeer = iter.next();
					if (isNodeInRange(currentPeer, joiningNodesPredecessor, joiningNode, OPEN_INTERVAL)) {
						peers.add(currentPeer);
					}
				}

				m_dataLock.lock();
				trees = new ArrayList<CIDTreeOptimized>();
				responsibleArea = getResponsibleArea(joiningNode);
				if (0 != m_nodeList.size()) {
					index = Collections.binarySearch(m_nodeList, responsibleArea[0]);
					if (0 > index) {
						index = index * -1 - 1;
						if (index == m_nodeList.size()) {
							index = 0;
						}
					}
					startIndex = index;
					currentPeer = m_nodeList.get(index++);
					while (isNodeInRange(currentPeer, responsibleArea[0], responsibleArea[1], OPEN_INTERVAL)) {
						trees.add(getCIDTree(currentPeer));
						if (index == m_nodeList.size()) {
							index = 0;
						}
						if (index == startIndex) {
							break;
						}
						currentPeer = m_nodeList.get(index++);
					}
				}
				m_dataLock.unlock();

				m_mappingLock.lock();
				mappings = m_idTable.toArray(responsibleArea[0], responsibleArea[1], isOnlySuperpeer(), CLOSED_INTERVAL);
				m_mappingLock.unlock();

				try {
					new JoinResponse(p_joinRequest, (short) -1, joiningNodesPredecessor, m_me, mappings, m_superpeers, peers, trees).send(m_network);
				} catch (final NetworkException e) {
					// Joining node is not available anymore -> ignore request and return directly
					m_overlayLock.unlock();
					return;
				}

				for (int i = 0; i < peers.size(); i++) {
					removePeer(peers.get(i));
				}

				// Notify predecessor about the joining node
				if (isOnlySuperpeer()) {
					setSuccessor(joiningNode);
					setPredecessor(joiningNode);
					m_overlayLock.unlock();
				} else {
					setPredecessor(joiningNode);
					m_overlayLock.unlock();

					try {
						new NotifyAboutNewSuccessorMessage(joiningNodesPredecessor, m_predecessor).send(m_network);
					} catch (final NetworkException e) {
						// Old predecessor is not available anymore, ignore it
					}
				}
			} else {
				m_overlayLock.lock();
				insertPeer(joiningNode);
				try {
					new JoinResponse(p_joinRequest, (short) -1, (short) -1, (short) -1, null, m_superpeers, null, null).send(m_network);
				} catch (final NetworkException e) {
					// Joining node is not available anymore, ignore request
				}
				m_overlayLock.unlock();
			}
		} else {
			superpeer = getResponsibleSuperpeer(joiningNode, NO_CHECK);
			try {
				new JoinResponse(p_joinRequest, superpeer, (short) -1, (short) -1, null, null, null, null).send(m_network);
			} catch (final NetworkException e) {
				// Joining node is not available anymore, ignore request
			}
		}
	}

	/**
	 * Handles an incoming LookupRequest
	 * @param p_lookupRequest
	 *            the LookupRequest
	 */
	private void incomingLookupRequest(final LookupRequest p_lookupRequest) {
		long chunkID;
		Locations result = null;
		CIDTreeOptimized tree;

		LOGGER.trace("Got request: LOOKUP_REQUEST " + p_lookupRequest.getSource());

		chunkID = p_lookupRequest.getChunkID();
		m_dataLock.lock();
		tree = getCIDTree(ChunkID.getCreatorID(chunkID));
		if (null != tree) {
			result = tree.getMetadata(chunkID);
		}
		m_dataLock.unlock();
		try {
			new LookupResponse(p_lookupRequest, result).send(m_network);
		} catch (final NetworkException e) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	/**
	 * Handles an incoming InitRangeRequest
	 * @param p_initRangeRequest
	 *            the InitRangeRequest
	 */
	private void incomingInitRangeRequest(final InitRangeRequest p_initRangeRequest) {
		Locations primaryAndBackupPeers;
		long startChunkIDRangeID;
		short creator;
		short[] backupSuperpeers;
		CIDTreeOptimized tree;
		InitRangeRequest request;
		boolean isBackup;

		LOGGER.trace("Got Message: INIT_RANGE_REQUEST from " + p_initRangeRequest.getSource());

		primaryAndBackupPeers = new Locations(p_initRangeRequest.getLocations());
		startChunkIDRangeID = p_initRangeRequest.getStartChunkIDOrRangeID();
		creator = primaryAndBackupPeers.getPrimaryPeer();
		isBackup = p_initRangeRequest.isBackup();

		if (isOnlySuperpeer() || isNodeInRange(creator, m_predecessor, m_me, OPEN_INTERVAL)) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				tree = new CIDTreeOptimized(ORDER);
				addCIDTree(creator, tree);
			}
			if (ChunkID.getCreatorID(startChunkIDRangeID) != -1) {
				tree.initRange(startChunkIDRangeID, creator, primaryAndBackupPeers.getBackupPeers());
			} else {
				tree.initMigrationRange((int) startChunkIDRangeID, creator, primaryAndBackupPeers.getBackupPeers());
			}
			m_dataLock.unlock();

			m_overlayLock.lock();
			backupSuperpeers = getBackupSuperpeers(m_me);
			m_overlayLock.unlock();
			if (-1 != backupSuperpeers[0]) {
				// Send backups
				for (int i = 0; i < backupSuperpeers.length; i++) {
					request = new InitRangeRequest(backupSuperpeers[i], startChunkIDRangeID, primaryAndBackupPeers.convertToLong(), BACKUP);
					Contract.checkNotNull(request);
					try {
						request.sendSync(m_network);
					} catch (final NetworkException e) {
						// Ignore superpeer failure, superpeer will fix this later
						continue;
					}
				}
			}
			try {
				new InitRangeResponse(p_initRangeRequest, true).send(m_network);
			} catch (final NetworkException e) {
				// Requesting peer is not available anymore, ignore it
			}
		} else if (isBackup) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				tree = new CIDTreeOptimized((short) 10);
				addCIDTree(creator, tree);
			}
			if ((startChunkIDRangeID & 0x0000FFFFFFFFFFFFL) != 0) {
				tree.initRange(startChunkIDRangeID, creator, primaryAndBackupPeers.getBackupPeers());
			} else {
				tree.initMigrationRange((int) startChunkIDRangeID, creator, primaryAndBackupPeers.getBackupPeers());
			}
			m_dataLock.unlock();
			try {
				new InitRangeResponse(p_initRangeRequest, true).send(m_network);
			} catch (final NetworkException e) {
				// Requesting peer is not available anymore, ignore it
			}
		} else {
			// Not responsible for requesting peer
			try {
				new InitRangeResponse(p_initRangeRequest, false).send(m_network);
			} catch (final NetworkException e) {
				// Requesting node is not available anymore, ignore it
			}
		}
	}

	/**
	 * Handles an incoming MigrateRequest
	 * @param p_migrateRequest
	 *            the MigrateRequest
	 */
	private void incomingMigrateRequest(final MigrateRequest p_migrateRequest) {
		short nodeID;
		long chunkID;
		short creator;
		short[] backupSuperpeers;
		CIDTreeOptimized tree;
		MigrateRequest request;
		boolean isBackup;

		LOGGER.trace("Got Message: MIGRATE_REQUEST from " + p_migrateRequest.getSource());

		nodeID = p_migrateRequest.getNodeID();
		chunkID = p_migrateRequest.getChunkID();
		creator = ChunkID.getCreatorID(chunkID);
		isBackup = p_migrateRequest.isBackup();

		if (isOnlySuperpeer() || isNodeInRange(creator, m_predecessor, m_me, OPEN_INTERVAL)) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				m_dataLock.unlock();
				LOGGER.error("CIDTree range not initialized on responsible superpeer " + m_me);
				try {
					new MigrateResponse(p_migrateRequest, false).send(m_network);
				} catch (final NetworkException e) {
					// Requesting peer is not available anymore, ignore request it
				}
			} else {
				tree.migrateObject(chunkID, nodeID);
				m_dataLock.unlock();

				m_overlayLock.lock();
				backupSuperpeers = getBackupSuperpeers(m_me);
				m_overlayLock.unlock();
				if (-1 != backupSuperpeers[0]) {
					// Send backups
					for (int i = 0; i < backupSuperpeers.length; i++) {
						request = new MigrateRequest(backupSuperpeers[i], chunkID, nodeID, BACKUP);
						Contract.checkNotNull(request);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							// Ignore superpeer failure, superpeer will fix this later
							continue;
						}
					}
				}
				try {
					new MigrateResponse(p_migrateRequest, true).send(m_network);
				} catch (final NetworkException e) {
					// Requesting peer is not available anymore, ignore it
				}
			}
		} else if (isBackup) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				LOGGER.warn("CIDTree range not initialized on backup superpeer " + m_me);
			} else {
				tree.migrateObject(chunkID, nodeID);
			}
			m_dataLock.unlock();
			try {
				new MigrateResponse(p_migrateRequest, true).send(m_network);
			} catch (final NetworkException e) {
				// Requesting peer is not available anymore, ignore it
			}
		} else {
			// Not responsible for requesting peer
			try {
				new MigrateResponse(p_migrateRequest, false).send(m_network);
			} catch (final NetworkException e) {
				// Requesting peer is not available anymore, ignore it
			}
		}
	}

	/**
	 * Handles an incoming MigrateRangeRequest
	 * @param p_migrateRangeRequest
	 *            the MigrateRangeRequest
	 */
	private void incomingMigrateRangeRequest(final MigrateRangeRequest p_migrateRangeRequest) {
		short nodeID;
		long startChunkID;
		long endChunkID;
		short creator;
		short[] backupSuperpeers;
		CIDTreeOptimized tree;
		MigrateRangeRequest request;
		boolean isBackup;

		LOGGER.trace("Got Message: MIGRATE_RANGE_REQUEST from " + p_migrateRangeRequest.getSource());

		nodeID = p_migrateRangeRequest.getNodeID();
		startChunkID = p_migrateRangeRequest.getStartChunkID();
		endChunkID = p_migrateRangeRequest.getEndChunkID();
		creator = ChunkID.getCreatorID(startChunkID);
		isBackup = p_migrateRangeRequest.isBackup();

		if (creator != ChunkID.getCreatorID(endChunkID)) {
			LOGGER.error("start and end objects creators not equal");
			return;
		}

		if (isOnlySuperpeer() || isNodeInRange(creator, m_predecessor, m_me, OPEN_INTERVAL)) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				m_dataLock.unlock();
				LOGGER.error("CIDTree range not initialized on responsible superpeer " + m_me);
				try {
					new MigrateRangeResponse(p_migrateRangeRequest, false).send(m_network);
				} catch (final NetworkException e) {
					// Requesting peer is not available anymore, ignore it
				}
			} else {
				tree.migrateRange(startChunkID, endChunkID, nodeID);
				m_dataLock.unlock();

				m_overlayLock.lock();
				backupSuperpeers = getBackupSuperpeers(m_me);
				m_overlayLock.unlock();
				if (-1 != backupSuperpeers[0]) {
					// Send backups
					for (int i = 0; i < backupSuperpeers.length; i++) {
						request = new MigrateRangeRequest(backupSuperpeers[i], startChunkID, endChunkID, nodeID, BACKUP);
						Contract.checkNotNull(request);
						try {
							request.sendSync(m_network);
						} catch (final NetworkException e) {
							// Ignore superpeer failure, superpeer will fix this later
							continue;
						}
					}
				}
				try {
					new MigrateRangeResponse(p_migrateRangeRequest, true).send(m_network);
				} catch (final NetworkException e) {
					// Requesting peer is not available anymore, ignore it
				}
			}
		} else if (isBackup) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				LOGGER.warn("CIDTree range not initialized on backup superpeer " + m_me);
			} else {
				tree.migrateRange(startChunkID, endChunkID, nodeID);
			}
			m_dataLock.unlock();
			try {
				new MigrateRangeResponse(p_migrateRangeRequest, true).send(m_network);
			} catch (final NetworkException e) {
				// Requesting peer is not available anymore, ignore it
			}
		} else {
			// Not responsible for requesting peer
			try {
				new MigrateRangeResponse(p_migrateRangeRequest, false).send(m_network);
			} catch (final NetworkException e) {
				// Requesting peer is not available anymore, ignore request it
			}
		}
	}

	/**
	 * Handles an incoming RemoveRequest
	 * @param p_removeRequest
	 *            the RemoveRequest
	 */
	private void incomingRemoveRequest(final RemoveRequest p_removeRequest) {
		long[] chunkIDs;
		short creator;
		short[] backupSuperpeers;
		boolean isBackup;
		CIDTreeOptimized tree;

		LOGGER.trace("Got Message: REMOVE_REQUEST from " + p_removeRequest.getSource());

		chunkIDs = p_removeRequest.getChunkIDs();
		isBackup = p_removeRequest.isBackup();

		for (long chunkID : chunkIDs) {
			creator = ChunkID.getCreatorID(chunkID);
			if (isOnlySuperpeer() || isNodeInRange(creator, m_predecessor, m_me, OPEN_INTERVAL)) {
				m_dataLock.lock();
				tree = getCIDTree(creator);
				if (null == tree) {
					m_dataLock.unlock();
					LOGGER.error("CIDTree range not initialized on responsible superpeer " + m_me);
					try {
						new RemoveResponse(p_removeRequest, new short[] {-1}).send(m_network);
					} catch (final NetworkException e) {
						// Requesting peer is not available anymore, ignore it
					}
				} else {
					tree.removeObject(chunkID);
					m_dataLock.unlock();

					m_overlayLock.lock();
					backupSuperpeers = getBackupSuperpeers(m_me);
					m_overlayLock.unlock();
					try {
						new RemoveResponse(p_removeRequest, backupSuperpeers).send(m_network);
					} catch (final NetworkException e) {
						// Requesting peer is not available anymore, ignore it
					}
				}
			} else if (isBackup) {
				m_dataLock.lock();
				tree = getCIDTree(creator);
				if (null == tree) {
					LOGGER.warn("CIDTree range not initialized on backup superpeer " + m_me);
				} else {
					tree.removeObject(chunkID);
				}
				m_dataLock.unlock();
				try {
					new RemoveResponse(p_removeRequest, null).send(m_network);
				} catch (final NetworkException e) {
					// Requesting peer is not available anymore, ignore it
				}
			} else {
				// Not responsible for requesting peer
				try {
					new RemoveResponse(p_removeRequest, null).send(m_network);
				} catch (final NetworkException e) {
					// Requesting peer is not available anymore, ignore it
				}
			}
		}

	}

	/**
	 * Handles an incoming SendBackupsMessage
	 * @param p_sendBackupsMessage
	 *            the SendBackupsMessage
	 */
	private void incomingSendBackupsMessage(final SendBackupsMessage p_sendBackupsMessage) {
		short source;
		ArrayList<CIDTreeOptimized> trees;
		CIDTreeOptimized tree;

		source = p_sendBackupsMessage.getSource();
		LOGGER.trace("Got Message: SEND_BACKUPS_MESSAGE from " + source);

		trees = p_sendBackupsMessage.getCIDTrees();
		m_dataLock.lock();
		for (int i = 0; i < trees.size(); i++) {
			tree = trees.get(i);
			addCIDTree(tree.getCreator(), tree);
		}
		m_dataLock.unlock();

		m_mappingLock.lock();
		m_idTable.putAll(p_sendBackupsMessage.getMappings());
		m_mappingLock.unlock();
	}

	/**
	 * Handles an incoming SendSuperpeersMessage
	 * @param p_sendSuperpeersMessage
	 *            the SendSuperpeersMessage
	 */
	private void incomingSendSuperpeersMessage(final SendSuperpeersMessage p_sendSuperpeersMessage) {
		short source;

		source = p_sendSuperpeersMessage.getSource();
		LOGGER.trace("Got Message: SEND_SUPERPEERS_MESSAGE from " + source);

		m_overlayLock.lock();
		m_superpeers = p_sendSuperpeersMessage.getSuperpeers();
		insertSuperpeer(source);
		m_overlayLock.unlock();

		if (m_mySuperpeer != source) {
			if (source == getResponsibleSuperpeer(m_me, NO_CHECK)) {
				m_mySuperpeer = source;
			}
		}
	}

	/**
	 * Info about chunk, called by incomingReflectionRequest
	 * @param p_cmd
	 *            the command string
	 * @return the result String
	 */
	private String cmdReqChunkinfo(final String p_cmd) {
		String ret = null;
		short nodeID;
		long localID;
		long chunkID;
		String[] arguments;
		CIDTreeOptimized tree;
		Locations locations;

		arguments = p_cmd.split(" ");
		if (arguments == null) {
			ret = "  error: problem in command";
		} else if (arguments.length < 3) {
			ret = "  error: problem in command";
		} else {
			nodeID = CmdUtils.getNIDfromTuple(arguments[1]);
			localID = CmdUtils.getLIDfromTuple(arguments[1]);
			chunkID = CmdUtils.calcCID(nodeID, localID);

			System.out.println("chunkinfo for " + nodeID + "," + localID);
			// System.out.println("   getCIDTree:"+nodeID);
			tree = getCIDTree(nodeID);
			if (tree == null) {
				ret = "  error: no CIDtree for given NID=" + nodeID;
			} else {
				// get meta-data from tree
				locations = tree.getMetadata(chunkID);
				if (locations == null) {
					System.out.println(" tree.getMetadata failed");
					ret = "  error: tree.getMetadata failed";
				} else {
					ret = "  Stored on peer=" + locations.toString();
				}
			}
		}
		return ret;
	}

	/**
	 * Handles 'backups' command. Called by incomingReflectionRequest
	 * @param p_command
	 *            the CommandMessage
	 * @return the result string
	 */
	private String cmdReqBackups(final String p_command) {
		final String ret = "";

		System.out.println("LookupHandler.cmdReqBackups");

		/*-if (m_ownBackupRanges != null) {
			for (int i = 0; i < m_ownBackupRanges.size(); i++) {
				final BackupRange br = m_ownBackupRanges.get(i);
				ret = ret + "  BR" + Integer.toString(i) + ":";

				if (br != null) {
					// System.out.println("   BackupRange: "+i+", m_firstChunkIDORRangeID="+br.m_firstChunkIDORRangeID);
					ret = ret + Long.toString(br.m_firstChunkIDORRangeID) + "(";

					for (int j = 0; j < br.m_backupPeers.length; j++) {
						// System.out.println("      backup peer: "+j+": "+br.m_backupPeers[j]);
						ret = ret + Short.toString(br.m_backupPeers[j]);
						if (j < br.m_backupPeers.length - 1) {
							ret = ret + ",";
						}
					}
					ret = ret + ")";
					if (i < m_ownBackupRanges.size() - 1) {
						ret = ret + "\n";
					}
				}
			}
		} else {
			ret = "  No backups.";
		}*/

		return ret;
	}

	/**
	 * Handles an incoming ReflectionRequest
	 * @param p_lookupRequest
	 *            the ReflectionRequest
	 */
	private void incomingReflectionRequest(final LookupReflectionRequest p_lookupRequest) {
		String cmd;
		String res = null;

		cmd = p_lookupRequest.getArgument();
		res = "success: incomingReflectionRequest";

		// process request
		if (NodeID.getRole().equals(Role.SUPERPEER)) {

			if (cmd.indexOf("chunkinfo") >= 0) {
				res = cmdReqChunkinfo(cmd);
			} else if (cmd.indexOf("backups") >= 0) {
				res = cmdReqBackups(cmd);
			}
		} else {
			res = "error: reflection command can be processed by superpeers only";
		}

		// send response
		try {
			new LookupReflectionResponse(p_lookupRequest, res).send(m_network);
			System.out.println("response sent");
		} catch (final NetworkException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Handles an incoming AskAboutBackupsRequest
	 * @param p_askAboutBackupsRequest
	 *            the AskAboutBackupsRequest
	 */
	private void incomingAskAboutBackupsRequest(final AskAboutBackupsRequest p_askAboutBackupsRequest) {
		int index;
		int startIndex;
		short currentPeer;
		short lowerBound;
		byte[] mappings;
		byte[] oldMappings;
		byte[] allMappings = null;
		ArrayList<CIDTreeOptimized> trees;
		ArrayList<Short> peers;

		LOGGER.trace("Got request: ASK_ABOUT_SUCCESSOR_REQUEST from " + p_askAboutBackupsRequest.getSource());

		trees = new ArrayList<CIDTreeOptimized>();
		peers = p_askAboutBackupsRequest.getPeers();
		m_dataLock.lock();
		lowerBound = m_predecessor;
		// Compare m_nodeList with given list, add missing entries to "trees"
		if (0 != m_nodeList.size()) {
			index = Collections.binarySearch(m_nodeList, lowerBound);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_nodeList.size()) {
					index = 0;
				}
			}
			startIndex = index;
			currentPeer = m_nodeList.get(index++);
			while (isNodeInRange(currentPeer, lowerBound, m_me, OPEN_INTERVAL)) {
				if (0 > Collections.binarySearch(peers, currentPeer)) {
					trees.add(getCIDTree(currentPeer));

					mappings = m_idTable.toArray(currentPeer, currentPeer, false, CLOSED_INTERVAL);
					if (null == allMappings) {
						allMappings = mappings;
					} else {
						oldMappings = allMappings;
						allMappings = new byte[oldMappings.length + mappings.length];
						System.arraycopy(oldMappings, 0, allMappings, 0, oldMappings.length);
						System.arraycopy(mappings, 0, allMappings, oldMappings.length, mappings.length);
					}

					System.out.println("---------------------------- " + currentPeer + " ----------------------------");
				}
				if (index == m_nodeList.size()) {
					index = 0;
				}
				if (index == startIndex) {
					break;
				}
				currentPeer = m_nodeList.get(index++);
			}
		}
		m_dataLock.unlock();

		try {
			new AskAboutBackupsResponse(p_askAboutBackupsRequest, trees, allMappings).send(m_network);
		} catch (final NetworkException e) {
			// Requesting superpeer is not available anymore, ignore request and remove superpeer
			failureHandling(p_askAboutBackupsRequest.getSource());
		}
	}

	/**
	 * Handles an incoming AskAboutSuccessorRequest
	 * @param p_askAboutSuccessorRequest
	 *            the AskAboutSuccessorRequest
	 */
	private void incomingAskAboutSuccessorRequest(final AskAboutSuccessorRequest p_askAboutSuccessorRequest) {
		LOGGER.trace("Got request: ASK_ABOUT_SUCCESSOR_REQUEST from " + p_askAboutSuccessorRequest.getSource());

		try {
			new AskAboutSuccessorResponse(p_askAboutSuccessorRequest, m_successor).send(m_network);
		} catch (final NetworkException e) {
			// Requesting superpeer is not available anymore, ignore request and remove superpeer
			failureHandling(p_askAboutSuccessorRequest.getSource());
		}
	}

	/**
	 * Handles an incoming NotifyAboutNewPredecessorMessage
	 * @param p_notifyAboutNewPredecessorMessage
	 *            the NotifyAboutNewPredecessorMessage
	 */
	private void incomingNotifyAboutNewPredecessorMessage(final NotifyAboutNewPredecessorMessage p_notifyAboutNewPredecessorMessage) {
		short possiblePredecessor;

		LOGGER.trace("Got Message: NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE from " + p_notifyAboutNewPredecessorMessage.getSource());

		possiblePredecessor = p_notifyAboutNewPredecessorMessage.getNewPredecessor();
		if (m_predecessor != possiblePredecessor) {
			if (isNodeInRange(possiblePredecessor, m_predecessor, m_me, OPEN_INTERVAL)) {
				m_overlayLock.lock();
				setPredecessor(possiblePredecessor);
				m_overlayLock.unlock();
			}
		}
	}

	/**
	 * Handles an incoming NotifyAboutNewSuccessorMessage
	 * @param p_notifyAboutNewSuccessorMessage
	 *            the NotifyAboutNewSuccessorMessage
	 */
	private void incomingNotifyAboutNewSuccessorMessage(final NotifyAboutNewSuccessorMessage p_notifyAboutNewSuccessorMessage) {
		short possibleSuccessor;

		LOGGER.trace("Got Message: NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE from " + p_notifyAboutNewSuccessorMessage.getSource());

		possibleSuccessor = p_notifyAboutNewSuccessorMessage.getNewSuccessor();
		if (m_successor != possibleSuccessor) {
			if (isNodeInRange(possibleSuccessor, m_me, m_successor, OPEN_INTERVAL)) {
				m_overlayLock.lock();
				setSuccessor(possibleSuccessor);
				m_overlayLock.unlock();
			}
		}
	}

	/**
	 * Handles an incoming SearchForPeerRequest
	 * @param p_searchForPeerRequest
	 *            the SearchForPeerRequest
	 */
	private void incomingSearchForPeerRequest(final SearchForPeerRequest p_searchForPeerRequest) {
		short peer = -1;

		LOGGER.trace("Got request: SEARCH_FOR_PEER_REQUEST from " + p_searchForPeerRequest.getSource());

		m_overlayLock.lock();
		if (0 < m_peers.size()) {
			peer = m_peers.get((int) (m_peers.size() * Math.random()));
		}
		m_overlayLock.unlock();
		try {
			new SearchForPeerResponse(p_searchForPeerRequest, peer).send(m_network);
		} catch (final NetworkException e) {
			// Requesting superpeer is not available anymore, ignore request and remove superpeer
			failureHandling(p_searchForPeerRequest.getSource());
		}
	}

	/**
	 * Handles an incoming PromotePeerRequest
	 * @param p_promotePeerRequest
	 *            the PromotePeerRequest
	 */
	private void incomingPromotePeerRequest(final PromotePeerRequest p_promotePeerRequest) {
		short replacement;
		ArrayList<CIDTreeOptimized> trees;
		CIDTreeOptimized tree;

		LOGGER.trace("Got request: PROMOTE_PEER_REQUEST from " + p_promotePeerRequest.getSource());

		System.out.println();
		System.out.println();
		System.out.println("********** ********** Promotion ********** **********");
		System.out.println("* Got promotion from " + p_promotePeerRequest.getSource());

		// Promote this peer to superpeer
		m_overlayLock.lock();

		NodeID.setRole(Role.SUPERPEER);
		m_numberOfSuperpeers--;
		m_nodeTable = new CIDTreeOptimized[65536];
		m_nodeList = new ArrayList<Short>();
		if (m_idTable == null) {
			m_idTable = new AIDTableOptimized(1000, 0.9f);
		}

		m_idTable.putAll(p_promotePeerRequest.getMappings());

		m_superpeers = p_promotePeerRequest.getSuperpeers();

		m_peers = p_promotePeerRequest.getPeers();

		trees = p_promotePeerRequest.getCIDTrees();
		for (int i = 0; i < trees.size(); i++) {
			tree = trees.get(i);
			addCIDTree(tree.getCreator(), tree);
			if (m_me == tree.getCreator()) {
				tree.setStatus(false);
			}
		}

		setSuccessor(p_promotePeerRequest.getSuccessor());
		setPredecessor(p_promotePeerRequest.getPredecessor());
		m_overlayLock.unlock();

		// Give away all stored chunks
		replacement = p_promotePeerRequest.getReplacement();
		System.out.println("* Migrating chunks to " + replacement);
		try {
			m_chunk.migrateAll(replacement);

			System.out.println("** Migration complete");

			LOGGER.trace("Starting stabilization thread");
			m_worker = new SOWorker();

			m_stabilizationThread = new Thread(m_worker);
			Contract.checkNotNull(m_stabilizationThread);
			m_stabilizationThread.setName(SOWorker.class.getSimpleName() + " for " + LookupHandler.class.getSimpleName());
			m_stabilizationThread.setDaemon(true);
			m_stabilizationThread.start();
			try {
				new PromotePeerResponse(p_promotePeerRequest, true).send(m_network);
			} catch (final NetworkException e) {
				// Requesting superpeer is not available anymore, ignore request and remove superpeer
				failureHandling(p_promotePeerRequest.getSource());
			}
		} catch (final DXRAMException e) {
			System.out.println("** Migration failed");

			// Revert everything
			NodeID.setRole(Role.PEER);
			m_numberOfSuperpeers++;
			m_nodeTable = null;
			m_nodeList = null;
			m_peers = null;
			m_successor = -1;
			m_predecessor = -1;
			try {
				new PromotePeerResponse(p_promotePeerRequest, false).send(m_network);
			} catch (final NetworkException exp) {
				// Requesting superpeer is not available anymore, ignore request and remove superpeer
			}
		}
		System.out.println("********** ********** ** End ** ********** **********");
		System.out.println();
		System.out.println();
	}

	/**
	 * Handles an incoming DelegatePromotePeerMessage
	 * @param p_delegatePromotePeerMessage
	 *            the DelegatePromotePeerMessage
	 */
	private void incomingDelegatePromotePeerMessage(final DelegatePromotePeerMessage p_delegatePromotePeerMessage) {
		short newSuperpeer = -1;
		short otherSuperpeer;
		short peer = -1;
		short hops;
		int ret = 0;

		LOGGER.trace("Got message: DELEGATE_PROMOTE_PEER_MESSAGE from " + p_delegatePromotePeerMessage.getSource());

		System.out.println();
		System.out.println();
		System.out.println("********** ********** Promoting Peer ********** **********");

		hops = p_delegatePromotePeerMessage.getHops();
		if (hops < 2 * m_superpeers.size()) {
			while (true) {
				if (1 < m_peers.size()) {
					m_overlayLock.lock();
					while (peer == newSuperpeer) {
						peer = m_peers.get((int) (m_peers.size() * Math.random()));
					}
					m_overlayLock.unlock();
					newSuperpeer = peer;
				} else {
					// Delegate promotion
					while (true) {
						if (!isOnlySuperpeer()) {
							m_overlayLock.lock();
							otherSuperpeer = m_superpeers.get((int) (m_superpeers.size() * Math.random()));
							m_overlayLock.unlock();
							System.out.println("* Delegating to " + otherSuperpeer + ", hopcount: " + hops);
							try {
								new DelegatePromotePeerMessage(otherSuperpeer, ++hops).send(m_network);
							} catch (final NetworkException e) {
								// Other superpeer is not available, remove it and try another
								m_overlayLock.unlock();
								failureHandling(otherSuperpeer);
								m_overlayLock.lock();
								continue;
							}
						} else {
							LOGGER.error("* Superpeer replacement not possible");
						}
						break;
					}
					break;
				}

				System.out.println("* New superpeer shall be " + newSuperpeer);
				ret = promote(newSuperpeer, false);
				if (0 == ret || -2 == ret) {
					break;
				}
			}
		}
	}

	/**
	 * Handles an incoming NotifyAboutFailedPeerMessage
	 * @param p_notifyAboutFailedPeerMessage
	 *            the NotifyAboutFailedPeerMessage
	 */
	private void incomingNotifyAboutFailedPeerMessage(final NotifyAboutFailedPeerMessage p_notifyAboutFailedPeerMessage) {
		short failedPeer;
		CIDTreeOptimized tree;
		Iterator<Short> iter;

		LOGGER.trace("Got message: NOTIFY_ABOUT_FAILED_PEER_MESSAGE from " + p_notifyAboutFailedPeerMessage.getSource());

		failedPeer = p_notifyAboutFailedPeerMessage.getFailedPeer();
		m_dataLock.lock();
		// Remove failedPeer from all backuppeer lists
		iter = m_nodeList.iterator();
		while (iter.hasNext()) {
			getCIDTree(iter.next()).removeBackupPeer(failedPeer, DUMMY);
		}

		tree = getCIDTree(failedPeer);
		if (null != tree) {
			tree.setStatus(false);
		}
		m_dataLock.unlock();
	}

	/**
	 * Handles an incoming StartRecoveryMessage
	 * @param p_startRecoveryMessage
	 *            the StartRecoveryMessage
	 */
	private void incomingStartRecoveryMessage(final StartRecoveryMessage p_startRecoveryMessage) {

		LOGGER.trace("Got message: START_RECOVERY_MESSAGE from " + p_startRecoveryMessage.getSource());

		System.out.println("********** Starting recovery for " + p_startRecoveryMessage.getFailedPeer() + " **********");
		// TODO: Start recovery
	}

	/**
	 * Handles an incoming InsertIDRequest
	 * @param p_insertIDRequest
	 *            the InsertIDRequest
	 */
	private void incomingInsertIDRequest(final InsertIDRequest p_insertIDRequest) {
		int id;
		short[] backupSuperpeers;

		LOGGER.trace("Got request: INSERT_ID_REQUEST from " + p_insertIDRequest.getSource());

		id = p_insertIDRequest.getID();
		if (isOnlySuperpeer() || isNodeInRange(m_hashGenerator.hash(id), m_predecessor, m_me, CLOSED_INTERVAL)) {
			m_mappingLock.lock();
			m_idTable.put(id, p_insertIDRequest.getChunkID());
			m_mappingLock.unlock();

			m_overlayLock.lock();
			backupSuperpeers = getBackupSuperpeers(m_me);
			m_overlayLock.unlock();
			try {
				new InsertIDResponse(p_insertIDRequest, backupSuperpeers).send(m_network);
			} catch (final NetworkException e) {
				// Requesting peer is not available anymore, ignore it
			}
		} else if (p_insertIDRequest.isBackup()) {
			m_mappingLock.lock();
			m_idTable.put(id, p_insertIDRequest.getChunkID());
			m_mappingLock.unlock();

			try {
				new InsertIDResponse(p_insertIDRequest, null).send(m_network);
			} catch (final NetworkException e) {
				// Requesting peer is not available anymore, ignore it
			}
		} else {
			// Not responsible for that chunk
			try {
				new InsertIDResponse(p_insertIDRequest, null).send(m_network);
			} catch (final NetworkException e) {
				// Requesting peer is not available anymore, ignore it
			}
		}
	}

	/**
	 * Handles an incoming GetChunkIDRequest
	 * @param p_getChunkIDRequest
	 *            the GetChunkIDRequest
	 */
	private void incomingGetChunkIDRequest(final GetChunkIDRequest p_getChunkIDRequest) {
		int id;
		long chunkID = -1;

		LOGGER.trace("Got request: GET_CHUNKID_REQUEST from " + p_getChunkIDRequest.getSource());

		id = p_getChunkIDRequest.getID();
		if (isOnlySuperpeer() || isNodeInRange(m_hashGenerator.hash(id), m_predecessor, m_me, CLOSED_INTERVAL)) {
			m_mappingLock.lock();
			chunkID = m_idTable.get(id);
			m_mappingLock.unlock();
		}
		try {
			new GetChunkIDResponse(p_getChunkIDRequest, chunkID).send(m_network);
		} catch (final NetworkException e) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	/**
	 * Handles an incoming Message
	 * @param p_message
	 *            the Message
	 */
	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == LookupMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case LookupMessages.SUBTYPE_JOIN_REQUEST:
					incomingJoinRequest((JoinRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_INIT_RANGE_REQUEST:
					incomingInitRangeRequest((InitRangeRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_LOOKUP_REQUEST:
					incomingLookupRequest((LookupRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_MIGRATE_REQUEST:
					incomingMigrateRequest((MigrateRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST:
					incomingMigrateRangeRequest((MigrateRangeRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_REMOVE_REQUEST:
					incomingRemoveRequest((RemoveRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE:
					incomingSendBackupsMessage((SendBackupsMessage) p_message);
					break;
				case LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE:
					incomingSendSuperpeersMessage((SendSuperpeersMessage) p_message);
					break;
				case LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST:
					incomingAskAboutBackupsRequest((AskAboutBackupsRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST:
					incomingAskAboutSuccessorRequest((AskAboutSuccessorRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE:
					incomingNotifyAboutNewPredecessorMessage((NotifyAboutNewPredecessorMessage) p_message);
					break;
				case LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE:
					incomingNotifyAboutNewSuccessorMessage((NotifyAboutNewSuccessorMessage) p_message);
					break;
				case LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE:
					break;
				case LookupMessages.SUBTYPE_SEARCH_FOR_PEER_REQUEST:
					incomingSearchForPeerRequest((SearchForPeerRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_PROMOTE_PEER_REQUEST:
					incomingPromotePeerRequest((PromotePeerRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_DELEGATE_PROMOTE_PEER_MESSAGE:
					incomingDelegatePromotePeerMessage((DelegatePromotePeerMessage) p_message);
					break;
				case LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE:
					incomingNotifyAboutFailedPeerMessage((NotifyAboutFailedPeerMessage) p_message);
					break;
				case LookupMessages.SUBTYPE_START_RECOVERY_MESSAGE:
					incomingStartRecoveryMessage((StartRecoveryMessage) p_message);
					break;
				case LookupMessages.SUBTYPE_INSERT_ID_REQUEST:
					incomingInsertIDRequest((InsertIDRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_GET_CHUNKID_REQUEST:
					incomingGetChunkIDRequest((GetChunkIDRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_LOOKUP_REFLECTION_REQUEST:
					incomingReflectionRequest((LookupReflectionRequest) p_message);
					break;
				default:
					break;
				}
			}
		}
	}

	/**
	 * Handles a lost connection
	 * @param p_event
	 *            the event
	 */
	@Override
	public void triggerEvent(final ConnectionLostEvent p_event) {
		LOGGER.trace("Entering trigger with: p_event=" + p_event);

		LOGGER.trace("Exiting onConnectionLost");
	}

	/**
	 * Determines the backup superpeers for this superpeer
	 * @param p_nodeID
	 *            the NodeID
	 * @return the three successing superpeers
	 * @note assumes m_overlayLock has been locked
	 */
	private short[] getBackupSuperpeers(final short p_nodeID) {
		short[] superpeers;
		int size;
		int index;

		if (isOnlySuperpeer()) {
			// LOGGER.warn("no replication possible. Too less superpeers");
			superpeers = new short[] {-1};
		} else {
			size = Math.min(m_superpeers.size(), 3);
			if (3 > size) {
				// LOGGER.warn("replication incomplete. Too less superpeers");
			}
			superpeers = new short[size];

			index = Collections.binarySearch(m_superpeers, p_nodeID);
			if (0 > index) {
				index = index * -1 - 1;
			} else {
				index++;
			}
			for (int i = 0; i < size; i++) {
				if (index == m_superpeers.size()) {
					superpeers[i] = m_superpeers.get(0);
					index = 1;
				} else {
					superpeers[i] = m_superpeers.get(index);
					index++;
				}
			}
		}
		return superpeers;
	}

	/**
	 * Determines the superpeers this superpeer stores backups for
	 * @param p_nodeID
	 *            the NodeID
	 * @return the superpeers p_nodeID is responsible for (got backups for)
	 * @note assumes m_overlayLock has been locked
	 */
	private short[] getResponsibleArea(final short p_nodeID) {
		short[] responsibleArea;
		short size;
		short index;

		size = (short) m_superpeers.size();
		responsibleArea = new short[2];
		if (3 < size) {
			index = (short) Collections.binarySearch(m_superpeers, m_predecessor);
			if (3 <= index) {
				index -= 3;
			} else {
				index = (short) (size - (3 - index));
			}
			responsibleArea[0] = m_superpeers.get(index);
			responsibleArea[1] = p_nodeID;
		} else {
			responsibleArea[0] = p_nodeID;
			responsibleArea[1] = p_nodeID;
		}
		return responsibleArea;
	}

	/**
	 * Determines a new bootstrap
	 */
	private void determineNewBootstrap() {
		m_bootstrap = replaceBootstrap(m_me);

		if (m_bootstrap != m_me) {
			try {
				new PingSuperpeerMessage(m_bootstrap).send(m_network);
			} catch (final NetworkException e) {
				// New bootstrap is not available, start failure handling to
				// remove bootstrap from superpeer array and to determine a new bootstrap
				LOGGER.error("new bootstrap failed, too");
				m_failureLock.unlock();
				failureHandling(m_bootstrap);
				m_failureLock.lock();
			}
		}
	}

	/**
	 * Takes over failed superpeers peers and CIDTrees
	 * @param p_nodeID
	 *            the NodeID
	 */
	private void takeOverPeersAndCIDTrees(final short p_nodeID) {
		short predecessor;
		short firstPeer;
		short currentPeer;
		int index;
		int startIndex;

		m_overlayLock.lock();
		if (m_superpeers.isEmpty()) {
			firstPeer = (short) (m_me + 1);
		} else {
			index = Collections.binarySearch(m_superpeers, p_nodeID);
			if (0 > index) {
				index = index * -1 - 1;
			}
			if (0 == index) {
				predecessor = m_superpeers.get(m_superpeers.size() - 1);
			} else {
				predecessor = m_superpeers.get(index - 1);
			}
			if (predecessor == p_nodeID) {
				firstPeer = (short) (m_me + 1);
			} else {
				firstPeer = predecessor;
			}
		}
		m_overlayLock.unlock();

		m_dataLock.lock();
		if (0 != m_nodeList.size()) {
			index = Collections.binarySearch(m_nodeList, firstPeer);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_nodeList.size()) {
					index = 0;
				}
			}
			startIndex = index;
			currentPeer = m_nodeList.get(index++);
			while (isNodeInRange(currentPeer, firstPeer, p_nodeID, CLOSED_INTERVAL)) {
				if (getCIDTree(currentPeer).getStatus()) {
					if (0 > Collections.binarySearch(m_peers, currentPeer) && 0 > Collections.binarySearch(m_superpeers, currentPeer)) {
						System.out.println("** Taking over " + currentPeer);
						m_overlayLock.lock();
						insertPeer(currentPeer);
						m_overlayLock.unlock();
					}
				}
				if (index == m_nodeList.size()) {
					index = 0;
				}
				if (index == startIndex) {
					break;
				}
				currentPeer = m_nodeList.get(index++);
			}
		}
		m_dataLock.unlock();
	}

	/**
	 * Promote a peer to superpeer
	 * @param p_nodeID
	 *            the NodeID
	 */
	private void promoteOnePeer(final short p_nodeID) {
		short newSuperpeer = -1;
		short otherSuperpeer;
		int ret = 0;
		boolean delegate = false;

		while (true) {
			if (0 < m_peers.size() && !delegate) {
				m_overlayLock.lock();
				newSuperpeer = m_peers.get(0);
				for (final short p : m_peers) {
					if (p < p_nodeID) {
						newSuperpeer = p;
					} else {
						break;
					}
				}
				m_overlayLock.unlock();
			} else {
				// Delegate promotion
				while (true) {
					if (!isOnlySuperpeer()) {
						m_overlayLock.lock();
						otherSuperpeer = m_superpeers.get((int) (m_superpeers.size() * Math.random()));
						m_overlayLock.unlock();
						System.out.println("** Do not have enough peers, delegating to " + otherSuperpeer);
						try {
							new DelegatePromotePeerMessage(otherSuperpeer, (short) 1).send(m_network);
						} catch (final NetworkException e) {
							// Other superpeer is not available, try another
							continue;
						}
					} else {
						LOGGER.error("superpeer replacement not possible");
					}
					break;
				}
				break;
			}

			System.out.println("** " + newSuperpeer + " shall be the new superpeer");
			ret = promote(newSuperpeer, true);
			if (0 == ret) {
				System.out.println("** Promotion successful");
				break;
			} else if (-2 == ret) {
				delegate = true;
			}
		}
	}

	/**
	 * Execute promote
	 * @param p_newSuperpeer
	 *            the NodeID of the peer that will be promoted
	 * @param p_safe
	 *            false if called by network thread, false otherwise
	 * @return 0 if promotion was successful, -1 if peer is not available, -2 if there is no second peer
	 */
	private int promote(final short p_newSuperpeer, final boolean p_safe) {
		int ret = 0;

		short newResponsiblePeer = -1;
		short superpeersPredecessor = 0;
		short otherSuperpeer;
		short currentPeer = p_newSuperpeer;
		short[] responsibleArea;

		int i = 0;
		int index;
		int startIndex;
		boolean success = false;

		byte[] mappings;

		CIDTreeOptimized tree;

		Iterator<Short> iter;
		ArrayList<Short> peers = null;
		ArrayList<CIDTreeOptimized> trees;

		SearchForPeerRequest searchForPeerRequest;
		SearchForPeerResponse searchForPeerResponse;
		PromotePeerRequest promotePeerRequest;

		if (m_promoteLock.tryLock()) {
			while (!success) {
				if (1 < m_peers.size()) {
					m_overlayLock.lock();
					while (currentPeer == p_newSuperpeer) {
						currentPeer = m_peers.get((int) (m_peers.size() * Math.random()));
					}
					newResponsiblePeer = currentPeer;
					m_overlayLock.unlock();
				} else if (p_safe) {
					// Search for another peer
					while (-1 == newResponsiblePeer && i++ < m_superpeers.size() * 2) {
						m_overlayLock.lock();
						otherSuperpeer = m_superpeers.get((int) (m_superpeers.size() * Math.random()));
						m_overlayLock.unlock();
						System.out.println("** do not have enough peers, aksing " + otherSuperpeer);
						searchForPeerRequest = new SearchForPeerRequest(otherSuperpeer);
						try {
							searchForPeerRequest.sendSync(m_network);
							searchForPeerResponse = searchForPeerRequest.getResponse(SearchForPeerResponse.class);
						} catch (final NetworkException exc) {
							// Other superpeer is not available, try another
							continue;
						}
						newResponsiblePeer = searchForPeerResponse.getPeer();
					}
					if (-1 == newResponsiblePeer) {
						ret = -2;
						break;
					}
				} else {
					ret = -2;
					break;
				}

				System.out.println("** " + newResponsiblePeer + " shall get all data of " + p_newSuperpeer);

				// Send the joining node not only the successor, but the predecessor, superpeers
				// and all relevant CIDTrees
				if (isOnlySuperpeer()) {
					superpeersPredecessor = m_me;
				} else {
					superpeersPredecessor = m_predecessor;
				}

				m_overlayLock.lock();
				iter = m_peers.iterator();
				peers = new ArrayList<Short>();
				while (iter.hasNext()) {
					currentPeer = iter.next();
					if (isNodeInRange(currentPeer, superpeersPredecessor, p_newSuperpeer, OPEN_INTERVAL)) {
						peers.add(currentPeer);
					}
				}

				trees = new ArrayList<CIDTreeOptimized>();
				responsibleArea = getResponsibleArea(p_newSuperpeer);
				m_overlayLock.unlock();

				m_dataLock.lock();
				if (0 != m_nodeList.size()) {
					index = Collections.binarySearch(m_nodeList, responsibleArea[0]);
					if (0 > index) {
						index = index * -1 - 1;
						if (index == m_nodeList.size()) {
							index = 0;
						}
					}
					startIndex = index;
					currentPeer = m_nodeList.get(index++);
					while (isNodeInRange(currentPeer, responsibleArea[0], responsibleArea[1], UPPER_CLOSED_INTERVAL)) {
						tree = getCIDTree(currentPeer);
						if (null != tree) {
							System.out.println("*** Sending meta-data from " + currentPeer + " to " + p_newSuperpeer);
							trees.add(tree);
						}
						if (index == m_nodeList.size()) {
							index = 0;
						}
						if (index == startIndex) {
							break;
						}
						currentPeer = m_nodeList.get(index++);
					}
				}
				m_dataLock.unlock();

				m_mappingLock.lock();
				mappings = m_idTable.toArray(responsibleArea[0], responsibleArea[1], isOnlySuperpeer(), UPPER_CLOSED_INTERVAL);
				m_mappingLock.unlock();

				promotePeerRequest =
						new PromotePeerRequest(p_newSuperpeer, superpeersPredecessor, m_me, newResponsiblePeer, mappings, m_superpeers, peers, trees);
				Contract.checkNotNull(promotePeerRequest);
				if (p_safe) {
					try {
						promotePeerRequest.sendSync(m_network);
						success = promotePeerRequest.getResponse(PromotePeerResponse.class).getStatus();
					} catch (final NetworkException e) {
						// Peer is not available anymore, get a new one
						LOGGER.error("*** Promote " + p_newSuperpeer + " failed, try another peer");
						ret = -1;
						break;
					}
				} else {
					// If this method is called after delegation, this is executed by the network thread
					// that must not wait for responses
					try {
						promotePeerRequest.send(m_network);
						success = true;
					} catch (final NetworkException e) {
						// Peer is not available anymore, get a new one
						LOGGER.error("*** Promote " + p_newSuperpeer + " failed, try another peer");
						ret = -1;
						break;
					}
				}
			}

			if (0 == ret) {
				m_overlayLock.lock();
				for (i = 0; i < peers.size(); i++) {
					removePeer(peers.get(i));
				}
				removePeer(p_newSuperpeer);

				// Notify predecessor about the new superpeer
				if (isOnlySuperpeer()) {
					setSuccessor(p_newSuperpeer);
					setPredecessor(p_newSuperpeer);
					m_overlayLock.unlock();
				} else {
					setPredecessor(p_newSuperpeer);
					m_overlayLock.unlock();

					try {
						new NotifyAboutNewSuccessorMessage(superpeersPredecessor, m_predecessor).send(m_network);
					} catch (final NetworkException e) {
						// Old predecessor is not available anymore, ignore it
					}
				}
			}
			m_promoteLock.unlock();
		}

		return ret;
	}

	/**
	 * Spread data of failed superpeer
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_responsibleArea
	 *            the responsible area
	 */
	private void spreadDataOfFailedSuperpeer(final short p_nodeID, final short[] p_responsibleArea) {
		short currentPeer;
		int index;
		int startIndex;
		byte[] mappings;
		byte[] oldMappings;
		byte[] allMappings = null;
		ArrayList<CIDTreeOptimized> trees;

		trees = new ArrayList<CIDTreeOptimized>();
		m_dataLock.lock();
		if (0 != m_nodeList.size()) {
			index = Collections.binarySearch(m_nodeList, p_responsibleArea[0]);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_nodeList.size()) {
					index = 0;
				}
			}
			startIndex = index;
			currentPeer = m_nodeList.get(index++);
			while (isNodeInRange(currentPeer, p_responsibleArea[0], p_nodeID, OPEN_INTERVAL)) {
				trees.add(getCIDTree(currentPeer));

				mappings = m_idTable.toArray(currentPeer, currentPeer, false, CLOSED_INTERVAL);
				if (null == allMappings) {
					allMappings = mappings;
				} else {
					oldMappings = allMappings;
					allMappings = new byte[oldMappings.length + mappings.length];
					System.arraycopy(oldMappings, 0, allMappings, 0, oldMappings.length);
					System.arraycopy(mappings, 0, allMappings, oldMappings.length, mappings.length);
				}

				if (index == m_nodeList.size()) {
					index = 0;
				}
				if (index == startIndex) {
					break;
				}
				currentPeer = m_nodeList.get(index++);
			}
		}
		m_dataLock.unlock();
		while (!isOnlySuperpeer()) {
			try {
				System.out.print("** Spreading failed superpeers meta-data to " + m_successor);
				new SendBackupsMessage(m_successor, allMappings, trees).send(m_network);
			} catch (final NetworkException e) {
				// Successor is not available anymore, remove from superpeer array and try next superpeer
				LOGGER.error("successor failed, too");
				m_failureLock.unlock();
				failureHandling(m_successor);
				m_failureLock.lock();
				continue;
			}
			break;
		}
	}

	/**
	 * Spread backups of failed superpeer
	 * @param p_backupSuperpeers
	 *            the current backup superpeers
	 */
	private void spreadBackupsOfThisSuperpeer(final short[] p_backupSuperpeers) {
		short currentPeer;
		short newBackupSuperpeer;
		short lowerBound;
		int index;
		int startIndex;
		byte[] mappings;
		byte[] oldMappings;
		byte[] allMappings = null;
		ArrayList<CIDTreeOptimized> trees;

		trees = new ArrayList<CIDTreeOptimized>();
		m_dataLock.lock();
		lowerBound = m_predecessor;
		if (0 != m_nodeList.size()) {
			index = Collections.binarySearch(m_nodeList, lowerBound);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_nodeList.size()) {
					index = 0;
				}
			}
			startIndex = index;
			currentPeer = m_nodeList.get(index++);
			System.out.print("** spreading data of");
			while (isNodeInRange(currentPeer, lowerBound, m_me, OPEN_INTERVAL)) {
				System.out.print(" " + currentPeer);
				trees.add(getCIDTree(currentPeer));

				mappings = m_idTable.toArray(currentPeer, currentPeer, false, CLOSED_INTERVAL);
				if (null == allMappings) {
					allMappings = mappings;
				} else {
					oldMappings = allMappings;
					allMappings = new byte[oldMappings.length + mappings.length];
					System.arraycopy(oldMappings, 0, allMappings, 0, oldMappings.length);
					System.arraycopy(mappings, 0, allMappings, oldMappings.length, mappings.length);
				}

				if (index == m_nodeList.size()) {
					index = 0;
				}
				if (index == startIndex) {
					break;
				}
				currentPeer = m_nodeList.get(index++);
			}
		}
		m_dataLock.unlock();

		while (!isOnlySuperpeer()) {
			m_overlayLock.lock();
			index = (short) Collections.binarySearch(m_superpeers, (short) (p_backupSuperpeers[2] + 1));
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_superpeers.size()) {
					index = 0;
				}
			}
			newBackupSuperpeer = m_superpeers.get(index);
			m_overlayLock.unlock();
			System.out.print(" to " + newBackupSuperpeer);

			try {
				new SendBackupsMessage(newBackupSuperpeer, allMappings, trees).send(m_network);
			} catch (final NetworkException e) {
				// Superpeer is not available anymore, remove from superpeer array and try next superpeer
				LOGGER.error("new backup superpeer failed, too");
				m_failureLock.unlock();
				failureHandling(newBackupSuperpeer);
				m_failureLock.lock();
				continue;
			}
			break;
		}
	}

	/**
	 * Handles a node failure
	 * @param p_failedNode
	 *            the failed nodes NodeID
	 */
	private void failureHandling(final short p_failedNode) {
		short[] responsibleArea;
		short[] backupSuperpeers;
		short backupPeer;
		short superpeer;
		int i = 0;
		long backupRange;
		boolean existsInZooKeeper = false;
		Iterator<Short> iter;
		ArrayList<long[]> backupRanges;
		CIDTreeOptimized tree;

		boolean promoteOnePeer = false;
		boolean finished = false;

		if (m_failureLock.tryLock()) {
			m_overlayLock.lock();

			// Check if failed node is a superpeer
			if (0 <= Collections.binarySearch(m_superpeers, p_failedNode)) {
				m_overlayLock.unlock();
				System.out.println();
				System.out.println();
				System.out.println("********** ********** Node Failure ********** **********");
				System.out.println("* Failed node was a superpeer, NodeID: " + p_failedNode);
				// Determine new bootstrap if failed node is current one
				if (p_failedNode == m_bootstrap) {
					determineNewBootstrap();
					System.out.println("* " + p_failedNode + " was bootstrap. New bootstrap is " + m_bootstrap);
				}
				// Take over failed nodes peers and CIDTrees if it is this nodes predecessor
				if (p_failedNode == m_predecessor) {
					System.out.println("* " + p_failedNode + " was my predecessor -> taking over all peers and data");
					takeOverPeersAndCIDTrees(m_predecessor);
					promoteOnePeer = true;
				}
				// Send failed nodes CIDTrees to this nodes successor if it is the first node in responsible area
				m_overlayLock.lock();
				responsibleArea = getResponsibleArea(m_me);
				m_overlayLock.unlock();
				if (3 < m_superpeers.size() && getResponsibleSuperpeer((short) (responsibleArea[0] + 1), NO_CHECK) == p_failedNode) {
					System.out.println("* " + p_failedNode + " was in my responsible area -> spreading his data");
					spreadDataOfFailedSuperpeer(p_failedNode, responsibleArea);
				}
				// Send this nodes CIDTrees to new backup node that replaces the failed node
				m_overlayLock.lock();
				backupSuperpeers = getBackupSuperpeers(m_me);
				m_overlayLock.unlock();
				if (3 < m_superpeers.size() && isNodeInRange(p_failedNode, backupSuperpeers[0], backupSuperpeers[2], CLOSED_INTERVAL)) {
					System.out.println("* " + p_failedNode + " was one of my backup nodes -> spreading my data");
					spreadBackupsOfThisSuperpeer(backupSuperpeers);
				}
				// Remove superpeer
				m_overlayLock.lock();
				removeSuperpeer(p_failedNode);
				m_overlayLock.unlock();

				if (promoteOnePeer) {
					// Promote a peer to replace the failed superpeer; for only stabilization thread, only
					System.out.println("* Promoting a peer to superpeer");
					promoteOnePeer(p_failedNode);
				}

				notifyAboutFailure(p_failedNode, true);
				System.out.println("********** ********** *** End **** ********** **********");
				System.out.println("********** ********** ************ ********** **********");
				System.out.println();
				System.out.println();

				m_failureLock.unlock();
			} else if (0 <= Collections.binarySearch(m_peers, p_failedNode)) {
				m_overlayLock.unlock();
				try {
					existsInZooKeeper = ZooKeeperHandler.exists("nodes/peers/" + p_failedNode);
				} catch (final ZooKeeperException e) {
					System.out.println("Could not access ZooKeeper! Assume failed node was a peer.");
					e.printStackTrace();
				}

				if (!existsInZooKeeper) {
					// Failed node was a monitor
					System.out.println();
					System.out.println();
					System.out.println("********** ********** Node Failure ********** **********");
					System.out.println("* Failed node was a monitor, NodeID: " + p_failedNode);
					// Remove peer
					m_overlayLock.lock();
					removePeer(p_failedNode);
					m_overlayLock.unlock();
					System.out.println("* No actions required");
					System.out.println("********** ********** *** End **** ********** **********");
					System.out.println("********** ********** ************ ********** **********");
					System.out.println();
					System.out.println();
				} else {
					// Failed node was a peer
					System.out.println();
					System.out.println();
					System.out.println("********** ********** Node Failure ********** **********");
					System.out.println("* Failed node was a peer, NodeID: " + p_failedNode);

					// Remove peer in meta-data (and replace with new backup node; DUMMY element currently)
					System.out.println("* Removing " + p_failedNode + " from local meta-data");
					m_dataLock.lock();
					iter = m_nodeList.iterator();
					while (iter.hasNext()) {
						tree = getCIDTree(iter.next());
						if (tree != null) {
							tree.removeBackupPeer(p_failedNode, DUMMY);
						}
					}
					tree = getCIDTree(p_failedNode);
					if (tree != null) {
						tree.setStatus(false);
					}
					m_dataLock.unlock();
					while (true) {
						m_overlayLock.lock();
						if (i < m_superpeers.size()) {
							superpeer = m_superpeers.get(i++);
							m_overlayLock.unlock();
						} else {
							m_overlayLock.unlock();
							break;
						}
						// Inform superpeer about failed peer to initialize deletion
						System.out.println("** Informing " + superpeer + " to remove " + p_failedNode + " from meta-data");
						try {
							new NotifyAboutFailedPeerMessage(superpeer, p_failedNode).send(m_network);
						} catch (final NetworkException e) {
							// Superpeer is not available anymore, remove from superpeer array and continue
							LOGGER.error("superpeer failed, too");
							m_failureLock.unlock();
							failureHandling(superpeer);
							m_failureLock.lock();
						}
					}

					// Start recovery
					System.out.println("* Starting recovery for " + p_failedNode);
					while (!finished) {
						finished = true;
						m_dataLock.lock();
						backupRanges = getCIDTree(p_failedNode).getAllBackupRanges();
						m_dataLock.unlock();
						for (i = 0; i < backupRanges.size(); i++) {
							for (int j = 0; j < 3; j++) {
								backupRange = backupRanges.get(i)[0];
								backupPeer = (short) (backupRanges.get(i)[1] >> j * 16);
								// Inform backupPeer to recover all chunks between (i * 1000) and ((i + 1) * 1000 -
								// 1)
								System.out.println("** Informing backup peer " + backupPeer + " to recover chunks" + " from backup range starting with "
										+ backupRange + " from " + p_failedNode);
								/*
								 * try {
								 * new StartRecoveryMessage(backupPeer, p_failedNode, i * 1000).send(m_network);
								 * } catch (final NetworkException e) {
								 * // Backup peer is not available anymore, try next one
								 * continue;
								 * }
								 */
							}
						}
					}

					// Remove peer
					m_overlayLock.lock();
					removePeer(p_failedNode);
					m_overlayLock.unlock();
					notifyAboutFailure(p_failedNode, false);
					System.out.println("********** ********** *** End **** ********** **********");
					System.out.println("********** ********** ************ ********** **********");
					System.out.println();
					System.out.println();

					m_failureLock.unlock();
				}
			}
		}
	}

	/**
	 * Returns the current bootstrap
	 * @return the current bootstrap
	 */
	public short getCurrentBootstrap() {
		short ret = -1;

		try {
			ret = Short.parseShort(new String(ZooKeeperHandler.getData("nodes/bootstrap")));
		} catch (final ZooKeeperException e) {
			LOGGER.error("ERR:Could not access ZooKeeper", e);
		}

		return ret;
	}

	/**
	 * Replaces the current bootstrap with p_nodeID if the failed bootstrap has not been replaced by another superpeer
	 * @param p_nodeID
	 *            the new bootstrap candidate
	 * @return the new bootstrap
	 */
	public short replaceBootstrap(final short p_nodeID) {
		short ret;
		Stat status;
		String entry;

		try {
			status = ZooKeeperHandler.getStatus("nodes/bootstrap");
			entry = new String(ZooKeeperHandler.getData("nodes/bootstrap", status));
			ret = Short.parseShort(entry);
			if (ret == m_bootstrap) {
				ZooKeeperHandler.setData("nodes/bootstrap", ("" + p_nodeID).getBytes(), status.getVersion());
				m_bootstrap = p_nodeID;
			} else {
				m_bootstrap = ret;
			}
		} catch (final ZooKeeperException e) {
			try {
				m_bootstrap = Short.parseShort(new String(ZooKeeperHandler.getData("nodes/bootstrap")));
			} catch (final ZooKeeperException e1) {
				LOGGER.error("ERR:Could not access ZooKeeper", e);
			}
		}

		return m_bootstrap;
	}

	/**
	 * Remove node from /nodes/new/ if listed and add NodeID to /nodes/free/
	 * @param p_failedNode
	 *            the failed node
	 * @param p_isSuperpeer
	 *            whether the failed node was a superpeer or not
	 * @return true if this node reported the failure first
	 */
	public boolean notifyAboutFailure(final short p_failedNode, final boolean p_isSuperpeer) {
		boolean ret = false;
		Stat status;

		try {
			if (!ZooKeeperHandler.exists("nodes/free/" + p_failedNode)) {
				ZooKeeperHandler.create("nodes/free/" + p_failedNode);

				status = ZooKeeperHandler.getStatus("nodes/new/" + p_failedNode);
				if (null != status) {
					ZooKeeperHandler.delete("nodes/new/" + p_failedNode, status.getVersion());
				}
				if (p_isSuperpeer) {
					status = ZooKeeperHandler.getStatus("nodes/superpeers/" + p_failedNode);
					if (null != status) {
						ZooKeeperHandler.delete("nodes/superpeers/" + p_failedNode, status.getVersion());
					}
				} else {
					status = ZooKeeperHandler.getStatus("nodes/peers/" + p_failedNode);
					if (null != status) {
						ZooKeeperHandler.delete("nodes/peers/" + p_failedNode, status.getVersion());
					}
				}

				ret = true;
			}
		} catch (final ZooKeeperException | KeeperException | InterruptedException e) {
			LOGGER.error("ERR:Could not access ZooKeeper", e);
		}

		return ret;
	}

	/**
	 * Print superpeer overlay
	 */
	public void printSuperpeerOverlay() {
		boolean printMe;
		String str;
		short superpeer;
		short peer;

		printMe = NodeID.getRole().equals(Role.SUPERPEER);
		str = "Superpeer overlay:";

		m_overlayLock.lock();
		for (int i = 0; i < m_superpeers.size(); i++) {
			superpeer = m_superpeers.get(i);
			if (printMe && superpeer > m_me) {
				str += " ’" + m_me + "’";
				printMe = false;
			}
			str += " " + superpeer;
		}
		if (printMe) {
			str += " ’" + m_me + "’";
		}
		str += "\n Peers:";
		for (int i = 0; i < m_peers.size(); i++) {
			peer = m_peers.get(i);
			str += " " + peer;
		}
		System.out.println(str);
		m_overlayLock.unlock();
	}

	// Classes
	/**
	 * Stabilizes superpeer overlay
	 * @author Kevin Beineke
	 *         03.06.2013
	 */
	private class SOWorker implements Runnable {

		// Attributes
		private int m_next;
		private int m_counter;

		// Constructors
		/**
		 * Creates an instance of Worker
		 */
		public SOWorker() {
			m_next = 0;
			m_counter = 0;
		}

		/**
		 * When an object implementing interface <code>Runnable</code> is used
		 * to create a thread, starting the thread causes the object's <code>run</code> method to be called in that
		 * separately executing
		 * thread.
		 * <p>
		 * The general contract of the method <code>run</code> is that it may take any action whatsoever.
		 * @see java.lang.Thread#run()
		 */
		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(m_sleepInterval * 1000);
				} catch (final InterruptedException e) {
					LOGGER.trace("Got an interrupt while sleeping");
					break;
				}

				performStabilization();
				for (int i = 0; i < m_numberOfSuperpeers / 300 || i < 1; i++) {
					fixSuperpeers();
				}
				promotePeerIfNecessary();

				if (!isOnlySuperpeer()) {
					backupMaintenance();
					takeOverPeersAndCIDTrees(m_me);
				}
				pingPeers();
			}
		}

		/**
		 * Performs stabilization protocol
		 * @note without disappearing superpeers this method does not do anything important; All the setup is done with
		 *       joining
		 */
		private void performStabilization() {
			while (-1 != m_predecessor) {
				try {
					new NotifyAboutNewSuccessorMessage(m_predecessor, m_me).send(m_network);
				} catch (final NetworkException e) {
					// Predecessor is not available anymore, determine new predecessor and repeat it
					failureHandling(m_predecessor);
					continue;
				}
				break;
			}

			while (-1 != m_successor) {
				try {
					new NotifyAboutNewPredecessorMessage(m_successor, m_me).send(m_network);
				} catch (final NetworkException e) {
					// Predecessor is not available anymore, determine new predecessor and repeat it
					failureHandling(m_successor);
					continue;
				}
				break;
			}
		}

		/**
		 * Fixes the superpeer array
		 * @note is called periodically
		 */
		private void fixSuperpeers() {
			boolean stop = false;
			short contactSuperpeer = -1;
			short possibleSuccessor = -1;
			short hisSuccessor;

			AskAboutSuccessorRequest request;
			AskAboutSuccessorResponse response;

			if (1 < m_superpeers.size()) {
				m_overlayLock.lock();
				if (m_next + 1 < m_superpeers.size()) {
					contactSuperpeer = m_superpeers.get(m_next);
					possibleSuccessor = m_superpeers.get(m_next + 1);
				} else if (m_next + 1 == m_superpeers.size()) {
					contactSuperpeer = m_superpeers.get(m_next);
					possibleSuccessor = m_superpeers.get(0);
				} else {
					m_next = 0;
					fixSuperpeers();
					stop = true;
				}

				if (!stop && contactSuperpeer == m_predecessor) {
					m_next++;
					fixSuperpeers();
					stop = true;
				}

				if (!stop) {
					m_next++;
					m_overlayLock.unlock();

					request = new AskAboutSuccessorRequest(contactSuperpeer);
					Contract.checkNotNull(request);
					try {
						request.sendSync(m_network);
					} catch (final NetworkException e) {
						// Superpeer is not available anymore, remove from superpeer array and try next superpeer
						failureHandling(contactSuperpeer);
						m_next--;
						fixSuperpeers();
						return;
					}
					response = request.getResponse(AskAboutSuccessorResponse.class);
					Contract.checkNotNull(response);

					hisSuccessor = response.getSuccessor();

					if (hisSuccessor != possibleSuccessor && -1 != hisSuccessor) {
						m_overlayLock.lock();
						insertSuperpeer(hisSuccessor);
						m_overlayLock.unlock();
					}
				} else {
					m_overlayLock.unlock();
				}
			}
		}

		/**
		 * Pings all peers and sends current superpeer overlay
		 * @note is called periodically
		 */
		private void pingPeers() {
			short peer;
			int i = 0;

			if (null != m_peers) {
				while (true) {
					m_overlayLock.lock();
					if (i < m_peers.size()) {
						peer = m_peers.get(i++);
						m_overlayLock.unlock();
					} else {
						m_overlayLock.unlock();
						break;
					}
					try {
						new SendSuperpeersMessage(peer, m_superpeers).send(m_network);
					} catch (final NetworkException e) {
						// Peer is not available anymore, remove it from peer array
						failureHandling(peer);
					}
				}
			}
		}

		/**
		 * Copy all unsigned superpeers in one int array
		 * @return array with all superpeers
		 */
		private int[] copyAllSuperpeersInArray() {
			int j = 0;
			int me;
			int superpeer;
			int[] allSuperpeers;
			boolean found = false;

			me = m_me & 0xFFFF;

			allSuperpeers = new int[m_superpeers.size() + 1];
			for (int i = 0; i < m_superpeers.size(); i++) {
				superpeer = m_superpeers.get(i).intValue() & 0xFFFF;
				if (!found && me < superpeer) {
					allSuperpeers[j++] = me;
					found = true;
				}
				allSuperpeers[j++] = superpeer;
			}
			if (!found) {
				allSuperpeers[j] = me;
			}

			return allSuperpeers;
		}

		/**
		 * Promote a peer if necessary
		 * @note is called periodically
		 */
		private void promotePeerIfNecessary() {
			short superpeer = -1;
			int biggestGap = 0;
			int currentGap;
			int index = 0;
			int[] allSuperpeers;

			if (!overlayIsStable()) {
				if (300 == m_counter || 300 > m_numberOfSuperpeers && m_numberOfSuperpeers <= m_counter && 30 <= m_counter) {
					System.out.println();
					System.out.println();
					System.out.println("********** ********** Promoting Peer ********** **********");
					if (isOnlySuperpeer()) {
						promoteOnePeer((short) ((m_me & 0xFFFF) + 32767));
					} else {
						m_overlayLock.lock();
						allSuperpeers = copyAllSuperpeersInArray();
						m_overlayLock.unlock();

						while (index < allSuperpeers.length - 1) {
							currentGap = allSuperpeers[index + 1] - allSuperpeers[index];
							if (biggestGap < currentGap) {
								biggestGap = currentGap;
								superpeer = (short) allSuperpeers[index + 1];
							}
							index++;
						}
						currentGap = allSuperpeers[0] + 65535 - allSuperpeers[index];
						if (biggestGap < currentGap) {
							biggestGap = currentGap;
							superpeer = (short) allSuperpeers[0];
						}

						System.out.println(superpeer + " has privilege");
						if (m_me == superpeer) {
							promoteOnePeer((short) ((m_me & 0xFFFF) - biggestGap / 2));
						}
					}
					m_counter = 0;
					System.out.println("********** ********** ***** End **** ********** **********");
					System.out.println();
					System.out.println();
				} else {
					m_counter++;
				}
			} else {
				m_counter = 0;
			}
		}

		/**
		 * Maintain backup replication
		 * @note is called periodically
		 */
		private void backupMaintenance() {
			short[] responsibleArea;

			m_overlayLock.lock();
			responsibleArea = getResponsibleArea(m_me);
			m_overlayLock.unlock();
			System.out.println("  Responsible area: " + responsibleArea[0] + ", " + responsibleArea[1]);

			gatherBackups(responsibleArea);
			deleteUnnecessaryBackups(responsibleArea);
		}

		/**
		 * Deletes all CIDTrees that are not in the responsible area
		 * @param p_responsibleArea
		 *            the responsible area
		 * @note assumes m_overlayLock has been locked
		 * @note is called periodically
		 */
		private void gatherBackups(final short[] p_responsibleArea) {
			int index;
			int startIndex;
			short currentSuperpeer;
			short oldSuperpeer;
			short currentPeer;

			CIDTreeOptimized tree;

			ArrayList<Short> peers;
			ArrayList<CIDTreeOptimized> trees;

			AskAboutBackupsRequest request;
			AskAboutBackupsResponse response;

			if (!isOnlySuperpeer()) {
				peers = new ArrayList<Short>();
				if (3 >= m_superpeers.size()) {
					oldSuperpeer = m_me;
					currentSuperpeer = m_successor;
				} else {
					oldSuperpeer = p_responsibleArea[0];
					currentSuperpeer = getResponsibleSuperpeer((short) (p_responsibleArea[0] + 1), false);
				}
				while (-1 != currentSuperpeer) {
					m_dataLock.lock();
					if (0 != m_nodeList.size()) {
						index = Collections.binarySearch(m_nodeList, oldSuperpeer);
						if (0 > index) {
							index = index * -1 - 1;
							if (index == m_nodeList.size()) {
								index = 0;
							}
						}
						startIndex = index;
						currentPeer = m_nodeList.get(index++);
						while (isNodeInRange(currentPeer, oldSuperpeer, currentSuperpeer, OPEN_INTERVAL)) {
							peers.add(Collections.binarySearch(peers, currentPeer) * -1 - 1, currentPeer);
							if (index == m_nodeList.size()) {
								index = 0;
							}
							if (index == startIndex) {
								break;
							}
							currentPeer = m_nodeList.get(index++);
						}
					}
					m_dataLock.unlock();
					request = new AskAboutBackupsRequest(currentSuperpeer, peers);
					Contract.checkNotNull(request);
					try {
						request.sendSync(m_network);
					} catch (final NetworkException e) {
						// CurrentSuperpeer is not available anymore, remove it from superpeer array
						failureHandling(currentSuperpeer);
						currentSuperpeer = getResponsibleSuperpeer((short) (oldSuperpeer + 1), false);
						peers.clear();
						continue;
					}
					response = request.getResponse(AskAboutBackupsResponse.class);
					Contract.checkNotNull(response);

					trees = response.getBackups();
					m_dataLock.lock();
					for (int i = 0; i < trees.size(); i++) {
						tree = trees.get(i);
						addCIDTree(tree.getCreator(), tree);
					}
					m_dataLock.unlock();

					m_mappingLock.lock();
					m_idTable.putAll(response.getMappings());
					m_mappingLock.unlock();

					peers.clear();

					if (currentSuperpeer == m_predecessor) {
						break;
					}
					oldSuperpeer = currentSuperpeer;
					currentSuperpeer = getResponsibleSuperpeer((short) (currentSuperpeer + 1), false);
				}
			}
		}

		/**
		 * Deletes all CIDTrees that are not in the responsible area
		 * @param p_responsibleArea
		 *            the responsible area
		 * @note assumes m_overlayLock has been locked
		 * @note is called periodically
		 */
		private void deleteUnnecessaryBackups(final short[] p_responsibleArea) {
			short currentPeer;
			int index;

			m_dataLock.lock();
			if (0 != m_nodeList.size()) {
				index = Collections.binarySearch(m_nodeList, p_responsibleArea[1]);
				if (0 > index) {
					index = index * -1 - 1;
					if (index == m_nodeList.size()) {
						index = 0;
					}
				}
				currentPeer = m_nodeList.get(index);
				while (isNodeInRange(currentPeer, p_responsibleArea[1], p_responsibleArea[0], OPEN_INTERVAL) && p_responsibleArea[0] != p_responsibleArea[1]) {
					deleteCIDTree(currentPeer);
					m_idTable.remove(currentPeer);

					if (index == m_nodeList.size()) {
						index = 0;
					}
					if (0 == m_nodeList.size()) {
						break;
					}
					currentPeer = m_nodeList.get(index);
				}
			}
			m_dataLock.unlock();
		}
	}

	/**
	 * Stores locations
	 * @author Kevin Beineke
	 *         03.09.2013
	 */
	public static final class Locations {

		// Attributes
		private short m_primaryPeer;
		private short[] m_backupPeers;
		private long[] m_range;

		// Constructors
		/**
		 * Creates an instance of Locations
		 * @param p_primaryPeer
		 *            the primary peer
		 * @param p_backupPeers
		 *            the backup peers
		 * @param p_range
		 *            the range's beginning and ending
		 */
		public Locations(final short p_primaryPeer, final short[] p_backupPeers, final long[] p_range) {
			super();

			m_primaryPeer = p_primaryPeer;
			m_backupPeers = p_backupPeers;
			m_range = p_range;
		}

		/**
		 * Creates an instance of Locations
		 * @param p_primaryAndBackupPeers
		 *            the locations in long representation
		 */
		public Locations(final long p_primaryAndBackupPeers) {
			this(p_primaryAndBackupPeers, null);
		}

		/**
		 * Creates an instance of Locations
		 * @param p_primaryAndBackupPeers
		 *            the locations in long representation
		 * @param p_range
		 *            the corresponding range
		 */
		public Locations(final long p_primaryAndBackupPeers, final long[] p_range) {
			this((short) p_primaryAndBackupPeers, new short[] {(short) ((p_primaryAndBackupPeers & 0x00000000FFFF0000L) >> 16),
					(short) ((p_primaryAndBackupPeers & 0x0000FFFF00000000L) >> 32), (short) ((p_primaryAndBackupPeers & 0xFFFF000000000000L) >> 48)}, p_range);
		}

		// Getter
		/**
		 * Get primary peer
		 * @return the primary peer
		 */
		public short getPrimaryPeer() {
			return m_primaryPeer;
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

		/**
		 * Get range
		 * @return the beginning and ending of range
		 */
		public long[] getRange() {
			return m_range;
		}

		/**
		 * Get the start LID
		 * @return the start LID
		 */
		public long getStartID() {
			return m_range[0];
		}

		/**
		 * Get the end LID
		 * @return the end LID
		 */
		public long getEndID() {
			return m_range[1];
		}

		// Setter
		/**
		 * Set primary peer
		 * @param p_primaryPeer
		 *            the primary peer
		 */
		public void setPrimaryPeer(final short p_primaryPeer) {
			m_primaryPeer = p_primaryPeer;
		}

		/**
		 * Set backup peers
		 * @param p_backupPeers
		 *            the backup peers
		 */
		public void setBackupPeers(final short[] p_backupPeers) {
			m_backupPeers = p_backupPeers;
		}

		// Methods
		/**
		 * Convert this instance to long
		 * @return the long representation
		 */
		public long convertToLong() {
			long ret;
			if (null != m_backupPeers) {
				if (m_backupPeers.length == 3) {
					ret =
							((m_backupPeers[2] & 0x000000000000FFFFL) << 48) + ((m_backupPeers[1] & 0x000000000000FFFFL) << 32)
							+ ((m_backupPeers[0] & 0x000000000000FFFFL) << 16) + (m_primaryPeer & 0x0000FFFF);
				} else if (m_backupPeers.length == 2) {
					ret = ((m_backupPeers[1] & 0x000000000000FFFFL) << 32) + ((m_backupPeers[0] & 0x000000000000FFFFL) << 16) + (m_primaryPeer & 0x0000FFFF);
				} else {
					ret = ((m_backupPeers[0] & 0x000000000000FFFFL) << 16) + (m_primaryPeer & 0x0000FFFF);
				}
			} else {
				ret = m_primaryPeer & 0x0000FFFF;
			}
			return ret;
		}

		/**
		 * Prints the locations
		 * @return String interpretation of locations
		 */
		@Override
		public String toString() {
			String ret;

			if (null != m_backupPeers) {
				if (m_backupPeers.length == 3) {
					if (m_backupPeers[0] == -1) {
						ret = m_primaryPeer + ", backup peers unknown (ask " + m_primaryPeer + ")";
					} else {
						ret = m_primaryPeer + ", [" + m_backupPeers[0] + ", " + m_backupPeers[1] + ", " + m_backupPeers[2] + "]";
					}
				} else if (m_backupPeers.length == 2) {
					ret = m_primaryPeer + ", [" + m_backupPeers[0] + ", " + m_backupPeers[1] + "]";
				} else {
					ret = m_primaryPeer + ", [" + m_backupPeers[0] + "]";
				}
			} else {
				ret = m_primaryPeer + ", no backup peers";
			}
			if (null != m_range) {
				ret += ", (" + m_range[0] + ", " + m_range[1] + ")";
			}
			return ret;
		}
	}

}

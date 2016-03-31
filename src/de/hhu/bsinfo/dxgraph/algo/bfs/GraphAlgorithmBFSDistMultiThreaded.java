package de.hhu.bsinfo.dxgraph.algo.bfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;

import de.hhu.bsinfo.dxcompute.coord.messages.CoordinatorMessages;
import de.hhu.bsinfo.dxcompute.coord.messages.MasterSyncBarrierBroadcastMessage;
import de.hhu.bsinfo.dxcompute.coord.messages.MasterSyncBarrierReleaseMessage;
import de.hhu.bsinfo.dxcompute.coord.messages.SlaveSyncBarrierSignOnMessage;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.ConcurrentBitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.FrontierList;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSMessages;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.VerticesForNextFrontierMessage;
import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxgraph.load.GraphLoaderResultDelegate;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.util.NodeID;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.locks.SpinLock;

public class GraphAlgorithmBFSDistMultiThreaded extends GraphAlgorithm implements MessageReceiver {

	private static boolean ms_setupOnceDone = false;
	
	private int m_vertexBatchCountPerThread = 100;
	private int m_vertexMessageBatchCount = 100;
	
	private int m_numSlaves = 0; // 0 is slave, > 0 is master
	private int m_broadcastIntervalMs = 2000;
	private int m_barrierIdentifer = -1;
	
	// master
	private ArrayList<Short> m_slavesSynced = new ArrayList<Short>();
	private Lock m_slavesSyncedMutex = new SpinLock();
	
	// slave
	private volatile short m_masterNodeID = NodeID.INVALID_ID;
	private volatile boolean m_masterBarrierReleased = false;
	
	private FrontierList m_curFrontier = null;
	private FrontierList m_nextFrontier = null;
	
	private BFSThread[] m_threads = null;
	
	public GraphAlgorithmBFSDistMultiThreaded(final int p_vertexBatchCountPerThread, final int p_vertexMessageBatchCount, 
			final int p_numSlaves, final int p_barrierBroadcastIntervalMs, final int p_barrierIdentifier, 
			final int p_threadCount, final GraphLoaderResultDelegate p_loaderResultsDelegate, final long... p_entryNodes)
	{
		super(p_loaderResultsDelegate, p_entryNodes);
		m_vertexBatchCountPerThread = p_vertexBatchCountPerThread;		
		m_vertexMessageBatchCount = p_vertexMessageBatchCount;
		m_numSlaves = p_numSlaves;
		m_broadcastIntervalMs = p_barrierBroadcastIntervalMs;
		m_barrierIdentifer = p_barrierIdentifier;
		m_threads = new BFSThread[p_threadCount];
	}
	
	@Override
	public void onIncomingMessage(AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == BFSMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE:
					incomingVerticesForNextFrontierMessage((VerticesForNextFrontierMessage) p_message);
					break;
				default:
					break;
				}
			} else if (p_message.getType() == CoordinatorMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case CoordinatorMessages.SUBTYPE_SLAVE_SYNC_BARRIER_SIGN_ON:
					incomingSlaveSyncBarrierSignOn((SlaveSyncBarrierSignOnMessage) p_message);
					break;
				case CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_BROADCAST:
					incomingMasterSyncBarrierBroadcast((MasterSyncBarrierBroadcastMessage) p_message);
					break;
				case CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_RELEASE:
					incomingMasterSyncBarrierRelease((MasterSyncBarrierReleaseMessage) p_message);
					break;
				default:
					break;
				}
			}
		}
	}
	
	@Override
	protected boolean setup() {
		// register network messages once
		if (!ms_setupOnceDone)
		{
			m_networkService.registerMessageType(BFSMessages.TYPE, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE, VerticesForNextFrontierMessage.class);
			m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_SLAVE_SYNC_BARRIER_SIGN_ON, SlaveSyncBarrierSignOnMessage.class);
			m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_BROADCAST, MasterSyncBarrierBroadcastMessage.class);
			m_networkService.registerMessageType(CoordinatorMessages.TYPE, CoordinatorMessages.SUBTYPE_MASTER_SYNC_BARRIER_RELEASE, MasterSyncBarrierReleaseMessage.class);
			ms_setupOnceDone = true;
		}
		
		m_networkService.registerReceiver(VerticesForNextFrontierMessage.class, this);
		
		if (m_numSlaves > 0) {
			// master
			m_networkService.registerReceiver(SlaveSyncBarrierSignOnMessage.class, this);
		} else {
			// slaves
			m_networkService.registerReceiver(MasterSyncBarrierBroadcastMessage.class, this);
			m_networkService.registerReceiver(MasterSyncBarrierReleaseMessage.class, this);
		}
		
		return true;
	}
	
	@Override
	protected boolean execute(long[] p_entryNodes) 
	{
		m_loggerService.info(getClass(), "Starting " + m_threads.length + " BFS Threads...");
		
		long totalVertexCount = getGraphLoaderResultDelegate().getTotalVertexCount();
		m_curFrontier = new ConcurrentBitVector(totalVertexCount);
		m_nextFrontier = new ConcurrentBitVector(totalVertexCount);

		for (int i = 0; i < m_threads.length; i++) {
			m_threads[i] = new BFSThread(i, m_loggerService, m_chunkService, m_networkService, 
					m_bootService.getNodeID(), m_vertexBatchCountPerThread, m_vertexMessageBatchCount, m_curFrontier, m_nextFrontier);
			m_threads[i].start();
		}
		
		// execute a full BFS for each node
		int bfsIteration = 0;
		for (long entryNode : p_entryNodes)
		{			
			// master: wait for all slaves to sign on
			if (m_numSlaves > 0) {
				// master
				coordinateMaster();
			} else {
				// slave
				coordinateSlaves();
			}

			short entryNodeId = ChunkID.getCreatorID(entryNode);
			if (entryNodeId != m_bootService.getNodeID())
			{
				// send to remote 
				VerticesForNextFrontierMessage msg = new VerticesForNextFrontierMessage(entryNodeId, 1);
				msg.getVertexIDBuffer()[0] = entryNode;
				msg.setNumVerticesInBatch(1);
				
				if (m_networkService.sendMessage(msg) != NetworkErrorCodes.SUCCESS) {
					m_loggerService.error(getClass(), "Sending entry vertex " + Long.toHexString(entryNode) + " to node " + 
								Integer.toHexString(entryNodeId) + " failed");
					continue; 
				}
			}
			else
			{
				m_nextFrontier.pushBack(ChunkID.getLocalID(entryNode));
			}
			
			// master: wait for all slaves to finish this
			if (m_numSlaves > 0) {
				// master
				coordinateMaster();
			} else {
				// slave
				coordinateSlaves();
			}
			
			// frontier swap for everything, because we must put our entry node in next
			FrontierList tmp = m_curFrontier;
			m_curFrontier = m_nextFrontier;
			m_nextFrontier = tmp;
			m_nextFrontier.reset();
			
			// also swap them for threads!
			for (int t = 0; t < m_threads.length; t++) {	
				m_threads[t].triggerFrontierSwap();
			}
			
			m_loggerService.info(getClass(), "Executing BFS iteration (" + bfsIteration + ") with root node " + Long.toHexString(entryNode));
			Pair<Long, Integer> results = runBFS(bfsIteration);
			m_loggerService.info(getClass(), "Results BFS iteration (" + bfsIteration + ") for root node " + Long.toHexString(entryNode) + ": Iteration depth " + results.second() + ", total visited vertices " + results.first());
		
			bfsIteration++;
		}
		
		for (int i = 0; i < m_threads.length; i++) {
			m_threads[i].exitThread();
		}
		
		m_loggerService.info(getClass(), "Joining BFS threads...");
		for (int i = 0; i < m_threads.length; i++) {
			try {
				m_threads[i].join();
			} catch (InterruptedException e) {
			}
		}
		
		// one last wait for everyone 
		if (m_numSlaves > 0) {
			// master
			coordinateMaster();
		} else {
			// slave
			coordinateSlaves();
		}

		m_loggerService.info(getClass(), "Finished BFS.");
		return true;
	}
	
	private static class BFSThread extends Thread {
		
		private int m_id = -1;
		
		private LoggerService m_loggerService = null;
		private ChunkService m_chunkService = null;
		private NetworkService m_networkService = null;
		private short m_nodeId = -1;
		
		private Vertex2[] m_vertexBatch = null;
		private int m_messageBatchSize = -1;
		private HashMap<Short, VerticesForNextFrontierMessage> m_remoteMessages = new HashMap<Short, VerticesForNextFrontierMessage>();
		
		private int m_currentIterationLevel = 0;		
		private FrontierList m_curFrontier = null;
		private FrontierList m_nextFrontier = null;
		
		private volatile int m_visitedCounterRun = 0;
		private volatile boolean m_iterationLevelDone = true;
		private volatile boolean m_exitThread = false;
		
		public BFSThread(
				final int p_id, 
				final LoggerService p_loggerService, 
				final ChunkService p_chunkService,
				final NetworkService p_networkService,
				final short p_nodeId, 
				final int p_vertexBatchSize,
				final int p_vertexMessageBatchSize,
				final FrontierList p_curFrontierShared,
				final FrontierList p_nextFrontierShared)
		{
			super("BFSThread-" + p_id);
			
			m_id = p_id;
			
			m_loggerService = p_loggerService;
			m_chunkService = p_chunkService;
			m_nodeId = p_nodeId;
			
			m_vertexBatch = new Vertex2[p_vertexBatchSize];
			for (int i = 0; i < m_vertexBatch.length; i++) {
				m_vertexBatch[i] = new Vertex2(ChunkID.INVALID_ID);
			}
			m_messageBatchSize = p_vertexMessageBatchSize;
			
			m_curFrontier = p_curFrontierShared;
			m_nextFrontier = p_nextFrontierShared;
		}
		
		public void setCurrentBFSIterationLevel(final int p_iterationLevel) {
			m_currentIterationLevel = p_iterationLevel;
		}
		
		public void triggerNextIteration() {
			m_visitedCounterRun = 0;
			m_iterationLevelDone = false;
		}
		
		public boolean isIterationLevelDone() {
			return m_iterationLevelDone;
		}
		
		public int getVisitedCountLastRun() {
			return m_visitedCounterRun;
		}
		
		public void triggerFrontierSwap() {
			FrontierList tmp = m_curFrontier;
			m_curFrontier = m_nextFrontier;
			m_nextFrontier = tmp;
		}
		
		public void exitThread() {
			m_exitThread = true;
		}
		
		@Override
		public void run() 
		{
			while (true)
			{
				while (m_iterationLevelDone)
				{						
					if (m_exitThread)
						return;
					
					Thread.yield();
				}
				
				int validVertsInBatch = 0;
				for (int i = 0; i < m_vertexBatch.length; i++) {
					long tmp = m_curFrontier.popFront();
					if (tmp != -1)
					{
						m_vertexBatch[i].setID(ChunkID.getChunkID(m_nodeId, tmp));
						validVertsInBatch++;
					}
					else
					{
						if (validVertsInBatch == 0) {
							break;
						}
						m_vertexBatch[i].setID(ChunkID.INVALID_ID);
					}
				}
				
				if (validVertsInBatch == 0) {
					m_iterationLevelDone = true;
					continue;
				}
				
				int gett = m_chunkService.get(m_vertexBatch, 0, validVertsInBatch);
				if (gett != validVertsInBatch)
				{
					m_loggerService.error(getClass(), "Getting vertices in BFS Thread " + m_id + " failed: " + gett + " != " + validVertsInBatch);
					return;
				}
				
				int writeBackCount = 0;
				for (int i = 0; i < validVertsInBatch; i++) {
					// check first if visited
					Vertex2 vertex = m_vertexBatch[i];
					
					// skip vertices that were already marked invalid before
					if (vertex.getID() == ChunkID.INVALID_ID) {
						continue;
					}
					
					if (vertex.getUserData() != m_currentIterationLevel) {							
						writeBackCount++;
						// set depth level
						vertex.setUserData(m_currentIterationLevel);
						m_visitedCounterRun++;
						long[] neighbours = vertex.getNeighbours();
						
						for (long neighbour : neighbours) {	
							// sort by remote and local vertices
							short creatorId = ChunkID.getCreatorID(neighbour);
							if (creatorId != m_nodeId) {
								// go remote, fill message buffers until they are full -> send
								VerticesForNextFrontierMessage msg = m_remoteMessages.get(creatorId);
								if (msg == null) {
									msg = new VerticesForNextFrontierMessage(creatorId, m_messageBatchSize);
								}
								
								if (msg.getNumVerticesInBatch() == msg.getBatchSize()) {
									if (m_networkService.sendMessage(msg) != NetworkErrorCodes.SUCCESS) {
										m_loggerService.error(getClass(), "Sending vertex message to node " + Integer.toHexString(creatorId) + " failed");
										return; 
									}
									
									msg.setNumVerticesInBatch(0);
								}
							} else {
								m_nextFrontier.pushBack(ChunkID.getLocalID(neighbour));
							}
						}
					} else {
						// already visited, don't have to put back to storage
						vertex.setID(ChunkID.INVALID_ID);
					}
				}
				
				// write back changes 
				int put = m_chunkService.put(ChunkLockOperation.NO_LOCK_OPERATION, m_vertexBatch, 0, validVertsInBatch);
				if (put != writeBackCount)
				{
					m_loggerService.error(getClass(), "Putting vertices in BFS Thread " + m_id + " failed: " + put + " != " + writeBackCount);
					return; 
				}
			}
		}
	}
	
	private Pair<Long, Integer> runBFS(final int p_currentBFSIteration)
	{
		long visitedCounter = 0;
		int curIterationLevel = 0;
		
		// make sure the threads are marking the vertices with different values on every iteration
		for (int t = 0; t < m_threads.length; t++) {	
			m_threads[t].setCurrentBFSIterationLevel(p_currentBFSIteration);
		}
		
		while (true)
		{
			m_loggerService.debug(getClass(), "Iteration level " + curIterationLevel + " cur frontier size: " + m_curFrontier.size());
			
			
			// kick off threads with current frontier
			for (int t = 0; t < m_threads.length; t++) {	
				m_threads[t].triggerNextIteration();
			}
			
			// join forked threads
			int i = 0;
			long tmpVisitedCounter = 0;
			while (i < m_threads.length) {
				if (!m_threads[i].isIterationLevelDone()) {
					Thread.yield();
					continue;
				}
				tmpVisitedCounter += m_threads[i].getVisitedCountLastRun();
				i++;
			}
			
			visitedCounter += tmpVisitedCounter;
		
		
			m_loggerService.debug(getClass(), "Finished iteration of level " + curIterationLevel);
			
			// swap buffers at the end of the round when iteration level done
			FrontierList tmp = m_curFrontier;
			m_curFrontier = m_nextFrontier;
			m_nextFrontier = tmp;
			m_nextFrontier.reset();
			
			// also swap them for threads!
			for (int t = 0; t < m_threads.length; t++) {	
				m_threads[t].triggerFrontierSwap();
			}
			
			if (m_curFrontier.isEmpty())
			{
				break;
			}
			
			curIterationLevel++;
		}
		
		return new Pair<Long, Integer>(visitedCounter, curIterationLevel);
	}
	
	// TODO this is ugly, just copy/pasting stuff from the syncBarrierSlave/master classes
	// ...try to create something re-usable
	private boolean coordinateMaster() {
		
		m_loggerService.info(getClass(), "Broadcasting (every " + m_broadcastIntervalMs + "ms) and waiting until " + m_numSlaves + " slaves have signed on...");
		
		// wait until all slaves have signed on
		while (m_slavesSynced.size() != m_numSlaves)
		{
			// broadcast to all peers, which are potential slaves
			List<Short> peers = m_bootService.getAvailablePeerNodeIDs();
			for (short peer : peers)
			{
				// don't send to ourselves
				if (peer != m_bootService.getNodeID())
				{
					MasterSyncBarrierBroadcastMessage message = new MasterSyncBarrierBroadcastMessage(peer, m_barrierIdentifer);
					NetworkErrorCodes error = m_networkService.sendMessage(message);
					if (error != NetworkErrorCodes.SUCCESS) {
						m_loggerService.error(getClass(), "Sending broadcast message to peer " + peer + " failed: " + error);
					} 
				}
			}

			try {
				Thread.sleep(m_broadcastIntervalMs);
			} catch (InterruptedException e) {
			}		
		}
		
		m_loggerService.info(getClass(), m_numSlaves + " slaves have signed on.");
		
		// release barrier
		for (short slavePeerID : m_slavesSynced)
		{
			m_loggerService.debug(getClass(), "Releasing slave " + slavePeerID);
			MasterSyncBarrierReleaseMessage message = new MasterSyncBarrierReleaseMessage(slavePeerID, m_barrierIdentifer);
			NetworkErrorCodes error = m_networkService.sendMessage(message);
			if (error != NetworkErrorCodes.SUCCESS) {
				m_loggerService.error(getClass(), "Sending release to " + slavePeerID + " failed: " + error);
			} 
		}
		
		m_loggerService.info(getClass(), "Barrier releaseed.");
		
		return true;
	}
	
	private boolean coordinateSlaves() {
		
		m_loggerService.info(getClass(), "Waiting to receive master broadcast...");
		
		m_masterBarrierReleased = false;
		
		// wait until we got a broadcast by the master
		while (m_masterNodeID == NodeID.INVALID_ID)
		{
			Thread.yield();
		}
		
		m_loggerService.info(getClass(), "Waiting for master " + m_masterNodeID + " to release barrier...");
		
		while (!m_masterBarrierReleased)
		{
			Thread.yield();
		}

		m_loggerService.info(getClass(), "Master barrier released.");
		
		return true;
	}
	
	private void incomingVerticesForNextFrontierMessage(final VerticesForNextFrontierMessage p_message) {
		long[] vertexIds = p_message.getVertexIDBuffer();
		for (int i = 0; i < p_message.getNumVerticesInBatch(); i++) {
			m_nextFrontier.pushBack(vertexIds[i]);
		}
	}
	
	private void incomingSlaveSyncBarrierSignOn(final SlaveSyncBarrierSignOnMessage p_message) {
		// from different sync call
		if (p_message.getSyncToken() != m_barrierIdentifer) {
			return;
		}
		
		m_slavesSyncedMutex.lock();
		
		// avoid dupes
		if (!m_slavesSynced.contains(p_message.getSource())) {
			m_slavesSynced.add(p_message.getSource());
		}
		
		m_slavesSyncedMutex.unlock();
		
		m_loggerService.debug(getClass(), "Slave " + p_message.getSource() + " has signed on.");
	}
	
	/**
	 * Handle incoming MasterSyncBarrierBroadcastMessage.
	 * @param p_message Message to handle.
	 */
	private void incomingMasterSyncBarrierBroadcast(final MasterSyncBarrierBroadcastMessage p_message) {
		m_loggerService.debug(getClass(), "Got master broadcast from " + p_message.getSource());
		m_masterNodeID = p_message.getSource();
		
		// reply with sign on
		SlaveSyncBarrierSignOnMessage message = new SlaveSyncBarrierSignOnMessage(m_masterNodeID, m_barrierIdentifer);
		NetworkErrorCodes error = m_networkService.sendMessage(message);
		if (error != NetworkErrorCodes.SUCCESS) {
			m_loggerService.error(getClass(), "Sending sign on to " + m_masterNodeID + " failed: " + error);
		} 
	}
	
	/**
	 * Handle incoming MasterSyncBarrierReleaseMessage.
	 * @param p_message Message to handle.
	 */
	private void incomingMasterSyncBarrierRelease(final MasterSyncBarrierReleaseMessage p_message) {
		m_masterBarrierReleased = true;
	}
}

package de.hhu.bsinfo.dxgraph.algo.bfs;

import de.hhu.bsinfo.dxcompute.coord.BarrierMaster;
import de.hhu.bsinfo.dxcompute.coord.BarrierSlave;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.ConcurrentBitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.FrontierList;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSMessages;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.SlaveRunning;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.VerticesForNextFrontierMessage;
import de.hhu.bsinfo.dxgraph.load.GraphLoaderResultDelegate;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.Pair;

public class GraphAlgorithmBFSDistMultiThreaded extends GraphAlgorithm implements MessageReceiver {

	private int m_vertexBatchCountPerThread = 100;
	private int m_vertexMessageBatchCount = 100;
	
	private int m_numSlaves = 0; // 0 is slave, > 0 is master
	private int m_broadcastIntervalMs = 2000;
	private int m_barrierIdentifer = -1;
	
	private FrontierList m_curFrontier = null;
	private FrontierList m_nextFrontier = null;
	
	private BFSThreadDist[] m_threads = null;
	
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
		m_threads = new BFSThreadDist[p_threadCount];
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
			}
		}
	}
	
	@Override
	protected boolean setup(final long p_totalVertexCount) {		
		m_curFrontier = new ConcurrentBitVector(p_totalVertexCount);
		m_nextFrontier = new ConcurrentBitVector(p_totalVertexCount);
		
		return true;
	}
	
	@Override
	protected boolean execute(long[] p_entryNodes) 
	{
		m_loggerService.info(getClass(), "Starting " + m_threads.length + " BFS Threads...");
		
		for (int i = 0; i < m_threads.length; i++) {
			m_threads[i] = new BFSThreadDist(i, m_loggerService, m_chunkService, m_networkService, 
					m_bootService.getNodeID(), m_vertexBatchCountPerThread, m_vertexMessageBatchCount, m_curFrontier, m_nextFrontier);
			m_threads[i].start();
		}
		
		if (m_numSlaves > 0) {
			executeMaster(p_entryNodes);
		} else {
			executeSlave();
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

		m_loggerService.info(getClass(), "Finished BFS.");
		return true;
	}
	
	private class Master
	{
		private BarrierMaster m_barrier;
		
		public Master()
		{
			m_barrier = new BarrierMaster(m_numSlaves, m_broadcastIntervalMs, m_networkService, m_bootService, m_loggerService);
		}
		
		public void execute(long[] p_entryNodes)
		{
			// entry barrier: wait for everyone to finish setup
			m_barrier.execute(1111, 0);
			
			// for each vertex of the root list execute a full bfs search
			for (long entryNode : p_entryNodes)
			{		 
				// send vertex to the node it is local to
				short entryNodeId = ChunkID.getCreatorID(entryNode);
				if (entryNodeId != m_bootService.getNodeID())
				{
					// send to remote 
					VerticesForNextFrontierMessage msg = new VerticesForNextFrontierMessage(entryNodeId, 1);
					msg.getVertexIDBuffer()[0] = entryNode;
					msg.setNumVerticesInBatch(1);
					
					if (m_networkService.sendMessage(msg) != NetworkErrorCodes.SUCCESS) {
						m_loggerService.error(getClass(), "Sending entry vertex " + Long.toHexString(entryNode) + " to node " + 
									NodeID.toHexString(entryNodeId) + " failed");
						continue; 
					}
				}
				else
				{
					// located here
					m_nextFrontier.pushBack(ChunkID.getLocalID(entryNode));
				}
				
				// for each bfs level
				int bfsLevel = 0;
				while (true)
				{
					// level sync
					m_barrier.execute(2222, bfsLevel);
					
					// frontier swap
					FrontierList tmp = m_curFrontier;
					m_curFrontier = m_nextFrontier;
					m_nextFrontier = tmp;
					m_nextFrontier.reset();
					
					// also swap them for threads!
					for (int t = 0; t < m_threads.length; t++) {	
						m_threads[t].triggerFrontierSwap();
					}
					
					// kick off threads with current frontier
					for (int t = 0; t < m_threads.length; t++) {	
						m_threads[t].runIteration(true);
					}
					
					// check periodically if threads are done
					// to detect end of bfs level
					boolean previousAllIdle = false;
//					while (!m_globalFinishedCurLevel)
//					{
//						boolean allIdle = true;
//						for (int t = 0; t < m_threads.length; t++) {	
//							if (!m_threads[t].isIdle()) {
//								allIdle = false;
//								break;
//							}
//						}
//						
//						if (!previousAllIdle && allIdle)
//						{
//							// signal we are bored
//							// TODO send message to master we are currently bored
//							// incoming level done message
//						}
//						else if (previousAllIdle && !allIdle)
//						{
//							// TODO send message to master that we are running and got work 
//						}
//						
//						previousAllIdle = allIdle;
//						
//						Thread.yield();
//					}
					
					// pause idling threads
					for (int t = 0; t < m_threads.length; t++) {	
						m_threads[t].runIteration(false);
					}
					
					// wait for all threads to enter pause state
					for (int t = 0; t < m_threads.length; t++) {	
						if (!m_threads[t].isIterationPaused()) {
							--t;
							Thread.yield();
							continue;
						}
					}
//					
//					m_totalVisitedCounter += lastLevelVisitedCounter;
//					m_totalBfsLevels++;
				
					bfsLevel++;
				}
			}
			
	
		}
	}
	
	private class Slave implements MessageReceiver
	{
		private BarrierSlave m_barrier;
		private volatile boolean m_globalFinishedCurLevel = false;
		
		private long m_totalVisitedCounter = 0;
		private int m_totalBfsLevels = 0;
		
		public Slave()
		{
			m_barrier = new BarrierSlave(m_networkService, m_loggerService);
		}
		
		public long getTotalVisitedCounter() {
			return m_totalVisitedCounter;
		}
		
		public int getTotalBfsLevels() {
			return m_totalBfsLevels;
		}
		
		public void execute()
		{
			short masterNodeId = -1;
			
			m_barrier.execute(1111, -1);
			masterNodeId = m_barrier.getMasterNodeID();
			
			// TODO for each vertex missing
			
			m_totalVisitedCounter = 0;
			m_totalBfsLevels = 0;
			
			long lastLevelVisitedCounter = 0;
			// for each level
			while (true)
			{
//				// use this to send our visited counter of the last iteration to the master
//				m_levelBarrier.execute(2222 + m_totalBfsLevels, lastLevelVisitedCounter);
//				// we received an exit signal
//				if (m_levelBarrier.getBarrierData() == -1)
//					break;

				// frontier swap
				FrontierList tmp = m_curFrontier;
				m_curFrontier = m_nextFrontier;
				m_nextFrontier = tmp;
				m_nextFrontier.reset();
				
				// also swap them for threads!
				for (int t = 0; t < m_threads.length; t++) {	
					m_threads[t].triggerFrontierSwap();
				}
				
				// TODO send message to master that we are running and got work 
				m_networkService.sendMessage(new SlaveRunning(masterNodeId));
				
				
				// kick off threads with current frontier
				for (int t = 0; t < m_threads.length; t++) {	
					m_threads[t].runIteration(true);
				}
				
				// check periodically if threads are done
				// to detect end of bfs level
				boolean previousAllIdle = false;
				while (!m_globalFinishedCurLevel)
				{
					boolean allIdle = true;
					for (int t = 0; t < m_threads.length; t++) {	
						if (!m_threads[t].isIdle()) {
							allIdle = false;
							break;
						}
					}
					
					if (!previousAllIdle && allIdle)
					{
						// signal we are bored
						// TODO send message to master we are currently bored
						// incoming level done message
					}
					else if (previousAllIdle && !allIdle)
					{
						// TODO send message to master that we are running and got work 
					}
					
					previousAllIdle = allIdle;
					
					Thread.yield();
				}
				
				// pause idling threads
				for (int t = 0; t < m_threads.length; t++) {	
					m_threads[t].runIteration(false);
				}
				
				// wait for all threads to enter pause state
				for (int t = 0; t < m_threads.length; t++) {	
					if (!m_threads[t].isIterationPaused()) {
						--t;
						Thread.yield();
						continue;
					}
				}
				
				m_totalVisitedCounter += lastLevelVisitedCounter;
				m_totalBfsLevels++;
			}
		}

		@Override
		public void onIncomingMessage(AbstractMessage p_message) {
			// TODO Auto-generated method stub
			
		}
	}
	
	private void executeMaster(long[] p_entryNodes)
	{
		// execute a full BFS for each node, master's task
		int bfsIteration = 0;
		for (long entryNode : p_entryNodes)
		{			
			// TODO send slaves our current iteration and how many iterations we have left
			// master: wait for all slaves to sign on
			// TODO catch error on execute
//			m_syncBarrierMaster = new SyncBarrierMaster(m_numSlaves, m_broadcastIntervalMs, 1111, m_networkService, m_bootService, m_loggerService);
//			m_syncBarrierMaster.execute();

			short entryNodeId = ChunkID.getCreatorID(entryNode);
			if (entryNodeId != m_bootService.getNodeID())
			{
				// send to remote 
				VerticesForNextFrontierMessage msg = new VerticesForNextFrontierMessage(entryNodeId, 1);
				msg.getVertexIDBuffer()[0] = entryNode;
				msg.setNumVerticesInBatch(1);
				
				if (m_networkService.sendMessage(msg) != NetworkErrorCodes.SUCCESS) {
					m_loggerService.error(getClass(), "Sending entry vertex " + Long.toHexString(entryNode) + " to node " + 
								NodeID.toHexString(entryNodeId) + " failed");
					continue; 
				}
			}
			else
			{
				m_nextFrontier.pushBack(ChunkID.getLocalID(entryNode));
			}
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// master: wait for all slaves to finish this
//			m_syncBarrierMaster = new SyncBarrierMaster(m_numSlaves, m_broadcastIntervalMs, 2222, m_networkService, m_bootService, m_loggerService);
//			m_syncBarrierMaster.execute();
			
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
		// TODO remove? have this external?
//		m_syncBarrierMaster = new SyncBarrierMaster(m_numSlaves, m_broadcastIntervalMs, 3333, m_networkService, m_bootService, m_loggerService);
//		m_syncBarrierMaster.execute();
	}
	
	private void executeSlave()
	{
		// execute a full BFS for each node, master's task
		int bfsIteration = 0;

		// TODO get from master how many iterations we got in total and how many are left
//		m_syncBarrierSlave = new SyncBarrierSlave(1111, m_networkService, m_loggerService);
//		m_syncBarrierSlave.execute();
//
//		
//		m_syncBarrierSlave = new SyncBarrierSlave(2222, m_networkService, m_loggerService);
//		m_syncBarrierSlave.execute();
		
		// frontier swap for everything, because we must put our entry node in next
		FrontierList tmp = m_curFrontier;
		m_curFrontier = m_nextFrontier;
		m_nextFrontier = tmp;
		m_nextFrontier.reset();
		
		// also swap them for threads!
		for (int t = 0; t < m_threads.length; t++) {	
			m_threads[t].triggerFrontierSwap();
		}
		
		m_loggerService.info(getClass(), "Executing BFS iteration (" + bfsIteration + ")");
		Pair<Long, Integer> results = runBFS(bfsIteration);
		m_loggerService.info(getClass(), "Results BFS iteration (" + bfsIteration + "): Iteration depth " + results.second() + ", total visited vertices " + results.first());
	
		bfsIteration++;
		
		
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
		
//		m_syncBarrierSlave = new SyncBarrierSlave(3333, m_networkService, m_loggerService);
//		m_syncBarrierSlave.execute();
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
			
			
//			// kick off threads with current frontier
//			for (int t = 0; t < m_threads.length; t++) {	
//				m_threads[t].triggerNextIteration();
//			}
//			
//			// join forked threads
//			int i = 0;
//			long tmpVisitedCounter = 0;
//			while (i < m_threads.length) {
//				if (!m_threads[i].isIterationLevelDone()) {
//					Thread.yield();
//					continue;
//				}
//				tmpVisitedCounter += m_threads[i].getVisitedCountLastRun();
//				i++;
//			}
			
//			visitedCounter += tmpVisitedCounter;
		
		
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
	
	private void incomingVerticesForNextFrontierMessage(final VerticesForNextFrontierMessage p_message) {
		long[] vertexIds = p_message.getVertexIDBuffer();
		for (int i = 0; i < p_message.getNumVerticesInBatch(); i++) {
			m_nextFrontier.pushBack(vertexIds[i]);
		}
	}
}

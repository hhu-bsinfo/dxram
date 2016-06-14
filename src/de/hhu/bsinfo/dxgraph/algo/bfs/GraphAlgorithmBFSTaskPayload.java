
package de.hhu.bsinfo.dxgraph.algo.bfs;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxgraph.GraphTaskPayloads;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.ConcurrentBitVectorHybrid;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSLevelFinishedMessage;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSMessages;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSResultMessage;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSTerminateMessage;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.PingMessage;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.VerticesForNextFrontierMessage;
import de.hhu.bsinfo.dxgraph.data.BFSResult;
import de.hhu.bsinfo.dxgraph.data.GraphRootList;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxgraph.load.GraphLoadBFSRootListTaskPayload;
import de.hhu.bsinfo.dxgraph.load.GraphLoadPartitionIndexTaskPayload;
import de.hhu.bsinfo.dxgraph.load.GraphPartitionIndex;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkMemoryService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.lookup.overlay.BarrierID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Compute task to run BFS on a loaded graph.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 13.05.16
 */
public class GraphAlgorithmBFSTaskPayload extends AbstractTaskPayload {

	private static final String MS_BFS_RESULT_NAMESRV_IDENT = "BFR";

	private static final Argument MS_ARG_BFS_ROOT =
			new Argument("bfsRootNameserviceEntryName", null, false,
					"Name of the nameservice entry for the roots to use for BFS.");
	private static final Argument MS_ARG_VERTEX_BATCH_SIZE =
			new Argument("vertexBatchSize", null, false, "Number of vertices to cache as a batch for processing.");
	private static final Argument MS_ARG_VERTEX_MSG_BATCH_SIZE =
			new Argument("vertexMessageBatchSize", null, false,
					"Name of vertices to send as a single batch over the network.");
	private static final Argument MS_ARG_NUM_THREADS =
			new Argument("numThreadsPerNode", null, false, "Number of threads to use for BFS on a single node.");
	private static final Argument MS_ARG_MARK_VERTICES =
			new Argument("markVertices", "true", true,
					"Mark the actual vertices/data visited with the level. On false, we just remember if we have visited it");

	private static final String MS_BARRIER_IDENT_0 = "BF0";

	private LoggerService m_loggerService;
	private ChunkService m_chunkService;
	private ChunkMemoryService m_chunkMemoryService;
	private NameserviceService m_nameserviceService;
	private NetworkService m_networkService;
	private BootService m_bootService;
	private SynchronizationService m_synchronizationService;
	private TemporaryStorageService m_temporaryStorageService;

	private short m_nodeId = NodeID.INVALID_ID;
	private GraphPartitionIndex m_graphPartitionIndex;

	private String m_bfsRootNameserviceEntry = GraphLoadBFSRootListTaskPayload.MS_BFS_ROOTS + "0";
	private int m_vertexBatchSize = 100;
	private int m_vertexMessageBatchSize = 100;
	private int m_numberOfThreadsPerNode = 4;
	private boolean m_markVertices = true;

	private int m_barrierId0 = BarrierID.INVALID_ID;

	public GraphAlgorithmBFSTaskPayload() {
		super(GraphTaskPayloads.TYPE, GraphTaskPayloads.SUBTYPE_GRAPH_ALGO_BFS);
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		m_loggerService = p_dxram.getService(LoggerService.class);
		m_chunkService = p_dxram.getService(ChunkService.class);
		m_chunkMemoryService = p_dxram.getService(ChunkMemoryService.class);
		m_nameserviceService = p_dxram.getService(NameserviceService.class);
		m_networkService = p_dxram.getService(NetworkService.class);
		m_bootService = p_dxram.getService(BootService.class);
		m_synchronizationService = p_dxram.getService(SynchronizationService.class);
		m_temporaryStorageService = p_dxram.getService(TemporaryStorageService.class);

		m_networkService.registerMessageType(BFSMessages.TYPE,
				BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE,
				VerticesForNextFrontierMessage.class);
		m_networkService.registerMessageType(BFSMessages.TYPE,
				BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE,
				BFSLevelFinishedMessage.class);
		m_networkService.registerMessageType(BFSMessages.TYPE,
				BFSMessages.SUBTYPE_BFS_TERMINATE_MESSAGE,
				BFSTerminateMessage.class);
		m_networkService.registerMessageType(BFSMessages.TYPE,
				BFSMessages.SUBTYPE_BFS_RESULT_MESSAGE,
				BFSResultMessage.class);
		m_networkService.registerMessageType(BFSMessages.TYPE,
				BFSMessages.SUBTYPE_PING_MESSAGE,
				PingMessage.class);

		// cache node id
		m_nodeId = m_bootService.getNodeID();

		// get partition index of the graph
		long graphPartitionIndexChunkId = m_nameserviceService
				.getChunkID(GraphLoadPartitionIndexTaskPayload.MS_PART_INDEX_IDENT + getComputeGroupId(), 5000);
		if (graphPartitionIndexChunkId == ChunkID.INVALID_ID) {
			// #if LOGGER >= ERROR
			m_loggerService.error(getClass(),
					"Cannot find graph partition index for compute group " + getComputeGroupId());
			// #endif /* LOGGER >= ERROR */
			return -1;
		}

		m_graphPartitionIndex = new GraphPartitionIndex();
		m_graphPartitionIndex.setID(graphPartitionIndexChunkId);
		if (!m_temporaryStorageService.get(m_graphPartitionIndex)) {
			// #if LOGGER >= ERROR
			m_loggerService.error(getClass(), "Getting graph partition index from temporary memory chunk "
					+ ChunkID.toHexString(graphPartitionIndexChunkId) + " failed.");
			// #endif /* LOGGER >= ERROR */
			return -2;
		}

		// get entry vertices for bfs
		long tmpStorageIdRootVertices = m_nameserviceService.getChunkID(m_bfsRootNameserviceEntry, 5000);
		if (tmpStorageIdRootVertices == ChunkID.INVALID_ID) {
			// #if LOGGER >= ERROR
			m_loggerService.error(getClass(),
					"Getting BFS entry vertex " + m_bfsRootNameserviceEntry + " failed, not valid.");
			// #endif /* LOGGER >= ERROR */
			return -3;
		}

		GraphRootList rootList = new GraphRootList(tmpStorageIdRootVertices);
		if (!m_temporaryStorageService.get(rootList)) {
			// #if LOGGER >= ERROR
			m_loggerService.error(getClass(),
					"Getting root list " + ChunkID.toHexString(tmpStorageIdRootVertices)
							+ " of vertices for bfs from temporary storage failed.");
			// #endif /* LOGGER >= ERROR */
			return -4;
		}

		// create barriers for bfs and register
		// or get the newly created barriers
		if (getSlaveId() == 0) {
			m_barrierId0 = m_synchronizationService.barrierAllocate(getSlaveNodeIds().length);

			m_nameserviceService.register(ChunkID.getChunkID(m_nodeId, m_barrierId0),
					MS_BARRIER_IDENT_0 + getComputeGroupId());
		} else {
			m_barrierId0 = (int) (m_nameserviceService.getChunkID(MS_BARRIER_IDENT_0 + getComputeGroupId(), -1));
		}

		// #if LOGGER >= INFO
		if (m_markVertices) {
			m_loggerService.info(getClass(), "Marking vertices mode (graph data will be altered)");
		} else {
			m_loggerService.info(getClass(), "Not marking vertices mode (graph data read only)");
		}
		// #endif /* LOGGER >= INFO */

		int iteration = 0;
		for (long root : rootList.getRoots()) {
			// #if LOGGER >= INFO
			m_loggerService.info(getClass(), "Executing BFS with root " + ChunkID.toHexString(root));
			// #endif /* LOGGER >= INFO */

			BFS bfs = new BFS(root);
			bfs.init(m_graphPartitionIndex.getPartitionIndex(getSlaveId()).getVertexCount(), m_markVertices);
			if (ChunkID.getCreatorID(root) == m_nodeId) {
				bfs.execute(root);
			} else {
				bfs.execute(ChunkID.INVALID_ID);
			}

			System.out.println("Local results of iteration " + iteration + ":\n" + bfs.getBFSResult());

			bfs.shutdown();

			// limit this to a single iteration on marking vertices
			// because we altered the vertex data, further iterations won't work (vertices already marked as visited)
			if (m_markVertices) {
				break;
			}

			iteration++;
		}

		// free barriers
		if (getSlaveId() == 0) {
			m_synchronizationService.barrierFree(m_barrierId0);
		} else {
			m_barrierId0 = BarrierID.INVALID_ID;
		}

		return 0;
	}

	@Override
	public void terminalCommandRegisterArguments(final ArgumentList p_argumentList) {
		p_argumentList.setArgument(MS_ARG_BFS_ROOT);
		p_argumentList.setArgument(MS_ARG_VERTEX_BATCH_SIZE);
		p_argumentList.setArgument(MS_ARG_VERTEX_MSG_BATCH_SIZE);
		p_argumentList.setArgument(MS_ARG_NUM_THREADS);
		p_argumentList.setArgument(MS_ARG_MARK_VERTICES);
	}

	@Override
	public void terminalCommandCallbackForArguments(final ArgumentList p_argumentList) {
		m_bfsRootNameserviceEntry = p_argumentList.getArgumentValue(MS_ARG_BFS_ROOT, String.class);
		m_vertexBatchSize = p_argumentList.getArgumentValue(MS_ARG_VERTEX_BATCH_SIZE, Integer.class);
		m_vertexMessageBatchSize = p_argumentList.getArgumentValue(MS_ARG_VERTEX_MSG_BATCH_SIZE, Integer.class);
		m_numberOfThreadsPerNode = p_argumentList.getArgumentValue(MS_ARG_NUM_THREADS, Integer.class);
		m_markVertices = p_argumentList.getArgumentValue(MS_ARG_MARK_VERTICES, Boolean.class);
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		int size = super.exportObject(p_exporter, p_size);

		p_exporter.writeInt(m_bfsRootNameserviceEntry.length());
		p_exporter.writeBytes(m_bfsRootNameserviceEntry.getBytes(StandardCharsets.US_ASCII));
		p_exporter.writeInt(m_vertexBatchSize);
		p_exporter.writeInt(m_vertexMessageBatchSize);
		p_exporter.writeInt(m_numberOfThreadsPerNode);
		p_exporter.writeByte((byte) (m_markVertices ? 1 : 0));

		return size + Integer.BYTES + m_bfsRootNameserviceEntry.length() + Integer.BYTES * 3 + 2 * Byte.BYTES;
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {
		int size = super.importObject(p_importer, p_size);

		int strLength = p_importer.readInt();
		byte[] tmp = new byte[strLength];
		p_importer.readBytes(tmp);
		m_bfsRootNameserviceEntry = new String(tmp, StandardCharsets.US_ASCII);
		m_vertexBatchSize = p_importer.readInt();
		m_vertexMessageBatchSize = p_importer.readInt();
		m_numberOfThreadsPerNode = p_importer.readInt();
		m_markVertices = p_importer.readByte() > 0;

		return size + Integer.BYTES + m_bfsRootNameserviceEntry.length() + Integer.BYTES * 3 + 2 * Byte.BYTES;
	}

	@Override
	public int sizeofObject() {
		return super.sizeofObject() + Integer.BYTES + m_bfsRootNameserviceEntry.length() + Integer.BYTES * 3
				+ Byte.BYTES;
	}

	/**
	 * Base class for the master/slave BFS roles. Every BFS instance can be used for a single
	 * BFS run, only.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 13.05.16
	 */
	private class BFS implements MessageReceiver {
		private BFSResult m_bfsLocalResult;

		private ConcurrentBitVectorHybrid m_curFrontier;
		private ConcurrentBitVectorHybrid m_nextFrontier;
		private ConcurrentBitVectorHybrid m_visitedFrontier;

		private BFSThread[] m_threads;
		private StatisticsThread m_statisticsThread;

		private AtomicInteger m_bfsSlavesLevelFinishedCounter = new AtomicInteger(0);
		private AtomicInteger m_bfsSlavesEmptyNextFrontiers = new AtomicInteger(0);
		private volatile boolean m_terminateBfs = true;
		private ReentrantReadWriteLock m_remoteDelegatesForNextFrontier = new ReentrantReadWriteLock(false);

		private AtomicLong m_edgeCountNextFrontier = new AtomicLong(0);

		/**
		 * Constructor
		 *
		 * @param p_bfsEntryNode Root entry node to start the BFS at.
		 */
		BFS(final long p_bfsEntryNode) {
			m_bfsLocalResult = new BFSResult();
			m_bfsLocalResult.m_rootVertexId = p_bfsEntryNode;
			GraphPartitionIndex.Entry entry = m_graphPartitionIndex.getPartitionIndex(getSlaveId());
			m_bfsLocalResult.m_graphSizeVertices = entry.getVertexCount();
			m_bfsLocalResult.m_graphSizeEdges = entry.getEdgeCount();
		}

		/**
		 * Get the local results of the BFS iteration (when done).
		 *
		 * @return BFS result.
		 */
		BFSResult getBFSResult() {
			return m_bfsLocalResult;
		}

		/**
		 * Initialize the BFS instance.
		 *
		 * @param p_totalVertexCount    Total vertex count of the graph.
		 * @param p_verticesMarkVisited False to not alter the graph data stored and use a local list to
		 *                              remember visited vertices, true to alter graph data and store visited
		 *                              information with the graph.
		 */
		void init(final long p_totalVertexCount, final boolean p_verticesMarkVisited) {
			m_curFrontier = new ConcurrentBitVectorHybrid(p_totalVertexCount, 1);
			m_nextFrontier = new ConcurrentBitVectorHybrid(p_totalVertexCount, 1);
			if (!p_verticesMarkVisited) {
				m_visitedFrontier = new ConcurrentBitVectorHybrid(p_totalVertexCount, 1);
			}

			m_networkService.registerReceiver(VerticesForNextFrontierMessage.class, this);
			m_networkService.registerReceiver(BFSLevelFinishedMessage.class, this);
			m_networkService.registerReceiver(BFSTerminateMessage.class, this);
			m_networkService.registerReceiver(BFSResultMessage.class, this);
			m_networkService.registerReceiver(PingMessage.class, this);

			m_statisticsThread = new StatisticsThread();

			// #if LOGGER >= INFO
			m_loggerService.info(getClass(), "Running BFS with " + m_numberOfThreadsPerNode + " threads on "
					+ p_totalVertexCount + " local vertices");
			// #endif /* LOGGER >= INFO */

			m_threads = new BFSThread[m_numberOfThreadsPerNode];
			for (int i = 0; i < m_threads.length; i++) {
				m_threads[i] =
						new BFSThread(i, m_vertexBatchSize, m_vertexMessageBatchSize, m_curFrontier, m_nextFrontier,
								m_visitedFrontier, m_statisticsThread.m_sharedVertexCounter,
								m_statisticsThread.m_sharedEdgeCounter);
				m_threads[i].start();
			}

			// sync before running
			m_synchronizationService.barrierSignOn(m_barrierId0, -1);

			// TODO workaround for network issue creating multiple socket connections
			{
				try {
					Thread.sleep(getSlaveId() * 1000);
				} catch (InterruptedException e) {
				}

				// send ping message to force open socket connections in order
				for (short slave : getSlaveNodeIds()) {
					if (slave != m_nodeId) {
						m_networkService.sendMessage(new PingMessage(slave));
					}
				}

				m_synchronizationService.barrierSignOn(m_barrierId0, -1);
			}
		}

		/**
		 * Execute one full BFS run.
		 *
		 * @param p_entryVertex Root vertex id to start BFS at.
		 */
		void execute(final long p_entryVertex) {
			boolean bottomUpApproach = false;
			long numEdgesInFrontier = 0;

			if (p_entryVertex != ChunkID.INVALID_ID) {
				// #if LOGGER >= INFO
				m_loggerService.info(getClass(),
						"I am starting BFS with entry vertex " + ChunkID.toHexString(p_entryVertex));
				// #endif /* LOGGER >= INFO */

				// mark root visited
				if (m_visitedFrontier == null) {
					Vertex vertex = new Vertex(p_entryVertex);
					if (m_chunkService.get(vertex) != 1) {
						m_loggerService.error(getClass(),
								"Getting root vertex " + ChunkID.toHexString(p_entryVertex) + " failed.");
						// TODO signal all other slaves to terminate (error)
						return;
					}

					vertex.setUserData(m_bfsLocalResult.m_totalBFSDepth);
					if (m_chunkService.put(vertex) != 1) {
						m_loggerService.error(getClass(),
								"Putting root vertex " + ChunkID.toHexString(p_entryVertex) + " failed.");
						// TODO signal all other slaves to terminate (error)
						return;
					}

					numEdgesInFrontier = vertex.getNeighbours().length;
				} else {
					m_visitedFrontier.pushBack(ChunkID.getLocalID(p_entryVertex));
					m_visitedFrontier.popFrontReset();
				}
				m_curFrontier.pushBack(ChunkID.getLocalID(p_entryVertex));
			}

			m_curFrontier.popFrontReset();

			// root already done
			m_bfsLocalResult.m_totalBFSDepth++;
			m_statisticsThread.start();

			long start = System.currentTimeMillis();

			while (true) {
				// determine bfs approach for next iteration
				if (bottomUpApproach) {
					// last iteration was bottom up approach, check if we should switch
					// TODO hardcoded edge degree 16
					if (m_curFrontier.size()
							< m_graphPartitionIndex.getPartitionIndex(getSlaveId()).getVertexCount() / 14 * 16) {
						bottomUpApproach = false;
					}
				} else {
					// last iteration was top down approach, check if we should switch
					if (numEdgesInFrontier
							> m_graphPartitionIndex.getPartitionIndex(getSlaveId()).getEdgeCount() / 10) {
						bottomUpApproach = true;
					}
				}

				m_loggerService.debug(getClass(), "BFS iteration executed "
						+ (bottomUpApproach ? "BOTTOM UP" : "TOP DOWN")
						+ ", curFront size: " + m_curFrontier.size()
						+ ", numEdgesInFrontier " + numEdgesInFrontier
						+ ", partition vertex count " + m_graphPartitionIndex.getPartitionIndex(getSlaveId())
						.getVertexCount()
						+ ", partition edge count " + m_graphPartitionIndex.getPartitionIndex(getSlaveId())
						.getEdgeCount());

				m_loggerService.info(getClass(), "BFS level " + m_bfsLocalResult.m_totalBFSDepth
						+ " with " + (bottomUpApproach ? "BOTTOM UP" : "TOP DOWN")
						+ " approach, frontier size " + m_curFrontier.size()
						+ " edges in cur frontier " + numEdgesInFrontier
						+ ", visited verts so far " + m_visitedFrontier.size());

				// reset
				numEdgesInFrontier = 0;

				// kick off threads with current frontier
				for (BFSThread thread : m_threads) {
					thread.runIteration(bottomUpApproach);
				}

				// wait actively until threads are done with their current iteration
				{
					int t = 0;
					while (t < m_threads.length) {
						if (!m_threads[t].hasIterationFinished()) {
							Thread.yield();
							continue;
						}
						numEdgesInFrontier += m_threads[t].getEdgeCountNextFrontier();
						t++;
					}
				}

				// #if LOGGER >= INFO
				m_loggerService.info(getClass(),
						"BFS Level " + m_bfsLocalResult.m_totalBFSDepth + " finished, verts " + m_statisticsThread
								.getTotalVertexCount()
								+ ", edges " + m_statisticsThread.getTotalEdgeCount() + " so far visited/traversed");
				// #endif /* LOGGER >= INFO */

				// --------------------------------
				// barrier
				{
					// inform all other slaves we are done
					short ownId = getSlaveId();
					short[] slavesNodeIds = getSlaveNodeIds();
					for (int i = 0; i < slavesNodeIds.length; i++) {
						if (i != ownId) {
							BFSLevelFinishedMessage msg = new BFSLevelFinishedMessage(slavesNodeIds[i]);
							NetworkErrorCodes err = m_networkService.sendMessage(msg);
							if (err != NetworkErrorCodes.SUCCESS) {
								m_loggerService.error(getClass(),
										"Sending level finished message to " + NodeID.toHexString(slavesNodeIds[i])
												+ " failed: " + err);
								// TODO abort execution and have the master send a kill signal
								// for this task to all other slaves

								i--;
							}
							System.out.println("Sent BFSLevelFinishedMessage " + NodeID.toHexString(slavesNodeIds[i]));
						}
					}

					System.out.println("Wait finish");
					// busy wait until everyone is done
					while (!m_bfsSlavesLevelFinishedCounter.compareAndSet(getSlaveNodeIds().length - 1, 0)) {
						Thread.yield();
					}
				}
				// --------------------------------

				// don't allow remote nodes to add delegates to our frontiers to avoid
				// data race on frontier swap
				m_remoteDelegatesForNextFrontier.writeLock().lock();

				// evaluate our own frontier first
				if (!m_nextFrontier.isEmpty()) {
					m_terminateBfs = false;
				}

				// --------------------------------
				// barrier
				{
					// inform all other slaves about our next frontier size to determine termination
					short ownId = getSlaveId();
					short[] slavesNodeIds = getSlaveNodeIds();
					for (int i = 0; i < slavesNodeIds.length; i++) {
						if (i != ownId) {
							BFSTerminateMessage msg =
									new BFSTerminateMessage(slavesNodeIds[i], m_nextFrontier.isEmpty());
							NetworkErrorCodes err = m_networkService.sendMessage(msg);
							if (err != NetworkErrorCodes.SUCCESS) {
								m_loggerService.error(getClass(),
										"Sending bfs terminate message to " + NodeID.toHexString(slavesNodeIds[i])
												+ " failed: " + err);
								// TODO abort execution and have the master send a kill signal
								// for this task to all other slaves

								i--;
							}
							System.out.println("Sent BFSTerminateMessage " + NodeID.toHexString(slavesNodeIds[i]));
						}
					}

					System.out.println("Wait termination");
					// wait until everyone reported the next frontier state/termination
					while (!m_bfsSlavesEmptyNextFrontiers.compareAndSet(getSlaveNodeIds().length - 1, 0)) {
						Thread.yield();
					}
				}
				// --------------------------------

				// check if we are done
				if (m_terminateBfs) {
					// #if LOGGER >= INFO
					m_loggerService.info(getClass(),
							"BFS terminated signal, last iteration level " + m_bfsLocalResult.m_totalBFSDepth
									+ ", total edges traversed " + m_bfsLocalResult.m_totalVisitedVertices
									+ ", num vertices of graph visited " + m_visitedFrontier.size());
					// #endif /* LOGGER >= INFO */
					break;
				}
				// reset for next run
				m_terminateBfs = true;

				// all nodes are finished, frontier swap
				ConcurrentBitVectorHybrid tmp = m_curFrontier;
				m_curFrontier = m_nextFrontier;
				m_nextFrontier = tmp;
				m_nextFrontier.reset();
				m_curFrontier.popFrontReset();
				m_visitedFrontier.popFrontReset();

				numEdgesInFrontier += m_edgeCountNextFrontier.get();
				m_edgeCountNextFrontier.set(0);

				// also swap the references of all threads!
				for (BFSThread thread : m_threads) {
					thread.triggerFrontierSwap();
					thread.setCurrentBFSDepthLevel(m_bfsLocalResult.m_totalBFSDepth);
				}

				// now we are good, allow new delegates from remotes
				m_remoteDelegatesForNextFrontier.writeLock().unlock();

				// #if LOGGER >= DEBUG
				m_loggerService.debug(getClass(), "Frontier swap, new cur frontier size: " + m_curFrontier.size());
				// #endif /* LOGGER >= DEBUG */

				// go for next run
				m_bfsLocalResult.m_totalBFSDepth++;
			}

			System.out.println(">>>>>>>>>>>>> iteration time: " + (System.currentTimeMillis() - start));

			m_statisticsThread.shutdown();
			try {
				m_statisticsThread.join();
			} catch (final InterruptedException ignored) {
			}

			// collect further results
			m_bfsLocalResult.m_totalVisitedVertices = m_statisticsThread.getTotalVertexCount();
			m_bfsLocalResult.m_totalEdgesTraversed = m_statisticsThread.getTotalEdgeCount();
			m_bfsLocalResult.m_maxVertsPerSecond = m_statisticsThread.getMaxVerticesVisitedPerSec();
			m_bfsLocalResult.m_maxEdgesPerSecond = m_statisticsThread.getMaxEdgesTraversedPerSec();
			m_bfsLocalResult.m_avgVertsPerSecond = m_statisticsThread.getAvgVerticesPerSec();
			m_bfsLocalResult.m_avgEdgesPerSecond = m_statisticsThread.getAvgEdgesPerSec();
			m_bfsLocalResult.m_totalTimeMs = m_statisticsThread.getTotalTimeMs();
		}

		/**
		 * Shutdown the instance and cleanup resources used for the BFS run.
		 */
		void shutdown() {
			for (BFSThread thread : m_threads) {
				thread.exitThread();
			}

			// #if LOGGER >= DEBUG
			m_loggerService.debug(getClass(), "Joining BFS threads...");
			// #endif /* LOGGER >= DEBUG */

			for (BFSThread thread : m_threads) {
				try {
					thread.join();
				} catch (final InterruptedException ignored) {
				}
			}

			m_networkService.unregisterReceiver(VerticesForNextFrontierMessage.class, this);
			m_networkService.unregisterReceiver(BFSLevelFinishedMessage.class, this);
			m_networkService.unregisterReceiver(BFSTerminateMessage.class, this);
			m_networkService.unregisterReceiver(BFSResultMessage.class, this);

			// #if LOGGER >= DEBUG
			m_loggerService.debug(getClass(), "BFS shutdown");
			// #endif /* LOGGER >= DEBUG */
		}

		@Override
		public void onIncomingMessage(final AbstractMessage p_message) {
			if (p_message != null) {
				if (p_message.getType() == BFSMessages.TYPE) {
					switch (p_message.getSubtype()) {
						case BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE:
							onIncomingVerticesForNextFrontierMessage(
									(VerticesForNextFrontierMessage) p_message);
							break;
						case BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE:
							onIncomingBFSLevelFinishedMessage(
									(BFSLevelFinishedMessage) p_message);
							break;
						case BFSMessages.SUBTYPE_BFS_TERMINATE_MESSAGE:
							onIncomingBFSTerminateMessage(
									(BFSTerminateMessage) p_message);
							break;
						default:
							break;
					}
				}
			}
		}

		/**
		 * Handle incoming AbstractVerticesForNextFrontierRequest messages.
		 *
		 * @param p_message AbstractVerticesForNextFrontierRequest to handle
		 */
		private void onIncomingVerticesForNextFrontierMessage(final VerticesForNextFrontierMessage p_message) {
			// this determines whether the message comes from a node running top down or bottom up approach
			if (p_message.getNumNeighborsInBatch() > 0) {
				// bottom up approach
				VerticesForNextFrontierMessage reply =
						new VerticesForNextFrontierMessage(p_message.getSource(), m_vertexBatchSize);
				long vertexId = p_message.getVertex();
				long neighborId = p_message.getNeighbor();
				while (vertexId != -1) {
					System.out.println("mark request bottom up " + ChunkID.toHexString(vertexId) + "/" + ChunkID
							.toHexString(neighborId));

					// check if the remote vertex is a neighbor of the
					// local parent (on this node)
					// -> inter node edge
					if (m_curFrontier.contains(ChunkID.getLocalID(neighborId))) {
						reply.addVertex(vertexId);
					}

					vertexId = p_message.getVertex();
					neighborId = p_message.getNeighbor();
				}

				if (reply.getNumVerticesInBatch() > 0) {
					NetworkErrorCodes err = m_networkService.sendMessage(reply);
					if (err != NetworkErrorCodes.SUCCESS) {
						m_loggerService
								.error(getClass(), "Sending reply for bottom up vertices next frontier failed: " + err);
					}
				}
			} else {
				// top down approach

				// check if we are allowed to add
				m_remoteDelegatesForNextFrontier.readLock().lock();

				long vertexId = p_message.getVertex();
				while (vertexId != -1) {
					System.out.println("mark response top down " + ChunkID.toHexString(vertexId));
					// TODO mark mode missing
					// check if visited and add to frontier if not
					long localId = ChunkID.getLocalID(vertexId);
					if (m_visitedFrontier.pushBack(localId)) {
						m_nextFrontier.pushBack(localId);
						System.out.println("Visited: " + ChunkID.toHexString(vertexId));

						// read num of edges for calculating bottom up <-> top down switching formula
						int numEdges = m_chunkMemoryService.readInt(vertexId, 4);
						if (numEdges != -1) {
							m_edgeCountNextFrontier.addAndGet(numEdges);
						} else {
							m_loggerService.error(getClass(),
									"Could not read num neighbors field of vertex " + ChunkID
											.toHexString(vertexId));
						}
					}

					vertexId = p_message.getVertex();
				}

				m_remoteDelegatesForNextFrontier.readLock().unlock();
			}
		}

		private void onIncomingBFSLevelFinishedMessage(final BFSLevelFinishedMessage p_message) {
			System.out.println("onIncomingBFSLevelFinishedMessage " + NodeID.toHexString(p_message.getSource()) + " | "
					+ m_bfsSlavesLevelFinishedCounter.incrementAndGet());
		}

		private void onIncomingBFSTerminateMessage(final BFSTerminateMessage p_message) {

			if (!p_message.isNextFrontierEmpty()) {
				m_terminateBfs = false;
			}

			System.out.println("onIncomingBFSTerminateMessage " + NodeID.toHexString(p_message.getSource()) + " | "
					+ m_bfsSlavesEmptyNextFrontiers.incrementAndGet());
		}

		/**
		 * Separate thread printing local statistics of the current BFS run periodically
		 */
		class StatisticsThread extends Thread {

			private volatile boolean m_run = true;
			private AtomicLong m_sharedVertexCounter;
			private AtomicLong m_sharedEdgeCounter;
			private long m_maxVerticesPerSec;
			private long m_maxEdgesPerSec;
			private long m_avgVerticesPerSec;
			private long m_avgEdgesPerSec;
			private long m_totalTimeMs;

			/**
			 * Constructor
			 */
			StatisticsThread() {
				m_sharedVertexCounter = new AtomicLong(0);
				m_sharedEdgeCounter = new AtomicLong(0);
				m_maxVerticesPerSec = 0;
				m_maxEdgesPerSec = 0;
			}

			/**
			 * Get the currently total vertex visited count.
			 *
			 * @return Current total vertex visited count.
			 */
			long getTotalVertexCount() {
				return m_sharedVertexCounter.get();
			}

			/**
			 * Get the currently total edge traversed count.
			 *
			 * @return Current total edge traversed count.
			 */
			long getTotalEdgeCount() {
				return m_sharedEdgeCounter.get();
			}

			/**
			 * Get the currently max visited vertices per second.
			 *
			 * @return Max visited vertices per second.
			 */
			long getMaxVerticesVisitedPerSec() {
				return m_maxVerticesPerSec;
			}

			/**
			 * Get the currently max traversed edges per second.
			 *
			 * @return Max traversed edges per second.
			 */
			long getMaxEdgesTraversedPerSec() {
				return m_maxEdgesPerSec;
			}

			/**
			 * Get the average vertices visited per second.
			 *
			 * @return Average vertices per second visited.
			 */
			long getAvgVerticesPerSec() {
				return m_avgVerticesPerSec;
			}

			/**
			 * Get the average edges traversed per second.
			 *
			 * @return Average edges per second traversed.
			 */
			long getAvgEdgesPerSec() {
				return m_avgEdgesPerSec;
			}

			/**
			 * Get the total running time in ms.
			 *
			 * @return Total running time.
			 */
			long getTotalTimeMs() {
				return m_totalTimeMs;
			}

			/**
			 * Shutdown the statistics thread.
			 */
			public void shutdown() {
				m_run = false;
				this.interrupt();
			}

			@Override
			public void run() {
				long lastValueVertexCounter = m_sharedVertexCounter.get();
				long lastValueEdgeCounter = m_sharedEdgeCounter.get();
				long avgCounter = 1;
				long avgVertsSum = 0;
				long avgEdgesSum = 0;
				long startTime = System.currentTimeMillis();
				while (m_run) {
					long valueVertexCounter = m_sharedVertexCounter.get();
					long valueEdgeCounter = m_sharedEdgeCounter.get();

					long verticesPerSec = valueVertexCounter - lastValueVertexCounter;
					long edgesPerSec = valueEdgeCounter - lastValueEdgeCounter;

					if (verticesPerSec > m_maxVerticesPerSec) {
						m_maxVerticesPerSec = verticesPerSec;
					}

					if (edgesPerSec > m_maxEdgesPerSec) {
						m_maxEdgesPerSec = edgesPerSec;
					}

					avgVertsSum += verticesPerSec;
					avgEdgesSum += edgesPerSec;
					m_avgVerticesPerSec = avgVertsSum / avgCounter;
					m_avgEdgesPerSec = avgEdgesSum / avgCounter;

					m_totalTimeMs = System.currentTimeMillis() - startTime;

					String str = "";
					str += "[Running time (ms): " + m_totalTimeMs + "]";
					str += "[Vertices/sec: " + verticesPerSec + "]";
					str += "[Edges/sec: " + edgesPerSec + "]";
					str += "[MaxVerts/sec: " + m_maxVerticesPerSec + "]";
					str += "[MaxEdges/sec: " + m_maxEdgesPerSec + "]";
					str += "[AvgVerts/sec: " + m_avgVerticesPerSec + "]";
					str += "[AvgEdges/sec: " + m_avgEdgesPerSec + "]";

					System.out.println(str);

					lastValueVertexCounter = valueVertexCounter;
					lastValueEdgeCounter = valueEdgeCounter;
					avgCounter++;

					try {
						Thread.sleep(1000);
					} catch (final InterruptedException ignored) {
						m_totalTimeMs = System.currentTimeMillis() - startTime;
					}
				}
			}
		}
	}

	/**
	 * Single BFS thread running BFS algorithm locally.
	 */
	private class BFSThread extends Thread {

		private int m_id = -1;
		private int m_vertexMessageBatchSize;
		private ConcurrentBitVectorHybrid m_curFrontier;
		private ConcurrentBitVectorHybrid m_nextFrontier;
		private ConcurrentBitVectorHybrid m_visitedFrontier;

		private short m_nodeId;
		private Vertex[] m_vertexBatch;
		private int m_currentDepthLevel;
		private HashMap<Short, VerticesForNextFrontierMessage> m_remoteMessages =
				new HashMap<>();

		private volatile boolean m_runIteration;
		private volatile boolean m_exitThread;
		private volatile boolean m_bottomUpIteration;
		private AtomicLong m_edgeCountNextFrontier = new AtomicLong(0);

		private AtomicLong m_sharedVertexCounter;
		private AtomicLong m_sharedEdgeCounter;

		/**
		 * Constructor
		 *
		 * @param p_id                     Id of the thread.
		 * @param p_vertexBatchSize        Number of vertices to process in a single batch.
		 * @param p_vertexMessageBatchSize Number of vertices to back as a batch into a single message to
		 *                                 send to other nodes.
		 * @param p_curFrontierShared      Shared instance with other threads of the current frontier.
		 * @param p_nextFrontierShared     Shared instance with other threads of the next frontier
		 * @param p_visitedFrontierShared  Shared instance with other threads of the visited frontier.
		 * @param p_sharedVertexCounter    Shared instance with other threads to count visited vertices
		 * @param p_sharedEdgeCounter      Shared instance with other threads to count traversed edges
		 */
		BFSThread(final int p_id, final int p_vertexBatchSize, final int p_vertexMessageBatchSize,
				final ConcurrentBitVectorHybrid p_curFrontierShared,
				final ConcurrentBitVectorHybrid p_nextFrontierShared,
				final ConcurrentBitVectorHybrid p_visitedFrontierShared,
				final AtomicLong p_sharedVertexCounter, final AtomicLong p_sharedEdgeCounter) {
			super("BFSThread-" + p_id);

			m_id = p_id;
			m_vertexMessageBatchSize = p_vertexMessageBatchSize;
			m_curFrontier = p_curFrontierShared;
			m_nextFrontier = p_nextFrontierShared;
			m_visitedFrontier = p_visitedFrontierShared;

			m_nodeId = m_bootService.getNodeID();
			m_vertexBatch = new Vertex[p_vertexBatchSize];
			for (int i = 0; i < m_vertexBatch.length; i++) {
				m_vertexBatch[i] = new Vertex(ChunkID.INVALID_ID);
				// performance hack: if writing back, we only write back what we changed
				m_vertexBatch[i].setWriteUserDataOnly(true);
			}

			m_sharedVertexCounter = p_sharedVertexCounter;
			m_sharedEdgeCounter = p_sharedEdgeCounter;
		}

		/**
		 * Set the current BFS depth level
		 *
		 * @param p_curDepth Current BFS depth.
		 */
		void setCurrentBFSDepthLevel(final int p_curDepth) {
			m_currentDepthLevel = p_curDepth;
		}

		/**
		 * Trigger running a single iteration until all vertices of the current frontier are processed.
		 */
		void runIteration(final boolean p_bottomUpIteration) {
			m_bottomUpIteration = p_bottomUpIteration;
			m_edgeCountNextFrontier.set(0);
			m_runIteration = true;
		}

		long getEdgeCountNextFrontier() {
			return m_edgeCountNextFrontier.get();
		}

		/**
		 * Check if the currently running iteration has finished.
		 *
		 * @return True if finished, false if still running.
		 */
		boolean hasIterationFinished() {
			return !m_runIteration;
		}

		/**
		 * Trigger a frontier swap. Swap cur and next to prepare for next iteration.
		 */
		void triggerFrontierSwap() {
			ConcurrentBitVectorHybrid tmp = m_curFrontier;
			m_curFrontier = m_nextFrontier;
			m_nextFrontier = tmp;
		}

		/**
		 * Trigger thread exit.
		 */
		void exitThread() {
			m_exitThread = true;
		}

		@Override
		public void run() {
			boolean enterIdle = false;
			while (true) {
				do {
					if (m_exitThread) {
						return;
					}

					if (enterIdle) {
						enterIdle = false;
						m_runIteration = false;
					}

					if (!m_runIteration) {
						Thread.yield();
					}
				} while (!m_runIteration);

				// --------------------------------------------------

				int validVertsInBatch = 0;

				if (m_bottomUpIteration) {
					m_visitedFrontier.popFrontLock();
					for (Vertex vertexBatch : m_vertexBatch) {
						long tmp = m_visitedFrontier.popFrontInverse();
						// 0 is the index chunk, re-pop
						if (tmp == 0) {
							tmp = m_visitedFrontier.popFrontInverse();
						}
						if (tmp != -1) {
							vertexBatch.setID(ChunkID.getChunkID(m_nodeId, tmp));
							validVertsInBatch++;
						} else {
							if (validVertsInBatch == 0) {
								enterIdle = true;
								break;
							}
							vertexBatch.setID(ChunkID.INVALID_ID);
						}
					}
					m_visitedFrontier.popFrontUnlock();
				} else {
					m_curFrontier.popFrontLock();
					for (Vertex vertexBatch : m_vertexBatch) {
						long tmp = m_curFrontier.popFront();
						if (tmp != -1) {
							vertexBatch.setID(ChunkID.getChunkID(m_nodeId, tmp));
							validVertsInBatch++;
						} else {
							if (validVertsInBatch == 0) {
								enterIdle = true;
								break;
							}
							vertexBatch.setID(ChunkID.INVALID_ID);
						}
					}
					m_curFrontier.popFrontUnlock();
				}

				// --------------------------------------------------

				if (validVertsInBatch == 0) {
					// make sure to send out remaining messages which have not reached the
					// batch size, yet (because they will never reach it in this round)
					for (Entry<Short, VerticesForNextFrontierMessage> entry : m_remoteMessages.entrySet()) {
						VerticesForNextFrontierMessage msg = entry.getValue();
						if (msg.getBatchSize() > 0) {
							if (m_networkService.sendMessage(msg) != NetworkErrorCodes.SUCCESS) {
								// #if LOGGER >= ERROR
								m_loggerService.error(getClass(), "Sending vertex message to node "
										+ NodeID.toHexString(msg.getDestination()) + " failed");
								// #endif /* LOGGER >= ERROR */
								return;
							}

							// don't reuse requests, does not work with previous responses counting as fulfilled
							msg = new VerticesForNextFrontierMessage(msg.getDestination(),
									m_vertexMessageBatchSize);
							m_remoteMessages.put(entry.getKey(), msg);
						}
					}

					// we are done, go to start
					continue;
				}

				// --------------------------------------------------

				int gett = m_chunkService.getLocal(m_vertexBatch, 0, validVertsInBatch);
				if (gett != validVertsInBatch) {
					// #if LOGGER >= ERROR
					m_loggerService.error(getClass(),
							"Getting vertices in BFS Thread " + m_id + " failed: " + gett + " != " + validVertsInBatch);
					// #endif /* LOGGER >= ERROR */
					return;
				}

				// --------------------------------------------------

				int writeBackCount = 0;
				for (int i = 0; i < validVertsInBatch; i++) {
					// check first if visited
					Vertex vertex = m_vertexBatch[i];

					// skip vertices that were already marked invalid before
					if (vertex.getID() == ChunkID.INVALID_ID) {
						continue;
					}

					writeBackCount++;
					m_sharedVertexCounter.incrementAndGet();

					System.out.println(vertex);

					long[] neighbours = vertex.getNeighbours();
					long vertexLocalId = ChunkID.getLocalID(vertex.getID());
					for (long neighbour : neighbours) {
						// check if neighbors are valid, otherwise something's not ok with the data
						if (neighbour == ChunkID.INVALID_ID) {
							// #if LOGGER >= WARN
							m_loggerService.warn(getClass(), "Invalid neighbor found on vertex " + vertex);
							// #endif /* LOGGER >= WARN */
							continue;
						}

						// don't allow access to the index chunk
						if (ChunkID.getLocalID(neighbour) == 0) {
							// #if LOGGER >= WARN
							m_loggerService.warn(getClass(), "Neighbor id refers to index chunk " + ChunkID
									.toHexString(neighbour) + ", vertex " + vertex);
							// #endif /* LOGGER >= WARN */
							continue;
						}

						m_sharedEdgeCounter.incrementAndGet();

						// sort by remote and local vertices
						short neighborCreatorId = ChunkID.getCreatorID(neighbour);
						long neighborLocalId = ChunkID.getLocalID(neighbour);

						if (m_bottomUpIteration) {
							if (neighborCreatorId != m_nodeId) {
								// the child is on the current node but its parent is on a diferent one
								// => inter node edge

								// delegate to remote, fill message buffers until they are full -> send
								VerticesForNextFrontierMessage msg = m_remoteMessages.get(neighborCreatorId);
								if (msg == null) {
									msg = new VerticesForNextFrontierMessage(neighborCreatorId,
											m_vertexMessageBatchSize);

									m_remoteMessages.put(neighborCreatorId, msg);
								}

								// add vertex to message batch
								if (!msg.addVertex(vertex.getID())) {
									// vertex does not fit anymore, full
									if (m_networkService.sendMessage(msg) != NetworkErrorCodes.SUCCESS) {
										// #if LOGGER >= ERROR
										m_loggerService.error(getClass(), "Sending vertex message to node "
												+ NodeID.toHexString(neighborCreatorId) + " failed");
										// #endif /* LOGGER >= ERROR */
										return;
									}

									// TODO message re-using? didn't work with requests due to fullfilled
									msg = new VerticesForNextFrontierMessage(neighborCreatorId,
											m_vertexMessageBatchSize);
									m_remoteMessages.put(neighborCreatorId, msg);
									msg.addVertex(vertex.getID());
									msg.addNeighbor(neighbour);
								} else {
									msg.addNeighbor(neighbour);
								}
							} else {
								// is our child connected to any of the parents
								if (m_curFrontier.contains(neighborLocalId)) {
									// child -> parent relationship, got our next vertex
									// TODO differ mark mode and non mark mode
									// mark child (!) visited
									if (m_visitedFrontier.pushBack(vertexLocalId)) {
										m_nextFrontier.pushBack(vertexLocalId);

										System.out.println("Visited: " + ChunkID.toHexString(vertex.getID()));

										// read num of edges for calculating bottom up <-> top down switching formula
										m_edgeCountNextFrontier.addAndGet(neighbours.length);

										// we don't have to continue with any other neighbors
										// for all neighbors (possible parents) of child
										break;
									}
								}
							}
						} else {
							if (neighborCreatorId != m_nodeId) {
								// delegate to remote, fill message buffers until they are full -> send
								VerticesForNextFrontierMessage msg = m_remoteMessages.get(neighborCreatorId);
								if (msg == null) {
									msg = new VerticesForNextFrontierMessage(neighborCreatorId,
											m_vertexMessageBatchSize);

									m_remoteMessages.put(neighborCreatorId, msg);
								}

								// add vertex to message batch
								if (!msg.addVertex(neighbour)) {
									// neighbor does not fit anymore, full
									if (m_networkService.sendMessage(msg) != NetworkErrorCodes.SUCCESS) {
										// #if LOGGER >= ERROR
										m_loggerService.error(getClass(), "Sending vertex message to node "
												+ NodeID.toHexString(neighborCreatorId) + " failed");
										// #endif /* LOGGER >= ERROR */
										return;
									}

									// don't reuse requests, does not work with previous responses counting as fulfilled
									msg = new VerticesForNextFrontierMessage(neighborCreatorId,
											m_vertexMessageBatchSize);
									m_remoteMessages.put(msg.getDestination(), msg);
									msg.addVertex(neighbour);
								}
							} else {
								// mark visited
								// TODO differ mark mode and non mark mode
								// mark visited and add to next if not visited so far
								if (m_visitedFrontier.pushBack(neighborLocalId)) {
									m_nextFrontier.pushBack(neighborLocalId);

									System.out.println("Visited: " + ChunkID.toHexString(neighbour));

									// read num of edges for calculating bottom up <-> top down switching formula
									int numEdges = m_chunkMemoryService.readInt(neighbour, 4);
									if (numEdges != -1) {
										m_edgeCountNextFrontier.addAndGet(numEdges);
									} else {
										m_loggerService.error(getClass(),
												"Could not read num neighbors field of vertex " + ChunkID
														.toHexString(neighbour));
									}

								}

							}
						}
					}
				}

				// --------------------------------------------------

				//				if (m_visitedFrontier == null) {
				//					// for marking mode, write back data
				//					int put =
				//							m_chunkService
				//									.put(ChunkLockOperation.NO_LOCK_OPERATION, m_vertexBatch, 0, validVertsInBatch);
				//					if (put != writeBackCount) {
				//						// #if LOGGER >= ERROR
				//						m_loggerService.error(getClass(),
				//								"Putting vertices in BFS Thread " + m_id + " failed: " + put + " != " + writeBackCount);
				//						// #endif /* LOGGER >= ERROR */
				//						return;
				//					}
				//				}
			}
		}
	}
}

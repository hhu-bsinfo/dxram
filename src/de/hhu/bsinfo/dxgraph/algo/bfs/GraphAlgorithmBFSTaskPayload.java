
package de.hhu.bsinfo.dxgraph.algo.bfs;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxgraph.GraphTaskPayloads;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.ConcurrentBitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.AbstractVerticesForNextFrontierRequest;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSMessages;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSResultMessage;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.VerticesForNextFrontierCompressedRequest;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.VerticesForNextFrontierRequest;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.VerticesForNextFrontierResponse;
import de.hhu.bsinfo.dxgraph.data.BFSResult;
import de.hhu.bsinfo.dxgraph.data.BFSResults;
import de.hhu.bsinfo.dxgraph.data.GraphRootList;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxgraph.load.GraphLoadBFSRootListTaskPayload;
import de.hhu.bsinfo.dxgraph.load.GraphLoadPartitionIndexTaskPayload;
import de.hhu.bsinfo.dxgraph.load.GraphPartitionIndex;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
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
import de.hhu.bsinfo.utils.Pair;
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
	private static final Argument MS_ARG_COMP_VERTEX_MSGS =
			new Argument("compVertexMsgs", "false", true,
					"Use compressed messages when sending non local vertices to their owners");

	private static final String MS_BARRIER_IDENT_0 = "BF0";
	private static final String MS_BARRIER_IDENT_1 = "BF1";
	private static final String MS_BARRIER_IDENT_2 = "BF2";
	private static final String MS_BARRIER_IDENT_3 = "BF3";

	private LoggerService m_loggerService;
	private ChunkService m_chunkService;
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
	private boolean m_compressedVertexMessages;

	private int m_barrierId0 = BarrierID.INVALID_ID;
	private int m_barrierId1 = BarrierID.INVALID_ID;
	private int m_barrierId2 = BarrierID.INVALID_ID;
	private int m_barrierId3 = BarrierID.INVALID_ID;

	public GraphAlgorithmBFSTaskPayload() {
		super(GraphTaskPayloads.TYPE, GraphTaskPayloads.SUBTYPE_GRAPH_ALGO_BFS);
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		m_loggerService = p_dxram.getService(LoggerService.class);
		m_chunkService = p_dxram.getService(ChunkService.class);
		m_nameserviceService = p_dxram.getService(NameserviceService.class);
		m_networkService = p_dxram.getService(NetworkService.class);
		m_bootService = p_dxram.getService(BootService.class);
		m_synchronizationService = p_dxram.getService(SynchronizationService.class);
		m_temporaryStorageService = p_dxram.getService(TemporaryStorageService.class);

		m_networkService.registerMessageType(BFSMessages.TYPE, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_REQUEST,
				VerticesForNextFrontierRequest.class);
		m_networkService.registerMessageType(BFSMessages.TYPE,
				BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_COMPRESSED_REQUEST,
				VerticesForNextFrontierCompressedRequest.class);
		m_networkService.registerMessageType(BFSMessages.TYPE,
				BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_RESPONSE,
				VerticesForNextFrontierResponse.class);
		m_networkService.registerMessageType(BFSMessages.TYPE,
				BFSMessages.SUBTYPE_BFS_RESULT_MESSAGE,
				BFSResultMessage.class);

		// cache node id
		m_nodeId = m_bootService.getNodeID();

		// get partition index of the graph
		long graphPartitionIndexChunkId = m_nameserviceService
				.getChunkID(GraphLoadPartitionIndexTaskPayload.MS_PART_INDEX_IDENT + getComputeGroupId(), 5000);
		if (graphPartitionIndexChunkId == ChunkID.INVALID_ID) {
			m_loggerService.error(getClass(),
					"Cannot find graph partition index for compute group " + getComputeGroupId());
			return -1;
		}

		m_graphPartitionIndex = new GraphPartitionIndex();
		m_graphPartitionIndex.setID(graphPartitionIndexChunkId);
		if (!m_temporaryStorageService.get(m_graphPartitionIndex)) {
			m_loggerService.error(getClass(), "Getting graph partition index from temporary memory chunk "
					+ ChunkID.toHexString(graphPartitionIndexChunkId) + " failed.");
			return -2;
		}

		// get entry vertices for bfs
		long tmpStorageIdRootVertices = m_nameserviceService.getChunkID(m_bfsRootNameserviceEntry, 5000);
		if (tmpStorageIdRootVertices == ChunkID.INVALID_ID) {
			m_loggerService.error(getClass(),
					"Getting BFS entry vertex " + m_bfsRootNameserviceEntry + " failed, not valid.");
			return -3;
		}

		GraphRootList rootList = new GraphRootList(tmpStorageIdRootVertices);
		if (!m_temporaryStorageService.get(rootList)) {
			m_loggerService.error(getClass(),
					"Getting root list " + ChunkID.toHexString(tmpStorageIdRootVertices)
							+ " of vertices for bfs from temporary storage failed.");
			return -4;
		}

		// create barriers for bfs and register
		// or get the newly created barriers
		if (getSlaveId() == 0) {
			m_barrierId0 = m_synchronizationService.barrierAllocate(getSlaveNodeIds().length);
			m_barrierId1 = m_synchronizationService.barrierAllocate(getSlaveNodeIds().length);
			m_barrierId2 = m_synchronizationService.barrierAllocate(getSlaveNodeIds().length);
			m_barrierId3 = m_synchronizationService.barrierAllocate(getSlaveNodeIds().length);

			m_nameserviceService.register(ChunkID.getChunkID(m_nodeId, m_barrierId0),
					MS_BARRIER_IDENT_0 + getComputeGroupId());
			m_nameserviceService.register(ChunkID.getChunkID(m_nodeId, m_barrierId1),
					MS_BARRIER_IDENT_1 + getComputeGroupId());
			m_nameserviceService.register(ChunkID.getChunkID(m_nodeId, m_barrierId2),
					MS_BARRIER_IDENT_2 + getComputeGroupId());
			m_nameserviceService.register(ChunkID.getChunkID(m_nodeId, m_barrierId3),
					MS_BARRIER_IDENT_3 + getComputeGroupId());
		} else {
			m_barrierId0 = (int) (m_nameserviceService.getChunkID(MS_BARRIER_IDENT_0 + getComputeGroupId(), -1));
			m_barrierId1 = (int) (m_nameserviceService.getChunkID(MS_BARRIER_IDENT_1 + getComputeGroupId(), -1));
			m_barrierId2 = (int) (m_nameserviceService.getChunkID(MS_BARRIER_IDENT_2 + getComputeGroupId(), -1));
			m_barrierId3 = (int) (m_nameserviceService.getChunkID(MS_BARRIER_IDENT_3 + getComputeGroupId(), -1));
		}

		if (m_markVertices) {
			m_loggerService.info(getClass(), "Marking vertices mode (graph data will be altered)");
		} else {
			m_loggerService.info(getClass(), "Not marking vertices mode (graph data read only)");
		}

		if (m_compressedVertexMessages) {
			m_loggerService.info(getClass(), "Using compressed vertex messages for forwarding");
		} else {
			m_loggerService.info(getClass(), "Using non compressed vertex messages for forwarding");
		}

		int bfsIteration = 0;
		for (long root : rootList.getRoots()) {
			m_loggerService.info(getClass(), "Executing BFS with root " + ChunkID.toHexString(root));

			// run the bfs root on the node it is local to
			if (ChunkID.getCreatorID(root) == m_nodeId) {
				// run as bfs master
				m_loggerService.info(getClass(), "I (" + getSlaveId() + ") am running as master");
				BFSMaster master = new BFSMaster(root, bfsIteration);
				master.init(m_graphPartitionIndex.getPartitionIndex(getSlaveId()).getVertexCount(), m_markVertices,
						m_compressedVertexMessages);
				master.execute(root);
				master.shutdown();
			} else {
				// run as bfs slave, master is the owner of the root node
				m_loggerService.info(getClass(), "I (" + getSlaveId() + ") am running as slave");
				BFSSlave slave = new BFSSlave(root, bfsIteration);
				slave.init(m_graphPartitionIndex.getPartitionIndex(getSlaveId()).getVertexCount(), m_markVertices,
						m_compressedVertexMessages);
				slave.execute(ChunkID.INVALID_ID);
				slave.shutdown();
			}

			bfsIteration++;
			// limit this to a single iteration on marking vertices
			// because we altered the vertex data, further iterations won't work (vertices already marked as visited)
			if (m_markVertices) {
				break;
			}
		}

		// free barriers
		if (getSlaveId() == 0) {
			m_synchronizationService.barrierFree(m_barrierId0);
			m_synchronizationService.barrierFree(m_barrierId1);
			m_synchronizationService.barrierFree(m_barrierId2);
			m_synchronizationService.barrierFree(m_barrierId3);
		} else {
			m_barrierId0 = BarrierID.INVALID_ID;
			m_barrierId1 = BarrierID.INVALID_ID;
			m_barrierId2 = BarrierID.INVALID_ID;
			m_barrierId3 = BarrierID.INVALID_ID;
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
		p_argumentList.setArgument(MS_ARG_COMP_VERTEX_MSGS);
	}

	@Override
	public void terminalCommandCallbackForArguments(final ArgumentList p_argumentList) {
		m_bfsRootNameserviceEntry = p_argumentList.getArgumentValue(MS_ARG_BFS_ROOT, String.class);
		m_vertexBatchSize = p_argumentList.getArgumentValue(MS_ARG_VERTEX_BATCH_SIZE, Integer.class);
		m_vertexMessageBatchSize = p_argumentList.getArgumentValue(MS_ARG_VERTEX_MSG_BATCH_SIZE, Integer.class);
		m_numberOfThreadsPerNode = p_argumentList.getArgumentValue(MS_ARG_NUM_THREADS, Integer.class);
		m_markVertices = p_argumentList.getArgumentValue(MS_ARG_MARK_VERTICES, Boolean.class);
		m_compressedVertexMessages = p_argumentList.getArgumentValue(MS_ARG_COMP_VERTEX_MSGS, Boolean.class);
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
		p_exporter.writeByte((byte) (m_compressedVertexMessages ? 1 : 0));

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
		m_compressedVertexMessages = p_importer.readByte() > 0;

		return size + Integer.BYTES + m_bfsRootNameserviceEntry.length() + Integer.BYTES * 3 + 2 * Byte.BYTES;
	}

	@Override
	public int sizeofObject() {
		return super.sizeofObject() + Integer.BYTES + m_bfsRootNameserviceEntry.length() + Integer.BYTES * 3
				+ 2 * Byte.BYTES;
	}

	/**
	 * Base class for the master/slave BFS roles. Every BFS instance can be used for a single
	 * BFS run, only.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 13.05.16
	 */
	private abstract class AbstractBFSMS implements MessageReceiver {
		private BFSResult m_bfsLocalResult;

		private ConcurrentBitVector m_curFrontier;
		private ConcurrentBitVector m_nextFrontier;
		private ConcurrentBitVector m_visitedFrontier;

		private BFSThread[] m_threads;
		private StatisticsThread m_statisticsThread;
		private int m_bfsIteration;

		/**
		 * Constructor
		 *
		 * @param p_bfsEntryNode Root entry node to start the BFS at.
		 * @param p_bfsIteration BFS iteration count (not BFS depth).
		 */
		AbstractBFSMS(final long p_bfsEntryNode, final int p_bfsIteration) {
			m_bfsLocalResult = new BFSResult();
			m_bfsLocalResult.m_rootVertexId = p_bfsEntryNode;
			GraphPartitionIndex.Entry entry = m_graphPartitionIndex.getPartitionIndex(getSlaveId());
			m_bfsLocalResult.m_graphSizeVertices = entry.getVertexCount();
			m_bfsLocalResult.m_graphSizeEdges = entry.getEdgeCount();
			m_bfsIteration = p_bfsIteration;
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
		 * @param p_totalVertexCount         Total vertex count of the graph.
		 * @param p_verticesMarkVisited      False to not alter the graph data stored and use a local list to
		 *                                   remember visited vertices, true to alter graph data and store visited
		 *                                   information with the graph.
		 * @param p_compressedVertexMessages True to use compressed vertex messages when sending vertex data to other
		 *                                   nodes, false to use uncompressed messages.
		 */
		void init(final long p_totalVertexCount, final boolean p_verticesMarkVisited,
				final boolean p_compressedVertexMessages) {
			m_curFrontier = new ConcurrentBitVector(p_totalVertexCount);
			m_nextFrontier = new ConcurrentBitVector(p_totalVertexCount);
			if (!p_verticesMarkVisited) {
				m_visitedFrontier = new ConcurrentBitVector(p_totalVertexCount);
			}

			m_networkService.registerReceiver(VerticesForNextFrontierRequest.class, this);
			m_networkService.registerReceiver(VerticesForNextFrontierCompressedRequest.class, this);
			m_networkService.registerReceiver(BFSResultMessage.class, this);

			m_statisticsThread = new StatisticsThread();

			m_loggerService.info(getClass(), "Running BFS with " + m_numberOfThreadsPerNode + " threads on "
					+ p_totalVertexCount + " local vertices");

			m_threads = new BFSThread[m_numberOfThreadsPerNode];
			for (int i = 0; i < m_threads.length; i++) {
				m_threads[i] =
						new BFSThread(i, m_vertexBatchSize, m_vertexMessageBatchSize, m_curFrontier, m_nextFrontier,
								m_visitedFrontier, p_compressedVertexMessages, m_statisticsThread.m_sharedVertexCounter,
								m_statisticsThread.m_sharedEdgeCounter);
				m_threads[i].start();
			}
		}

		/**
		 * Execute one full BFS run.
		 *
		 * @param p_entryVertex Root vertex id to start BFS at.
		 */
		void execute(final long p_entryVertex) {
			if (p_entryVertex != ChunkID.INVALID_ID) {
				m_loggerService.info(getClass(),
						"I am starting BFS with entry vertex " + ChunkID.toHexString(p_entryVertex));
				m_curFrontier.pushBack(ChunkID.getLocalID(p_entryVertex));
			}

			m_statisticsThread.start();

			while (true) {
				m_loggerService.debug(getClass(),
						"Processing next BFS level " + m_bfsLocalResult.m_totalBFSDepth
								+ ", total vertices visited so far "
								+ m_bfsLocalResult.m_totalVisitedVertices + "...");

				// kick off threads with current frontier
				for (BFSThread thread : m_threads) {
					thread.runIteration();
				}

				// wait actively until threads are done with their current iteration
				{
					int t = 0;
					while (t < m_threads.length) {
						if (!m_threads[t].hasIterationFinished()) {
							Thread.yield();
							continue;
						}
						t++;
					}
				}

				m_loggerService.info(getClass(),
						"BFS Level " + m_bfsLocalResult.m_totalBFSDepth + " finished, verts " + m_statisticsThread
								.getTotalVertexCount()
								+ ", edges " + m_statisticsThread.getTotalEdgeCount() + " so far visited/traversed");

				// signal we are done with our iteration
				barrierSignalIterationComplete();

				// all nodes are finished, frontier swap
				ConcurrentBitVector tmp = m_curFrontier;
				m_curFrontier = m_nextFrontier;
				m_nextFrontier = tmp;
				m_nextFrontier.reset();

				m_loggerService.debug(getClass(), "Frontier swap, new cur frontier size: " + m_curFrontier.size());

				// also swap the references of all threads!
				for (BFSThread thread : m_threads) {
					thread.triggerFrontierSwap();
				}

				// signal frontier swap and ready for next iteration
				barrierSignalFrontierSwap(m_curFrontier.size());

				if (barrierSignalTerminate()) {
					m_loggerService.info(getClass(),
							"BFS terminated signal, last iteration level " + m_bfsLocalResult.m_totalBFSDepth
									+ ", total visited " + m_bfsLocalResult.m_totalVisitedVertices);
					break;
				}

				m_loggerService.debug(getClass(), "Continue next BFS level");

				// go for next run
				m_bfsLocalResult.m_totalBFSDepth++;
			}

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

			System.out.println("Local: " + m_bfsLocalResult);

			m_synchronizationService.barrierSignOn(m_barrierId3, -1);
			handleBFSResult(m_bfsLocalResult, m_bfsIteration);
		}

		/**
		 * Shutdown the instance and cleanup resources used for the BFS run.
		 */
		void shutdown() {
			for (BFSThread thread : m_threads) {
				thread.exitThread();
			}

			m_loggerService.debug(getClass(), "Joining BFS threads...");
			for (BFSThread thread : m_threads) {
				try {
					thread.join();
				} catch (final InterruptedException ignored) {
				}
			}

			m_networkService.unregisterReceiver(VerticesForNextFrontierRequest.class, this);
			m_networkService.unregisterReceiver(VerticesForNextFrontierCompressedRequest.class, this);
			m_networkService.unregisterReceiver(BFSResultMessage.class, this);

			m_loggerService.debug(getClass(), "BFS shutdown");
		}

		@Override
		public void onIncomingMessage(final AbstractMessage p_message) {
			if (p_message != null) {
				if (p_message.getType() == BFSMessages.TYPE) {
					switch (p_message.getSubtype()) {
						case BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_REQUEST:
						case BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_COMPRESSED_REQUEST:
							onIncomingVerticesForNextFrontierMessage(
									(AbstractVerticesForNextFrontierRequest) p_message);
							break;
						case BFSMessages.SUBTYPE_BFS_RESULT_MESSAGE:
							onIncomingBFSResultMessage((BFSResultMessage) p_message);
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
		private void onIncomingVerticesForNextFrontierMessage(final AbstractVerticesForNextFrontierRequest p_message) {
			long vertexId = p_message.getVertex();
			while (vertexId != -1) {
				m_nextFrontier.pushBack(vertexId);
				vertexId = p_message.getVertex();
			}

			// TODO we have to use requests here to make sure all messages arrive sync with sending them
			// otherwise, using normal messages, we can't tell when the message gets processed and reaches this function
			// this can lead to messages sent out very late arriving too late when the current bfs iteration level
			// is already considered complete. this results in missing out on vertex data and putting into the
			// wrong frontier leading to frontier corruption

			VerticesForNextFrontierResponse response = new VerticesForNextFrontierResponse(p_message);
			m_networkService.sendMessage(response);
		}

		/**
		 * Called when a single BFS level iteration completed.
		 */
		protected abstract void barrierSignalIterationComplete();

		/**
		 * Called after the frontier swap completed.
		 *
		 * @param p_nextFrontierSize Size of the next frontier.
		 */
		protected abstract void barrierSignalFrontierSwap(final long p_nextFrontierSize);

		/**
		 * Called to check if BFS has to terminate.
		 *
		 * @return True to terminate, false to continue with next level.
		 */
		protected abstract boolean barrierSignalTerminate();

		/**
		 * Called when BFS terminated and results need to be aggregated and stored.
		 *
		 * @param p_bfsResult    Single local BFS result.
		 * @param p_bfsIteration BFS iteration id (not BFS level depth).
		 */
		protected abstract void handleBFSResult(final BFSResult p_bfsResult, final int p_bfsIteration);

		/**
		 * Called to receive BFSResultMessages from other nodes for aggregation.
		 *
		 * @param p_message BFSResultMessage to aggregate.
		 */
		protected abstract void onIncomingBFSResultMessage(final BFSResultMessage p_message);

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
	 * Master instance for BFS
	 */
	private class BFSMaster extends AbstractBFSMS {
		private boolean m_signalTermination;

		private ReentrantLock m_aggregateResultLock = new ReentrantLock(false);
		private ArrayList<Pair<Short, BFSResult>> m_resultsSlaves = new ArrayList<>();
		private int m_slaveCount;

		/**
		 * Constructor
		 *
		 * @param p_bfsEntryNode Root entry node to start the BFS at.
		 * @param p_bfsIteration BFS iteration count (not BFS depth).
		 */
		BFSMaster(final long p_bfsEntryNode, final int p_bfsIteration) {
			super(p_bfsEntryNode, p_bfsIteration);

			// don't count ourselves
			m_slaveCount = getSlaveNodeIds().length - 1;
		}

		@Override
		protected void barrierSignalIterationComplete() {

			Pair<short[], long[]> result = m_synchronizationService.barrierSignOn(m_barrierId0, -1);
			if (result == null) {
				m_loggerService.error(getClass(),
						"Iteration complete, sign on to barrier " + BarrierID.toHexString(m_barrierId0) + " failed.");
			}
		}

		@Override
		protected void barrierSignalFrontierSwap(final long p_nextFrontierSize) {
			Pair<short[], long[]> result = m_synchronizationService.barrierSignOn(m_barrierId1, -1);
			if (result == null) {
				m_loggerService.error(getClass(),
						"Frontier swap, sign on to barrier " + BarrierID.toHexString(m_barrierId1) + " failed.");
			}

			// check if all frontier sizes are 0 -> terminate bfs
			boolean allFrontiersEmpty = true;
			assert result != null;
			for (long data : result.second()) {
				if (data > 0) {
					allFrontiersEmpty = false;
					break;
				}
			}

			// frontiers of all other slaves and this one have to be empty
			if (allFrontiersEmpty && p_nextFrontierSize == 0) {
				m_signalTermination = true;
			}
		}

		@Override
		protected boolean barrierSignalTerminate() {
			if (m_synchronizationService.barrierSignOn(m_barrierId2, m_signalTermination ? 1 : 0) == null) {
				m_loggerService.error(getClass(),
						"Signal terminate, sign on to barrier " + BarrierID.toHexString(m_barrierId2) + " failed.");
			}

			return m_signalTermination;
		}

		@Override
		protected void handleBFSResult(final BFSResult p_bfsResult, final int p_bfsIteration) {
			m_loggerService.info(getClass(), "Aggregating BFS results of iteration " + p_bfsIteration);

			while (m_slaveCount > 0) {
				try {
					Thread.sleep(1);
				} catch (final InterruptedException ignored) {
				}
			}

			m_loggerService.debug(getClass(), "All slaves have submitted their results");

			BFSResults results = new BFSResults();
			results.addResult(getSlaveId(), m_nodeId, p_bfsResult);
			results.getAggregatedResult().m_totalTimeMs = p_bfsResult.m_totalTimeMs;
			results.getAggregatedResult().m_totalBFSDepth = p_bfsResult.m_totalBFSDepth;

			results.getAggregatedResult().m_graphSizeVertices += p_bfsResult.m_graphSizeVertices;
			results.getAggregatedResult().m_graphSizeEdges += p_bfsResult.m_graphSizeEdges;
			results.getAggregatedResult().m_totalVisitedVertices += p_bfsResult.m_totalVisitedVertices;
			results.getAggregatedResult().m_totalEdgesTraversed += p_bfsResult.m_totalEdgesTraversed;
			results.getAggregatedResult().m_maxVertsPerSecond += p_bfsResult.m_maxVertsPerSecond;
			results.getAggregatedResult().m_maxEdgesPerSecond += p_bfsResult.m_maxEdgesPerSecond;
			results.getAggregatedResult().m_avgVertsPerSecond += p_bfsResult.m_avgVertsPerSecond;
			results.getAggregatedResult().m_avgEdgesPerSecond += p_bfsResult.m_avgEdgesPerSecond;

			// aggregate results
			for (Pair<Short, BFSResult> otherResult : m_resultsSlaves) {
				// get compute group id
				short slaveId = -1;
				short[] slaveNodeIds = getSlaveNodeIds();
				for (int i = 0; i < slaveNodeIds.length; i++) {
					if (slaveNodeIds[i] == otherResult.first()) {
						slaveId = (short) i;
						break;
					}
				}

				results.addResult(slaveId, otherResult.m_first, otherResult.m_second);

				results.getAggregatedResult().m_graphSizeVertices += otherResult.m_second.m_graphSizeVertices;
				results.getAggregatedResult().m_graphSizeEdges += otherResult.m_second.m_graphSizeEdges;
				results.getAggregatedResult().m_totalVisitedVertices += otherResult.m_second.m_totalVisitedVertices;
				results.getAggregatedResult().m_totalEdgesTraversed += otherResult.m_second.m_totalEdgesTraversed;
				results.getAggregatedResult().m_maxVertsPerSecond += otherResult.m_second.m_maxVertsPerSecond;
				results.getAggregatedResult().m_maxEdgesPerSecond += otherResult.m_second.m_maxEdgesPerSecond;
				results.getAggregatedResult().m_avgVertsPerSecond += otherResult.m_second.m_avgVertsPerSecond;
				results.getAggregatedResult().m_avgEdgesPerSecond += otherResult.m_second.m_avgEdgesPerSecond;

			}

			// upload
			System.out.println("Result of BFS iteration: \n" + results);

			if (m_chunkService.create(results) != 1) {
				m_loggerService.error(getClass(), "Creating chunk for bfs result failed.");
				return;
			}

			if (m_chunkService.put(results) != 1) {
				m_loggerService.error(getClass(), "Putting data of bfs result failed.");
				return;
			}

			String resultName = MS_BFS_RESULT_NAMESRV_IDENT + p_bfsIteration;
			m_nameserviceService.register(results, resultName);
			m_loggerService.info(getClass(), "BFS results stored and registered: " + resultName);
		}

		@Override
		protected void onIncomingBFSResultMessage(final BFSResultMessage p_message) {
			m_aggregateResultLock.lock();

			m_resultsSlaves.add(new Pair<>(p_message.getSource(), p_message.getBFSResult()));
			m_slaveCount--;

			m_aggregateResultLock.unlock();
		}
	}

	/**
	 * Slave BFS instance
	 */
	private class BFSSlave extends AbstractBFSMS {

		/**
		 * Constructor
		 *
		 * @param p_bfsEntryNode Root entry node to start the BFS at.
		 * @param p_bfsIteration BFS iteration count (not BFS depth).
		 */
		BFSSlave(final long p_bfsEntryNode, final int p_bfsIteration) {
			super(p_bfsEntryNode, p_bfsIteration);
		}

		@Override
		protected void barrierSignalIterationComplete() {
			if (m_synchronizationService.barrierSignOn(m_barrierId0, -1) == null) {
				m_loggerService.error(getClass(),
						"Iteration complete, sign on to barrier " + BarrierID.toHexString(m_barrierId0) + " failed.");
			}
		}

		@Override
		protected void barrierSignalFrontierSwap(final long p_nextFrontierSize) {
			if (m_synchronizationService.barrierSignOn(m_barrierId1, p_nextFrontierSize) == null) {
				m_loggerService.error(getClass(),
						"Frontier swap, sign on to barrier " + BarrierID.toHexString(m_barrierId1) + " failed.");
			}
		}

		@Override
		protected boolean barrierSignalTerminate() {
			Pair<short[], long[]> result = m_synchronizationService.barrierSignOn(m_barrierId2, -1);
			if (result == null) {
				m_loggerService.error(getClass(),
						"Signal terminate, sign on to barrier " + BarrierID.toHexString(m_barrierId2) + " failed.");
			}

			// look for signal terminate flag (0 or 1)
			assert result != null;
			for (int i = 0; i < result.first().length; i++) {
				if (result.second()[i] == 1) {
					return true;
				}
			}

			return false;
		}

		@Override
		protected void handleBFSResult(final BFSResult p_bfsResult, final int p_bfsIteration) {
			m_loggerService.info(getClass(), "Sending local results for aggregation to master");
			// send result to master
			// master is the node owning the root for the bfs iteration
			BFSResultMessage message =
					new BFSResultMessage(ChunkID.getCreatorID(p_bfsResult.m_rootVertexId), p_bfsResult);
			NetworkErrorCodes err = m_networkService.sendMessage(message);
			if (err != NetworkErrorCodes.SUCCESS) {
				m_loggerService.error(getClass(), "Sending bfs results to master " + NodeID
						.toHexString(getSlaveNodeIds()[0]) + " failed: " + err);
			}
		}

		@Override
		protected void onIncomingBFSResultMessage(final BFSResultMessage p_message) {
			m_loggerService.error(getClass(),
					"Received BFS results message but I am not master, wrong node id destination specified on sender "
							+ NodeID.toHexString(p_message.getSource()));
		}
	}

	/**
	 * Single BFS thread running BFS algorithm locally.
	 */
	private class BFSThread extends Thread {

		private int m_id = -1;
		private int m_vertexMessageBatchSize;
		private ConcurrentBitVector m_curFrontier;
		private ConcurrentBitVector m_nextFrontier;
		private ConcurrentBitVector m_visitedFrontier;
		private boolean m_compressedVertexMessages;

		private short m_nodeId;
		private Vertex[] m_vertexBatch;
		private int m_currentIterationLevel;
		private HashMap<Short, AbstractVerticesForNextFrontierRequest> m_remoteMessages =
				new HashMap<>();

		private volatile boolean m_runIteration;
		private volatile boolean m_exitThread;
		private volatile boolean m_bottomUp;

		private AtomicLong m_sharedVertexCounter;
		private AtomicLong m_sharedEdgeCounter;

		/**
		 * Constructor
		 *
		 * @param p_id                       Id of the thread.
		 * @param p_vertexBatchSize          Number of vertices to process in a single batch.
		 * @param p_vertexMessageBatchSize   Number of vertices to back as a batch into a single message to
		 *                                   send to other nodes.
		 * @param p_curFrontierShared        Shared instance with other threads of the current frontier.
		 * @param p_nextFrontierShared       Shared instance with other threads of the next frontier
		 * @param p_visitedFrontierShared    Shared instance with other threads of the visited frontier.
		 * @param p_compressedVertexMessages True to use compressed messages, false for uncompressed
		 * @param p_sharedVertexCounter      Shared instance with other threads to count visited vertices
		 * @param p_sharedEdgeCounter        Shared instance with other threads to count traversed edges
		 */
		BFSThread(final int p_id, final int p_vertexBatchSize, final int p_vertexMessageBatchSize,
				final ConcurrentBitVector p_curFrontierShared, final ConcurrentBitVector p_nextFrontierShared,
				final ConcurrentBitVector p_visitedFrontierShared, final boolean p_compressedVertexMessages,
				final AtomicLong p_sharedVertexCounter, final AtomicLong p_sharedEdgeCounter) {
			super("BFSThread-" + p_id);

			m_id = p_id;
			m_vertexMessageBatchSize = p_vertexMessageBatchSize;
			m_curFrontier = p_curFrontierShared;
			m_nextFrontier = p_nextFrontierShared;
			m_visitedFrontier = p_visitedFrontierShared;
			m_compressedVertexMessages = p_compressedVertexMessages;

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
		 * Set the current BFS iteration level (BFS depth)
		 *
		 * @param p_iterationLevel Current BFS depth.
		 */
		void setCurrentBFSIterationLevel(final int p_iterationLevel) {
			m_currentIterationLevel = p_iterationLevel;
		}

		/**
		 * Trigger running a single iteration until all vertices of the current frontier are processed.
		 */
		void runIteration() {
			// determine to use top down or bottom up approach
			// for next iteration
			if (m_curFrontier.size() > m_curFrontier.capacity() / 2) {
				m_bottomUp = true;
				m_loggerService.debug(getClass(), "Going bottom up for this iteration");
			} else {
				m_bottomUp = false;
				m_loggerService.debug(getClass(), "Going top down for this iteration");
			}

			m_runIteration = true;
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
			ConcurrentBitVector tmp = m_curFrontier;
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

				int validVertsInBatch = 0;
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

				if (validVertsInBatch == 0) {
					// make sure to send out remaining messages which have not reached the
					// batch size, yet (because they will never reach it in this round)
					for (Entry<Short, AbstractVerticesForNextFrontierRequest> entry : m_remoteMessages.entrySet()) {
						AbstractVerticesForNextFrontierRequest msg = entry.getValue();
						if (msg.getBatchSize() > 0) {
							if (m_networkService.sendSync(msg) != NetworkErrorCodes.SUCCESS) {
								m_loggerService.error(getClass(), "Sending vertex message to node "
										+ NodeID.toHexString(msg.getDestination()) + " failed");
								return;
							}

							// don't reuse requests, does not work with previous responses counting as fulfilled
							if (m_compressedVertexMessages) {
								msg = new VerticesForNextFrontierCompressedRequest(msg.getDestination(),
										m_vertexMessageBatchSize);
							} else {
								msg = new VerticesForNextFrontierRequest(msg.getDestination(),
										m_vertexMessageBatchSize);
							}
							m_remoteMessages.put(entry.getKey(), msg);
						}
					}

					// we are done, go to start
					continue;
				}

				int gett = m_chunkService.get(m_vertexBatch, 0, validVertsInBatch);
				if (gett != validVertsInBatch) {
					m_loggerService.error(getClass(),
							"Getting vertices in BFS Thread " + m_id + " failed: " + gett + " != " + validVertsInBatch);
					return;
				}

				int writeBackCount = 0;
				for (int i = 0; i < validVertsInBatch; i++) {
					// check first if visited
					Vertex vertex = m_vertexBatch[i];

					// skip vertices that were already marked invalid before
					if (vertex.getID() == ChunkID.INVALID_ID) {
						continue;
					}

					// two "modes": mark the actual vertex visited with the current bfs level
					// or just remember that we have visited it and don't alter vertex data
					boolean isVisited;
					if (m_visitedFrontier == null) {
						if (vertex.getUserData() == -1) {
							// set depth level
							vertex.setUserData(m_currentIterationLevel);
							isVisited = false;
						} else {
							// already visited, don't have to put back to storage
							vertex.setID(ChunkID.INVALID_ID);
							isVisited = true;
						}
					} else {
						long id = ChunkID.getLocalID(vertex.getID());
						isVisited = !m_visitedFrontier.pushBack(id);
					}

					if (!isVisited) {
						writeBackCount++;
						m_sharedVertexCounter.incrementAndGet();
						long[] neighbours = vertex.getNeighbours();

						for (long neighbour : neighbours) {
							// check if neighbors are valid, otherwise something's not ok with the data
							if (neighbour == ChunkID.INVALID_ID) {
								m_loggerService.warn(getClass(), "Invalid neighbor found on vertex " + vertex);
								continue;
							}

							// don't allow access to the index chunk
							if (ChunkID.getLocalID(neighbour) == 0) {
								m_loggerService.warn(getClass(), "Neighbor id refers to index chunk " + ChunkID
										.toHexString(neighbour) + ", vertex " + vertex);
								continue;
							}

							m_sharedEdgeCounter.incrementAndGet();

							// sort by remote and local vertices
							short creatorId = ChunkID.getCreatorID(neighbour);
							if (creatorId != m_nodeId) {
								// go remote, fill message buffers until they are full -> send
								AbstractVerticesForNextFrontierRequest msg = m_remoteMessages.get(creatorId);
								if (msg == null) {
									if (m_compressedVertexMessages) {
										msg = new VerticesForNextFrontierCompressedRequest(creatorId,
												m_vertexMessageBatchSize);
									} else {
										msg = new VerticesForNextFrontierRequest(creatorId,
												m_vertexMessageBatchSize);
									}

									m_remoteMessages.put(creatorId, msg);
								}

								// add vertex to message batch
								if (!msg.addVertex(neighbour)) {
									// neighbor does not fit anymore, full
									if (m_networkService.sendSync(msg) != NetworkErrorCodes.SUCCESS) {
										m_loggerService.error(getClass(), "Sending vertex message to node "
												+ NodeID.toHexString(creatorId) + " failed");
										return;
									}

									// don't reuse requests, does not work with previous responses counting as fulfilled
									if (m_compressedVertexMessages) {
										msg = new VerticesForNextFrontierCompressedRequest(creatorId,
												m_vertexMessageBatchSize);
									} else {
										msg = new VerticesForNextFrontierRequest(creatorId,
												m_vertexMessageBatchSize);
									}
									m_remoteMessages.put(msg.getDestination(), msg);
									msg.addVertex(neighbour);
								}
							} else {
								m_nextFrontier.pushBack(ChunkID.getLocalID(neighbour));
							}
						}
					}
				}

				if (m_visitedFrontier == null) {
					// for marking mode, write back data
					int put =
							m_chunkService
									.put(ChunkLockOperation.NO_LOCK_OPERATION, m_vertexBatch, 0, validVertsInBatch);
					if (put != writeBackCount) {
						m_loggerService.error(getClass(),
								"Putting vertices in BFS Thread " + m_id + " failed: " + put + " != " + writeBackCount);
						return;
					}
				}
			}
		}
	}
}

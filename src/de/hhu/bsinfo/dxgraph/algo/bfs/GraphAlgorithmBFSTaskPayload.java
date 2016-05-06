
package de.hhu.bsinfo.dxgraph.algo.bfs;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxgraph.GraphTaskPayloads;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.ConcurrentBitVector;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.FrontierList;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSMessages;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.VerticesForNextFrontierMessage;
import de.hhu.bsinfo.dxgraph.data.BFSResult;
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
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map.Entry;

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

	private LoggerService m_loggerService;
	private ChunkService m_chunkService;
	private NameserviceService m_nameserviceService;
	private NetworkService m_networkService;
	private BootService m_bootService;
	private SynchronizationService m_synchronizationService;

	private short m_nodeId = NodeID.INVALID_ID;
	private GraphPartitionIndex m_graphPartitionIndex;

	private String m_bfsRootNameserviceEntry = new String(GraphLoadBFSRootListTaskPayload.MS_BFS_ROOTS + "0");
	private int m_vertexBatchSize = 100;
	private int m_vertexMessageBatchSize = 100;
	private int m_numberOfThreadsPerNode = 4;

	public GraphAlgorithmBFSTaskPayload() {
		super(GraphTaskPayloads.TYPE, GraphTaskPayloads.SUBTYPE_GRAPH_ALGO_BFS);
	}

	public void setBFSRootNameserviceEntry(final String p_name) {
		m_bfsRootNameserviceEntry = p_name;
	}

	public void setVertexBatchSize(final int p_batchSize) {
		m_vertexBatchSize = p_batchSize;
	}

	public void setVertexMessageBatchSize(final int p_batchSize) {
		m_vertexMessageBatchSize = p_batchSize;
	}

	public void setNumberOfThreadsPerNode(final int p_numThreadsPerNode) {
		m_numberOfThreadsPerNode = p_numThreadsPerNode;
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		m_loggerService = p_dxram.getService(LoggerService.class);
		m_chunkService = p_dxram.getService(ChunkService.class);
		m_nameserviceService = p_dxram.getService(NameserviceService.class);
		m_networkService = p_dxram.getService(NetworkService.class);
		m_bootService = p_dxram.getService(BootService.class);
		m_synchronizationService = p_dxram.getService(SynchronizationService.class);

		m_networkService.registerMessageType(BFSMessages.TYPE, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE,
				VerticesForNextFrontierMessage.class);

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
		if (m_chunkService.get(m_graphPartitionIndex) != 1) {
			m_loggerService.error(getClass(), "Getting graph partition index from chunk "
					+ ChunkID.toHexString(graphPartitionIndexChunkId) + " failed.");
			return -2;
		}

		// get entry vertices for bfs
		long chunkIdRootVertices = m_nameserviceService.getChunkID(m_bfsRootNameserviceEntry, 5000);
		if (chunkIdRootVertices == ChunkID.INVALID_ID) {
			m_loggerService.error(getClass(),
					"Getting BFS entry vertex " + m_bfsRootNameserviceEntry + " failed, not valid.");
			return -3;
		}

		GraphRootList rootList = new GraphRootList(chunkIdRootVertices);
		if (m_chunkService.get(rootList) != 1) {
			m_loggerService.error(getClass(),
					"Getting root list " + ChunkID.toHexString(chunkIdRootVertices) + " of vertices for bfs failed.");
			return -4;
		}

		int bfsIteration = 0;
		for (long root : rootList.getRoots()) {
			m_loggerService.info(getClass(), "Executing BFS with root " + ChunkID.toHexString(root));

			// run the bfs root on the node it is local to
			if (ChunkID.getCreatorID(root) == m_nodeId) {
				// run as bfs master
				BFSMaster master = new BFSMaster(root);
				master.init(m_graphPartitionIndex.getPartitionIndex(getSlaveId()).getVertexCount());
				master.execute(root);
				master.shutdown();

				BFSResult result = master.getBFSResult();

				m_loggerService.info(getClass(), "Result of BFS iteration: " + result);

				if (m_chunkService.create(result) != 1) {
					m_loggerService.error(getClass(), "Creating chunk for bfs result failed.");
					continue;
				}

				if (m_chunkService.put(result) != 1) {
					m_loggerService.error(getClass(), "Putting data of bfs result failed.");
					continue;
				}

				String resultName = MS_BFS_RESULT_NAMESRV_IDENT + bfsIteration;
				m_nameserviceService.register(result, resultName);
				m_loggerService.info(getClass(), "BFS results stored and registered: " + resultName);
			} else {
				// run as bfs slave, mater is the owner of the root node
				BFSSlave slave = new BFSSlave(ChunkID.getCreatorID(root));
				slave.init(m_graphPartitionIndex.getPartitionIndex(getSlaveId()).getVertexCount());
				slave.execute(ChunkID.INVALID_ID);
				slave.shutdown();
			}

			bfsIteration++;
			// TODO limit this to one iteration for now. fix later to allow multiple iterations
			break;
		}

		return 0;
	}

	@Override
	public void terminalCommandRegisterArguments(final ArgumentList p_argumentList) {
		p_argumentList.setArgument(MS_ARG_BFS_ROOT);
		p_argumentList.setArgument(MS_ARG_VERTEX_BATCH_SIZE);
		p_argumentList.setArgument(MS_ARG_VERTEX_MSG_BATCH_SIZE);
		p_argumentList.setArgument(MS_ARG_NUM_THREADS);
	}

	@Override
	public void terminalCommandCallbackForArguments(final ArgumentList p_argumentList) {
		m_bfsRootNameserviceEntry = p_argumentList.getArgumentValue(MS_ARG_BFS_ROOT, String.class);
		m_vertexBatchSize = p_argumentList.getArgumentValue(MS_ARG_VERTEX_BATCH_SIZE, Integer.class);
		m_vertexMessageBatchSize = p_argumentList.getArgumentValue(MS_ARG_VERTEX_MSG_BATCH_SIZE, Integer.class);
		m_numberOfThreadsPerNode = p_argumentList.getArgumentValue(MS_ARG_NUM_THREADS, Integer.class);
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		int size = super.exportObject(p_exporter, p_size);

		p_exporter.writeInt(m_bfsRootNameserviceEntry.length());
		p_exporter.writeBytes(m_bfsRootNameserviceEntry.getBytes(StandardCharsets.US_ASCII));
		p_exporter.writeInt(m_vertexBatchSize);
		p_exporter.writeInt(m_vertexMessageBatchSize);
		p_exporter.writeInt(m_numberOfThreadsPerNode);

		return size + Integer.BYTES + m_bfsRootNameserviceEntry.length() + Integer.BYTES * 3;
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

		return size + Integer.BYTES + m_bfsRootNameserviceEntry.length() + Integer.BYTES * 3;
	}

	@Override
	public int sizeofObject() {
		return super.sizeofObject() + Integer.BYTES + m_bfsRootNameserviceEntry.length() + Integer.BYTES * 3;
	}

	private abstract class AbstractBFSMS implements MessageReceiver {
		protected static final String MS_BARRIER_IDENT_0 = "BF0";
		protected static final String MS_BARRIER_IDENT_1 = "BF1";
		protected static final String MS_BARRIER_IDENT_2 = "BF2";

		protected int m_barrierId0 = BarrierID.INVALID_ID;
		protected int m_barrierId1 = BarrierID.INVALID_ID;
		protected int m_barrierId2 = BarrierID.INVALID_ID;

		private FrontierList m_curFrontier;
		private FrontierList m_nextFrontier;

		private BFSThread[] m_threads;

		public AbstractBFSMS() {

		}

		public void init(final long p_totalVertexCount) {
			m_curFrontier = new ConcurrentBitVector(p_totalVertexCount);
			m_nextFrontier = new ConcurrentBitVector(p_totalVertexCount);

			m_networkService.registerReceiver(VerticesForNextFrontierMessage.class, this);

			m_loggerService.info(getClass(), "Running BFS with " + m_numberOfThreadsPerNode + " threads on "
					+ p_totalVertexCount + " local vertices");

			m_threads = new BFSThread[m_numberOfThreadsPerNode];
			for (int i = 0; i < m_threads.length; i++) {
				m_threads[i] =
						new BFSThread(i, m_vertexBatchSize, m_vertexMessageBatchSize, m_curFrontier, m_nextFrontier);
				m_threads[i].start();
			}
		}

		public void execute(final long p_entryVertex) {
			if (p_entryVertex != ChunkID.INVALID_ID) {
				m_loggerService.info(getClass(),
						"I am starting BFS with entry vertex " + ChunkID.toHexString(p_entryVertex));
				m_curFrontier.pushBack(ChunkID.getLocalID(p_entryVertex));
			}

			int curBfsLevel = 0;
			long totalVisistedVertices = 0;
			while (true) {
				m_loggerService.debug(getClass(),
						"Processing next BFS level " + curBfsLevel + ", total vertices visited so far "
								+ totalVisistedVertices + "...");

				// kick off threads with current frontier
				for (int t = 0; t < m_threads.length; t++) {
					m_threads[t].runIteration();
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

				// all threads finished their iteration, sum up visited vertices
				long visitedVertsIteration = 0;
				for (int t = 0; t < m_threads.length; t++) {
					visitedVertsIteration += m_threads[t].getVisitedCountLastRun();
				}

				m_loggerService.info(getClass(),
						"BFS Level " + curBfsLevel + " finished with " + visitedVertsIteration + " visited vertices");

				totalVisistedVertices += visitedVertsIteration;

				// signal we are done with our iteration
				barrierSignalIterationComplete(visitedVertsIteration);

				// all nodes are finished, frontier swap
				FrontierList tmp = m_curFrontier;
				m_curFrontier = m_nextFrontier;
				m_nextFrontier = tmp;
				m_nextFrontier.reset();

				m_loggerService.debug(getClass(), "Frontier swap, new cur frontier size: " + m_curFrontier.size());

				// also swap the references of all threads!
				for (int t = 0; t < m_threads.length; t++) {
					m_threads[t].triggerFrontierSwap();
				}

				// signal frontier swap and ready for next iteration
				barrierSignalFrontierSwap(m_curFrontier.size());

				if (barrierSignalTerminate()) {
					m_loggerService.info(getClass(), "BFS terminated signal, last iteration level " + curBfsLevel
							+ ", total visited " + totalVisistedVertices);
					break;
				}

				m_loggerService.debug(getClass(), "Continue next BFS level");

				// go for next run
				curBfsLevel++;
			}
		}

		public void shutdown() {
			for (int i = 0; i < m_threads.length; i++) {
				m_threads[i].exitThread();
			}

			m_loggerService.debug(getClass(), "Joining BFS threads...");
			for (int i = 0; i < m_threads.length; i++) {
				try {
					m_threads[i].join();
				} catch (final InterruptedException e) {
				}
			}

			m_loggerService.debug(getClass(), "BFS shutdown");
		}

		@Override
		public void onIncomingMessage(final AbstractMessage p_message) {
			if (p_message != null) {
				if (p_message.getType() == BFSMessages.TYPE) {
					switch (p_message.getSubtype()) {
						case BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE:
							onIncomingVerticesForNextFrontierMessage((VerticesForNextFrontierMessage) p_message);
							break;
						default:
							break;
					}
				}
			}
		}

		private void onIncomingVerticesForNextFrontierMessage(final VerticesForNextFrontierMessage p_message) {
			int batchSize = p_message.getBatchSize();
			long[] data = p_message.getVertexIDBuffer();
			for (int i = 0; i < batchSize; i++) {
				// TODO optimization: do not send node id, because we already filtered it
				m_nextFrontier.pushBack(ChunkID.getLocalID(data[i]));
			}
		}

		protected abstract void barrierSignalIterationComplete(final long p_verticesVisited);

		protected abstract void barrierSignalFrontierSwap(final long p_nextFrontierSize);

		protected abstract boolean barrierSignalTerminate();
	}

	private class BFSMaster extends AbstractBFSMS {
		private BFSResult m_bfsResult;
		private int m_slaveBarrierCount;
		private boolean m_signalTermination;

		public BFSMaster(final long p_bfsEntryNode) {
			// don't count ourselves
			m_slaveBarrierCount = getSlaveNodeIds().length - 1;

			m_barrierId0 = m_synchronizationService.barrierAllocate(m_slaveBarrierCount + 1);
			m_barrierId1 = m_synchronizationService.barrierAllocate(m_slaveBarrierCount + 1);
			m_barrierId2 = m_synchronizationService.barrierAllocate(m_slaveBarrierCount + 1);

			m_nameserviceService.register(m_barrierId1, MS_BARRIER_IDENT_0 + getComputeGroupId());
			m_nameserviceService.register(m_barrierId2, MS_BARRIER_IDENT_1 + getComputeGroupId());
			m_nameserviceService.register(m_barrierId0, MS_BARRIER_IDENT_2 + getComputeGroupId());

			m_bfsResult = new BFSResult();
			m_bfsResult.setRootVertexID(p_bfsEntryNode);
		}

		public BFSResult getBFSResult() {
			return m_bfsResult;
		}

		@Override
		protected void barrierSignalIterationComplete(final long p_verticesVisited) {

			Pair<short[], long[]> result = m_synchronizationService.barrierSignOn(m_barrierId0, -1);

			long iterationVertsVisited = p_verticesVisited;
			// results of other slaves of last iteration
			for (long data : result.second()) {
				if (data >= 0) {
					iterationVertsVisited += data;
				}
			}

			m_bfsResult.setTotalVisitedVertices(m_bfsResult.getTotalVisitedVertices() + iterationVertsVisited);
			m_bfsResult.setTotalBFSDepth(m_bfsResult.getTotalBFSDepth() + 1);
		}

		@Override
		protected void barrierSignalFrontierSwap(final long p_nextFrontierSize) {
			Pair<short[], long[]> result = m_synchronizationService.barrierSignOn(m_barrierId1, -1);

			// check if all frontier sizes are 0 -> terminate bfs
			boolean allFrontiersEmpty = true;
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
			m_synchronizationService.barrierSignOn(m_barrierId1, m_signalTermination ? 1 : 0);

			return m_signalTermination;
		}
	}

	private class BFSSlave extends AbstractBFSMS {

		public BFSSlave(final short p_bfsMasterNodeID) {
			m_barrierId0 = (int) (m_nameserviceService.getChunkID(MS_BARRIER_IDENT_0, -1) & 0xFFFFFFFF);
			m_barrierId1 = (int) (m_nameserviceService.getChunkID(MS_BARRIER_IDENT_1, -1) & 0xFFFFFFFF);
			m_barrierId2 = (int) (m_nameserviceService.getChunkID(MS_BARRIER_IDENT_2, -1) & 0xFFFFFFFF);
		}

		@Override
		protected void barrierSignalIterationComplete(final long p_verticesVisited) {
			m_synchronizationService.barrierSignOn(m_barrierId0, p_verticesVisited);
		}

		@Override
		protected void barrierSignalFrontierSwap(final long p_nextFrontierSize) {
			m_synchronizationService.barrierSignOn(m_barrierId1, p_nextFrontierSize);
		}

		@Override
		protected boolean barrierSignalTerminate() {
			Pair<short[], long[]> result = m_synchronizationService.barrierSignOn(m_barrierId2, -1);
			// look for signal terminate flag (0 or 1)
			for (int i = 0; i < result.first().length; i++) {
				if (result.second()[i] == 1) {
					return true;
				}
			}

			return false;
		}
	}

	private class BFSThread extends Thread {

		private int m_id = -1;
		private int m_vertexBatchSize;
		private int m_vertexMessageBatchSize;
		private FrontierList m_curFrontier;
		private FrontierList m_nextFrontier;

		private short m_nodeId;
		private Vertex[] m_vertexBatch;
		private int m_currentIterationLevel;
		private HashMap<Short, VerticesForNextFrontierMessage> m_remoteMessages =
				new HashMap<Short, VerticesForNextFrontierMessage>();

		private volatile boolean m_runIteration;
		private volatile int m_visitedCounterRun;
		private volatile boolean m_exitThread;

		public BFSThread(final int p_id, final int p_vertexBatchSize, final int p_vertexMessageBatchSize,
				final FrontierList p_curFrontierShared, final FrontierList p_nextFrontierShared) {
			super("BFSThread-" + p_id);

			m_id = p_id;
			m_vertexBatchSize = p_vertexBatchSize;
			m_vertexMessageBatchSize = p_vertexMessageBatchSize;
			m_curFrontier = p_curFrontierShared;
			m_nextFrontier = p_nextFrontierShared;

			m_nodeId = m_bootService.getNodeID();
			m_vertexBatch = new Vertex[p_vertexBatchSize];
			for (int i = 0; i < m_vertexBatch.length; i++) {
				m_vertexBatch[i] = new Vertex(ChunkID.INVALID_ID);
			}
		}

		public void setCurrentBFSIterationLevel(final int p_iterationLevel) {
			m_currentIterationLevel = p_iterationLevel;
		}

		public void runIteration() {
			m_visitedCounterRun = 0;
			m_runIteration = true;
		}

		public boolean hasIterationFinished() {
			return !m_runIteration;
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
				for (int i = 0; i < m_vertexBatch.length; i++) {
					long tmp = m_curFrontier.popFront();
					if (tmp != -1) {
						m_vertexBatch[i].setID(ChunkID.getChunkID(m_nodeId, tmp));
						validVertsInBatch++;
					} else {
						if (validVertsInBatch == 0) {
							enterIdle = true;
							break;
						}
						m_vertexBatch[i].setID(ChunkID.INVALID_ID);
					}
				}

				if (validVertsInBatch == 0) {
					// make sure to send out remaining messages which have not reached the
					// batch size, yet (because they will never reach it in this round)
					for (Entry<Short, VerticesForNextFrontierMessage> entry : m_remoteMessages.entrySet()) {
						VerticesForNextFrontierMessage msg = entry.getValue();
						if (msg.getBatchSize() > 0) {
							if (m_networkService.sendMessage(msg) != NetworkErrorCodes.SUCCESS) {
								m_loggerService.error(getClass(), "Sending vertex message to node "
										+ NodeID.toHexString(msg.getDestination()) + " failed");
								return;
							}

							msg.setNumVerticesInBatch(0);
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

					if (vertex.getUserData() == -1) {
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
									msg = new VerticesForNextFrontierMessage(creatorId, m_vertexMessageBatchSize);
									m_remoteMessages.put(creatorId, msg);
								}

								// add vertex to message batch
								int idx = msg.getNumVerticesInBatch();
								msg.getVertexIDBuffer()[idx] = neighbour;
								msg.setNumVerticesInBatch(idx + 1);

								if (msg.getNumVerticesInBatch() == msg.getBatchSize()) {
									if (m_networkService.sendMessage(msg) != NetworkErrorCodes.SUCCESS) {
										m_loggerService.error(getClass(), "Sending vertex message to node "
												+ NodeID.toHexString(creatorId) + " failed");
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

				// TODO instead of writing back the full vertices, just write back the the iteration level (one int)

				// write back changes
				int put = m_chunkService.put(ChunkLockOperation.NO_LOCK_OPERATION, m_vertexBatch, 0, validVertsInBatch);
				if (put != writeBackCount) {
					m_loggerService.error(getClass(),
							"Putting vertices in BFS Thread " + m_id + " failed: " + put + " != " + writeBackCount);
					return;
				}
			}
		}
	}
}

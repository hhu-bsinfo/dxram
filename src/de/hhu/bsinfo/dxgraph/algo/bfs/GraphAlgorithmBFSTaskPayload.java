
package de.hhu.bsinfo.dxgraph.algo.bfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import de.hhu.bsinfo.dxcompute.coord.BarrierMaster;
import de.hhu.bsinfo.dxcompute.coord.BarrierSlave;
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
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;

public class GraphAlgorithmBFSTaskPayload extends AbstractTaskPayload {

	private LoggerService m_loggerService;
	private ChunkService m_chunkService;
	private NameserviceService m_nameserviceService;
	private NetworkService m_networkService;
	private BootService m_bootService;

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

		for (long root : rootList.getRoots()) {
			// run the bfs root on the node it is local to
			if (ChunkID.getCreatorID(root) == m_nodeId) {
				// run as bfs master
				BFSMaster master = new BFSMaster(root);
				master.init(m_graphPartitionIndex.getPartitionIndex(getSlaveId()).getVertexCount());
				master.execute(root);
				master.shutdown();

				BFSResult result = master.getBFSResult();
				System.out.println("BFSResults: " + result);
				// TODO write bfs results to chunk + nameservice entry
			} else {
				// run as bfs slave
				BFSSlave slave = new BFSSlave();
				slave.init(m_graphPartitionIndex.getPartitionIndex(getSlaveId()).getVertexCount());
				slave.execute(root);
				slave.shutdown();
			}

			// TODO run for multiple roots and have option to run also for a single root
			break;
		}

		return 0;
	}

	private abstract class AbstractBFSMS implements MessageReceiver {
		protected static final int MS_BARRIER_IDENT_0 = 0xBF50;
		protected static final int MS_BARRIER_IDENT_1 = 0xBF51;
		protected static final int MS_BARRIER_IDENT_2 = 0xBF52;

		private FrontierList m_curFrontier;
		private FrontierList m_nextFrontier;

		private BFSThread[] m_threads;

		public AbstractBFSMS() {

		}

		public void init(final long p_totalVertexCount) {
			m_curFrontier = new ConcurrentBitVector(p_totalVertexCount);
			m_nextFrontier = new ConcurrentBitVector(p_totalVertexCount);

			m_networkService.registerReceiver(VerticesForNextFrontierMessage.class, this);

			m_threads = new BFSThread[m_numberOfThreadsPerNode];
			for (int i = 0; i < m_threads.length; i++) {
				m_threads[i] =
						new BFSThread(i, m_vertexBatchSize, m_vertexMessageBatchSize, m_curFrontier, m_nextFrontier);
				m_threads[i].start();
			}
		}

		public void execute(final long p_entryVertex) {
			if (p_entryVertex != ChunkID.INVALID_ID) {
				m_curFrontier.pushBack(ChunkID.getLocalID(p_entryVertex));
			}

			int curBfsLevel = 0;
			long totalVisistedVertices = 0;
			while (true) {
				// kick off threads with current frontier
				for (int t = 0; t < m_threads.length; t++) {
					m_threads[t].runIteration();
				}

				// wait actively until threads are done with their current iteration
				for (int t = 0; t < m_threads.length; t++) {
					if (!m_threads[t].hasIterationFinished()) {
						t = 0;
						Thread.yield();
					}
				}

				// all threads finished their iteration, sum up visited vertices
				long visitedVertsIteration = 0;
				for (int t = 0; t < m_threads.length; t++) {
					visitedVertsIteration += m_threads[t].getVisitedCountLastRun();
				}

				totalVisistedVertices += visitedVertsIteration;

				// signal we are done with our iteration
				barrierSignalIterationComplete(visitedVertsIteration);

				// all nodes are finished, frontier swap
				FrontierList tmp = m_curFrontier;
				m_curFrontier = m_nextFrontier;
				m_nextFrontier = tmp;
				m_nextFrontier.reset();

				// also swap the references of all threads!
				for (int t = 0; t < m_threads.length; t++) {
					m_threads[t].triggerFrontierSwap();
				}

				// signal frontier swap and ready for next iteration
				barrierSignalFrontierSwap(m_curFrontier.size());

				if (barrierSignalTerminate()) {
					break;
				}

				// go for next run
				curBfsLevel++;
			}
		}

		public void shutdown() {
			for (int i = 0; i < m_threads.length; i++) {
				m_threads[i].exitThread();
			}

			m_loggerService.info(getClass(), "Joining BFS threads...");
			for (int i = 0; i < m_threads.length; i++) {
				try {
					m_threads[i].join();
				} catch (final InterruptedException e) {}
			}
		}

		@Override
		public void onIncomingMessage(final AbstractMessage p_message) {
			if (p_message != null) {
				if (p_message.getType() == ChunkMessages.TYPE) {
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
				m_nextFrontier.pushBack(data[i]);
			}
		}

		protected abstract void barrierSignalIterationComplete(final long p_verticesVisited);

		protected abstract void barrierSignalFrontierSwap(final long p_nextFrontierSize);

		protected abstract boolean barrierSignalTerminate();
	}

	private class BFSMaster extends AbstractBFSMS {
		private BarrierMaster m_barrier;
		private BFSResult m_bfsResult;
		private int m_slaveBarrierCount;
		private boolean m_signalTermination;

		public BFSMaster(final long p_bfsEntryNode) {
			m_barrier = new BarrierMaster(m_networkService, m_loggerService);
			m_bfsResult = new BFSResult();
			m_bfsResult.setRootVertexID(p_bfsEntryNode);

			// don't count ourselves
			m_slaveBarrierCount = getSlaveNodeIds().length - 1;
		}

		public BFSResult getBFSResult() {
			return m_bfsResult;
		}

		@Override
		protected void barrierSignalIterationComplete(final long p_verticesVisited) {

			m_barrier.execute(m_slaveBarrierCount, MS_BARRIER_IDENT_0, -1);
			ArrayList<Pair<Short, Long>> barrierData = m_barrier.getBarrierData();

			long iterationVertsVisited = p_verticesVisited;
			// results of other slaves of last iteration
			for (Pair<Short, Long> data : barrierData) {
				iterationVertsVisited += data.second();
			}

			m_bfsResult.setTotalVisitedVertices(m_bfsResult.getTotalVisitedVertices() + iterationVertsVisited);
		}

		@Override
		protected void barrierSignalFrontierSwap(final long p_nextFrontierSize) {
			m_barrier.execute(m_slaveBarrierCount, MS_BARRIER_IDENT_1, -1);
			ArrayList<Pair<Short, Long>> barrierData = m_barrier.getBarrierData();

			boolean allTerminated = true;
			for (Pair<Short, Long> data : barrierData) {
				if (data.second() > 0) {
					allTerminated = false;
					break;
				}
			}

			if (allTerminated) {
				m_signalTermination = true;
			}
		}

		@Override
		protected boolean barrierSignalTerminate() {
			m_barrier.execute(m_slaveBarrierCount, MS_BARRIER_IDENT_2, m_signalTermination ? 1 : 0);

			return m_signalTermination;
		}
	}

	private class BFSSlave extends AbstractBFSMS {
		private BarrierSlave m_barrier;
		private short m_barrierMasterNodeID;

		public BFSSlave() {
			m_barrier = new BarrierSlave(m_networkService, m_loggerService);
			m_barrierMasterNodeID = getSlaveNodeIds()[0];
		}

		@Override
		protected void barrierSignalIterationComplete(long p_verticesVisited) {
			m_barrier.execute(m_barrierMasterNodeID, MS_BARRIER_IDENT_0, p_verticesVisited);
		}

		@Override
		protected void barrierSignalFrontierSwap(long p_nextFrontierSize) {
			m_barrier.execute(m_barrierMasterNodeID, MS_BARRIER_IDENT_0, p_nextFrontierSize);
		}

		@Override
		protected boolean barrierSignalTerminate() {
			m_barrier.execute(m_barrierMasterNodeID, MS_BARRIER_IDENT_0, -1);
			return m_barrier.getBarrierData() > 0;
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

					if (vertex.getUserData() != -1) {
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
								}

								// add vertex to message batch
								int idx = msg.getNumVerticesInBatch();
								msg.getVertexIDBuffer()[idx] = neighbour;
								msg.setNumVerticesInBatch(idx);

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

/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxgraph.algo.bfs;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraph.DXGraphMessageTypes;
import de.hhu.bsinfo.dxgraph.algo.bfs.front.ConcurrentBitVectorHybrid;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSMessages;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSResultMessage;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.BFSTerminateMessage;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.PingMessage;
import de.hhu.bsinfo.dxgraph.algo.bfs.messages.VerticesForNextFrontierMessage;
import de.hhu.bsinfo.dxgraph.data.BFSResult;
import de.hhu.bsinfo.dxgraph.data.GraphPartitionIndex;
import de.hhu.bsinfo.dxgraph.data.GraphRootList;
import de.hhu.bsinfo.dxgraph.data.VertexSimple;
import de.hhu.bsinfo.dxgraph.load.GraphLoadBFSRootListTask;
import de.hhu.bsinfo.dxgraph.load.GraphLoadPartitionIndexTask;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkMemoryService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Compute task to run BFS on a loaded graph.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.05.2016
 */
public class GraphAlgorithmBFSTask implements Task {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphAlgorithmBFSTask.class.getSimpleName());

    private static final String MS_BARRIER_IDENT_0 = "BF0";

    @Expose
    private String m_bfsRootNameserviceEntry = GraphLoadBFSRootListTask.MS_BFS_ROOTS + '0';
    @Expose
    private int m_vertexBatchSize = 100;
    @Expose
    private int m_vertexMessageBatchSize = 100;
    @Expose
    private int m_numberOfThreadsPerNode = 4;
    @Expose
    private boolean m_markVertices = true;
    @Expose
    private boolean m_beamerMode = true;
    @Expose
    private int m_beamerFormulaGraphEdgeDeg = 16;
    @Expose
    private boolean m_abortBFSOnError = true;

    private TaskContext m_ctx;
    private ChunkService m_chunkService;
    private ChunkMemoryService m_chunkMemoryService;
    private NameserviceService m_nameserviceService;
    private NetworkService m_networkService;
    private BootService m_bootService;
    private SynchronizationService m_synchronizationService;
    private TemporaryStorageService m_temporaryStorageService;

    private short m_nodeId = NodeID.INVALID_ID;
    private GraphPartitionIndex m_graphPartitionIndex;

    private BFS m_curBFS;
    private Lock m_signalLock = new ReentrantLock(false);
    private volatile boolean m_signalAbortTriggered = false;

    private int m_barrierId0 = BarrierID.INVALID_ID;

    /**
     * Default constructor
     */
    public GraphAlgorithmBFSTask() {

    }

    /**
     * Constructor
     *
     * @param p_bfsRootNameserviceEntry
     *         Name of the nameservice entry for the roots to use for BFS
     * @param p_vertexBatchSize
     *         Number of vertices to cache as a batch for processing
     * @param p_vertexMessageBatchSize
     *         Name of vertices to send as a single batch over the network
     * @param p_numberOfThreadsPerNode
     *         Number of threads to use for BFS on a single node
     * @param p_markVertices
     *         Mark the actual vertices/data visited with the level. On false, we just remember if we have visited it
     * @param p_beamerMode
     *         Run the BFS algorithm with bottom up optimized mode (beamer). False to run top-down approach only
     * @param p_beamerFormulaGraphEdgeDeg
     *         Avg. edge degree of the graph (parameter on typical rmat generators). Used to determine top down <-> bottom up switching on beamer mode
     * @param p_abortBFSOnError
     *         Abort BFS execution on error or continue even on errors
     */
    public GraphAlgorithmBFSTask(final String p_bfsRootNameserviceEntry, final int p_vertexBatchSize, final int p_vertexMessageBatchSize,
            final int p_numberOfThreadsPerNode, final boolean p_markVertices, final boolean p_beamerMode, final int p_beamerFormulaGraphEdgeDeg,
            final boolean p_abortBFSOnError) {
        m_bfsRootNameserviceEntry = p_bfsRootNameserviceEntry;
        m_vertexBatchSize = p_vertexBatchSize;
        m_vertexMessageBatchSize = p_vertexMessageBatchSize;
        m_numberOfThreadsPerNode = p_numberOfThreadsPerNode;
        m_markVertices = p_markVertices;
        m_beamerMode = p_beamerMode;
        m_beamerFormulaGraphEdgeDeg = p_beamerFormulaGraphEdgeDeg;
        m_abortBFSOnError = p_abortBFSOnError;
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        m_ctx = p_ctx;
        m_chunkService = m_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        m_chunkMemoryService = m_ctx.getDXRAMServiceAccessor().getService(ChunkMemoryService.class);
        m_nameserviceService = m_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);
        m_networkService = m_ctx.getDXRAMServiceAccessor().getService(NetworkService.class);
        m_bootService = m_ctx.getDXRAMServiceAccessor().getService(BootService.class);
        m_synchronizationService = m_ctx.getDXRAMServiceAccessor().getService(SynchronizationService.class);
        m_temporaryStorageService = m_ctx.getDXRAMServiceAccessor().getService(TemporaryStorageService.class);

        m_networkService.registerMessageType(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE,
                VerticesForNextFrontierMessage.class);
        m_networkService.registerMessageType(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_TERMINATE_MESSAGE, BFSTerminateMessage.class);
        m_networkService.registerMessageType(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_RESULT_MESSAGE, BFSResultMessage.class);

        // cache node id
        m_nodeId = m_bootService.getNodeID();

        // TODO workaround for network issue creating multiple socket connections
        {
            m_networkService.registerMessageType(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_PING_MESSAGE, PingMessage.class);

            // sync before running
            m_synchronizationService.barrierSignOn(m_barrierId0, -1);

            {
                try {
                    Thread.sleep(m_ctx.getCtxData().getSlaveId() * 500);
                } catch (final InterruptedException ignore) {
                }

                // send ping message to force open socket connections in order
                for (short slave : m_ctx.getCtxData().getSlaveNodeIds()) {
                    if (slave != m_nodeId) {
                        try {
                            m_networkService.sendMessage(new PingMessage(slave));
                        } catch (final NetworkException e) {

                        }
                    }
                }

                m_synchronizationService.barrierSignOn(m_barrierId0, -1);
            }
        }

        // get partition index of the graph
        long graphPartitionIndexChunkId =
                m_nameserviceService.getChunkID(GraphLoadPartitionIndexTask.MS_PART_INDEX_IDENT + m_ctx.getCtxData().getComputeGroupId(), 5000);
        if (graphPartitionIndexChunkId == ChunkID.INVALID_ID) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot find graph partition index for compute group %d", m_ctx.getCtxData().getComputeGroupId());
            // #endif /* LOGGER >= ERROR */
            return -1;
        }

        m_graphPartitionIndex = new GraphPartitionIndex();
        m_graphPartitionIndex.setID(graphPartitionIndexChunkId);
        if (!m_temporaryStorageService.get(m_graphPartitionIndex)) {
            // #if LOGGER >= ERROR
            LOGGER.error("Getting graph partition index from temporary memory chunk 0x%X failed", graphPartitionIndexChunkId);
            // #endif /* LOGGER >= ERROR */
            return -2;
        }

        // get entry vertices for bfs
        long tmpStorageIdRootVertices = m_nameserviceService.getChunkID(m_bfsRootNameserviceEntry, 5000);
        if (tmpStorageIdRootVertices == ChunkID.INVALID_ID) {
            // #if LOGGER >= ERROR
            LOGGER.error("Getting BFS entry vertex %s failed, not valid", m_bfsRootNameserviceEntry);
            // #endif /* LOGGER >= ERROR */
            return -3;
        }

        GraphRootList rootList = new GraphRootList(tmpStorageIdRootVertices);
        if (!m_temporaryStorageService.get(rootList)) {
            // #if LOGGER >= ERROR
            LOGGER.error("Getting root list 0x%X of vertices for bfs from temporary storage failed", tmpStorageIdRootVertices);
            // #endif /* LOGGER >= ERROR */
            return -4;
        }

        {
            List<GarbageCollectorMXBean> gcList = ManagementFactory.getGarbageCollectorMXBeans();
            for (GarbageCollectorMXBean gc : gcList) {
                if (gc.getCollectionCount() > 0 || gc.getCollectionTime() > 0) {
                    // #if LOGGER >= INFO
                    LOGGER.info("Using garbage collector %s for BFS", gc.getName());
                    // #endif /* LOGGER >= INFO */
                }
            }
        }
        // create barriers for bfs and register
        // or get the newly created barriers
        if (m_ctx.getCtxData().getSlaveId() == 0) {
            m_barrierId0 = m_synchronizationService.barrierAllocate(m_ctx.getCtxData().getSlaveNodeIds().length);

            m_nameserviceService.register(ChunkID.getChunkID(m_nodeId, m_barrierId0), MS_BARRIER_IDENT_0 + m_ctx.getCtxData().getComputeGroupId());
        } else {
            m_barrierId0 = (int) m_nameserviceService.getChunkID(MS_BARRIER_IDENT_0 + m_ctx.getCtxData().getComputeGroupId(), -1);
        }

        // #if LOGGER >= INFO
        if (m_markVertices) {
            LOGGER.info("Marking vertices mode (graph data will be altered)");
        } else {
            LOGGER.info("Not marking vertices mode (graph data read only)");
        }
        // #endif /* LOGGER >= INFO */

        // #if LOGGER >= INFO
        LOGGER.info("BFS mode: %s", m_beamerMode ? "BEAMER" : "TOP DOWN ONLY");
        // #endif /* LOGGER >= INFO */

        int iteration = 0;
        for (long root : rootList.getRoots()) {
            if (m_signalAbortTriggered) {
                break;
            }

            // #if LOGGER >= INFO
            LOGGER.info("Triggering GC...");
            // #endif /* LOGGER >= INFO */
            System.gc();
            try {
                Thread.sleep(2000);
            } catch (final InterruptedException ignored) {
            }

            // #if LOGGER >= INFO
            LOGGER.info("Executing BFS with root 0x%X", root);
            // #endif /* LOGGER >= INFO */

            m_signalLock.lock();
            m_curBFS = new BFS(root);
            m_signalLock.unlock();

            m_curBFS.init(m_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId()).getVertexCount(), m_markVertices);
            if (ChunkID.getCreatorID(root) == m_nodeId) {
                m_curBFS.execute(root);
            } else {
                m_curBFS.execute(ChunkID.INVALID_ID);
            }

            System.out.println("Local results of iteration " + iteration + ":\n" + m_curBFS.getBFSResult());

            m_signalLock.lock();
            // check if an abort signal has killed the run
            if (m_curBFS != null) {
                m_curBFS.shutdown();
                m_curBFS = null;
            }
            m_signalLock.unlock();

            // limit this to a single iteration on marking vertices
            // because we altered the vertex data, further iterations won't work (vertices already marked as visited)
            if (m_markVertices) {
                break;
            }

            iteration++;
        }

        // free barriers
        if (m_ctx.getCtxData().getSlaveId() == 0) {
            m_synchronizationService.barrierFree(m_barrierId0);
        } else {
            m_barrierId0 = BarrierID.INVALID_ID;
        }

        if (m_signalAbortTriggered) {
            return -5;
        } else {
            return 0;
        }
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        if (p_signal == Signal.SIGNAL_ABORT) {
            m_signalAbortTriggered = true;
            m_signalLock.lock();
            if (m_curBFS != null) {
                m_curBFS.shutdown();
                m_curBFS = null;
            }
            m_signalLock.unlock();
        }
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_bfsRootNameserviceEntry) + Integer.BYTES * 3 + 2 * ObjectSizeUtil.sizeofBoolean() + Integer.BYTES +
                ObjectSizeUtil.sizeofBoolean();
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_bfsRootNameserviceEntry);
        p_exporter.writeInt(m_vertexBatchSize);
        p_exporter.writeInt(m_vertexMessageBatchSize);
        p_exporter.writeInt(m_numberOfThreadsPerNode);
        p_exporter.writeBoolean(m_markVertices);
        p_exporter.writeBoolean(m_beamerMode);
        p_exporter.writeInt(m_beamerFormulaGraphEdgeDeg);
        p_exporter.writeBoolean(m_abortBFSOnError);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_bfsRootNameserviceEntry = p_importer.readString(m_bfsRootNameserviceEntry);
        m_vertexBatchSize = p_importer.readInt(m_vertexBatchSize);
        m_vertexMessageBatchSize = p_importer.readInt(m_vertexMessageBatchSize);
        m_numberOfThreadsPerNode = p_importer.readInt(m_numberOfThreadsPerNode);
        m_markVertices = p_importer.readBoolean(m_markVertices);
        m_beamerMode = p_importer.readBoolean(m_beamerMode);
        m_beamerFormulaGraphEdgeDeg = p_importer.readInt(m_beamerFormulaGraphEdgeDeg);
        m_abortBFSOnError = p_importer.readBoolean(m_abortBFSOnError);
    }

    /**
     * Base class for the master/slave BFS roles. Every BFS instance can be used for a single
     * BFS run, only.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.05.2016
     */
    private class BFS implements MessageReceiver {
        private BFSResult m_bfsLocalResult;

        private ConcurrentBitVectorHybrid m_curFrontier;
        private ConcurrentBitVectorHybrid m_nextFrontier;
        private ConcurrentBitVectorHybrid m_visitedFrontier;
        private boolean m_markVisited;

        private BFSThread[] m_threads;
        private StatisticsThread m_statisticsThread;

        private SyncBFSFinished m_syncBFSFinished;

        private AtomicInteger m_bfsSlavesEmptyNextFrontiers = new AtomicInteger(0);
        private volatile boolean m_terminateBfs = true;
        private ReentrantReadWriteLock m_remoteDelegatesForNextFrontier = new ReentrantReadWriteLock(false);

        private AtomicLong m_edgeCountNextFrontier = new AtomicLong(0);

        private AtomicLong m_nextFrontVertices = new AtomicLong(0);
        private AtomicLong m_nextFrontEdges = new AtomicLong(0);

        /**
         * Constructor
         *
         * @param p_bfsEntryNode
         *         Root entry node to start the BFS at.
         */
        BFS(final long p_bfsEntryNode) {
            m_bfsLocalResult = new BFSResult();
            m_bfsLocalResult.m_rootVertexId = p_bfsEntryNode;
            GraphPartitionIndex.Entry entry = m_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId());
            m_bfsLocalResult.m_graphPartitionSizeVertices = entry.getVertexCount();
            m_bfsLocalResult.m_graphPartitionSizeEdges = entry.getEdgeCount();
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
         * @param p_totalVertexCount
         *         Total vertex count of the graph.
         * @param p_verticesMarkVisited
         *         False to not alter the graph data stored and use a local list to
         *         remember visited vertices, true to alter graph data and store visited
         *         information with the graph.
         */
        void init(final long p_totalVertexCount, final boolean p_verticesMarkVisited) {
            m_curFrontier = new ConcurrentBitVectorHybrid(p_totalVertexCount, 1);
            m_nextFrontier = new ConcurrentBitVectorHybrid(p_totalVertexCount, 1);
            m_visitedFrontier = new ConcurrentBitVectorHybrid(p_totalVertexCount, 1);

            m_markVisited = p_verticesMarkVisited;

            m_networkService.registerReceiver(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE, this);
            m_networkService.registerReceiver(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_TERMINATE_MESSAGE, this);
            m_networkService.registerReceiver(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_RESULT_MESSAGE, this);

            m_statisticsThread = new StatisticsThread();

            m_syncBFSFinished = new SyncBFSFinished(m_ctx.getCtxData().getSlaveNodeIds(), m_ctx.getCtxData().getSlaveNodeIds()[m_ctx.getCtxData().getSlaveId()],
                    m_networkService);

            // #if LOGGER >= INFO
            LOGGER.info("Running BFS with %d threads on 0x%X local vertices", m_numberOfThreadsPerNode, p_totalVertexCount);
            // #endif /* LOGGER >= INFO */

            m_threads = new BFSThread[m_numberOfThreadsPerNode];
            for (int i = 0; i < m_threads.length; i++) {
                m_threads[i] = new BFSThread(i, m_vertexBatchSize, m_vertexMessageBatchSize, m_curFrontier, m_nextFrontier, m_visitedFrontier,
                        m_statisticsThread.m_sharedVertexCounter, m_statisticsThread.m_sharedEdgeCounter, m_syncBFSFinished);
                m_threads[i].start();
            }

            // sync before running
            m_synchronizationService.barrierSignOn(m_barrierId0, -1);
        }

        /**
         * Execute one full BFS run.
         *
         * @param p_entryVertex
         *         Root vertex id to start BFS at.
         */
        void execute(final long p_entryVertex) {
            boolean bottomUpApproach = false;
            long numEdgesInNextFrontier;

            // values to calculate top down <-> bottom up switching
            long fullGraphVertexCount = m_graphPartitionIndex.calcTotalVertexCount();
            long fullGraphEdgeCount = m_graphPartitionIndex.calcTotalEdgeCount();
            long fullGraphNextFrontVertexCount = 0;
            long fullGraphNextFrontEdgeCount = 0;

            if (p_entryVertex != ChunkID.INVALID_ID) {
                // #if LOGGER >= INFO
                LOGGER.info("I am starting BFS with entry vertex 0x%X", p_entryVertex);
                // #endif /* LOGGER >= INFO */

                VertexSimple vertex = new VertexSimple(p_entryVertex);
                if (m_chunkService.get(vertex) != 1) {
                    LOGGER.error("Getting root vertex 0x%X failed", p_entryVertex);
                    // signal all other slaves to terminate (error)
                    m_ctx.getSignalInterface().sendSignalToMaster(Signal.SIGNAL_ABORT);
                    return;
                }

                // mark root visited
                if (m_markVisited) {
                    // mark data mode
                    if (!m_chunkMemoryService.writeInt(vertex.getID(), 0, m_bfsLocalResult.m_totalBFSDepth)) {
                        LOGGER.error("Marking root vertex 0x%X failed", p_entryVertex);
                        m_ctx.getSignalInterface().sendSignalToMaster(Signal.SIGNAL_ABORT);
                        return;
                    }
                }

                m_visitedFrontier.pushBack(ChunkID.getLocalID(p_entryVertex));
                m_visitedFrontier.popFrontReset();

                numEdgesInNextFrontier = vertex.getNeighbours().length;

                m_curFrontier.pushBack(ChunkID.getLocalID(p_entryVertex));

                fullGraphNextFrontVertexCount++;
                fullGraphNextFrontEdgeCount += numEdgesInNextFrontier;
                m_bfsLocalResult.m_totalVisitedEdges += numEdgesInNextFrontier;
            }

            m_curFrontier.popFrontReset();

            // root already done
            m_bfsLocalResult.m_totalBFSDepth++;
            m_statisticsThread.start();

            while (true) {
                if (m_beamerMode) {
                    // determine bfs approach for next iteration
                    // formula taken from beamer's "Distributed Memory Breadth-First Search Revisited:
                    // Enabling Bottom-Up Search"
                    if (bottomUpApproach) {
                        // last iteration was bottom up approach, check if we should switch
                        if (fullGraphNextFrontVertexCount < fullGraphVertexCount / 14 * m_beamerFormulaGraphEdgeDeg) {
                            bottomUpApproach = false;
                        }
                    } else {
                        // last iteration was top down approach, check if we should switch
                        if (fullGraphNextFrontEdgeCount > fullGraphEdgeCount / 10) {
                            bottomUpApproach = true;
                        }
                    }
                }

                LOGGER.info("BFS iteration %s, curFront size: %d, numEdgesInFrontier %d, vertex count %d, edge count %d",
                        bottomUpApproach ? "BOTTOM UP" : "TOP DOWN", fullGraphNextFrontVertexCount, fullGraphNextFrontEdgeCount, fullGraphVertexCount,
                        fullGraphEdgeCount);

                // kick off threads with current frontier
                for (BFSThread thread : m_threads) {
                    thread.runIteration(bottomUpApproach);
                }

                // reset local counter
                numEdgesInNextFrontier = 0;

                // wait actively until threads are done with their current iteration
                {
                    int t = 0;
                    while (t < m_threads.length) {
                        if (!m_threads[t].hasIterationFinished()) {
                            try {
                                Thread.sleep(2);
                            } catch (final InterruptedException ignored) {
                            }
                            if (m_signalAbortTriggered) {
                                return;
                            }

                            continue;
                        }
                        numEdgesInNextFrontier += m_threads[t].getEdgeCountNextFrontier();
                        t++;
                    }
                }

                // #if LOGGER >= INFO
                LOGGER.info("BFS Level %d finished, verts %d, edges %d so far visited/traversed", m_bfsLocalResult.m_totalBFSDepth,
                        m_statisticsThread.getTotalVertexCount(), m_statisticsThread.getTotalEdgeCount());
                // #endif /* LOGGER >= INFO */

                // --------------------------------
                // barrier

                if (!m_syncBFSFinished.execute()) {
                    // abort execution and have the master send a kill signal
                    // for this task to all other slaves
                    m_ctx.getSignalInterface().sendSignalToMaster(Signal.SIGNAL_ABORT);
                    LOGGER.error("Execution BFS level finished sync step failed");
                    return;
                }

                // --------------------------------

                // evaluate our own frontier first
                if (!m_nextFrontier.isEmpty()) {
                    m_terminateBfs = false;
                }

                // also get counter for remote incoming edges
                numEdgesInNextFrontier += m_edgeCountNextFrontier.get();
                m_edgeCountNextFrontier.set(0);

                m_bfsLocalResult.m_totalVisitedEdges += numEdgesInNextFrontier;

                // --------------------------------
                // barrier
                {
                    m_nextFrontVertices.addAndGet(m_nextFrontier.size());
                    m_nextFrontEdges.addAndGet(numEdgesInNextFrontier);

                    // inform all other slaves about our next frontier size to determine termination
                    short ownId = m_ctx.getCtxData().getSlaveId();
                    short[] slavesNodeIds = m_ctx.getCtxData().getSlaveNodeIds();
                    for (int i = 0; i < slavesNodeIds.length; i++) {
                        if (i != ownId) {
                            BFSTerminateMessage msg = new BFSTerminateMessage(slavesNodeIds[i], m_nextFrontier.size(), numEdgesInNextFrontier);
                            // System.out.println("<<<<< BFSTerminateMessage " + NodeID.toHexString(slavesNodeIds[i]));
                            try {
                                m_networkService.sendMessage(msg);
                            } catch (final NetworkException e) {
                                LOGGER.error("Sending bfs terminate message to 0x%X failed: %s", slavesNodeIds[i], e);
                                // abort execution and have the master send a kill signal
                                // for this task to all other slaves
                                m_ctx.getSignalInterface().sendSignalToMaster(Signal.SIGNAL_ABORT);
                                return;
                            }
                            // System.out.println("<<<<<??? BFSTerminateMessage " +
                            // NodeID.toHexString(slavesNodeIds[i]));
                        }
                    }

                    // wait until everyone reported the next frontier state/termination
                    // System.out.println("!!!!! " + getSlaveNodeIds().length);
                    while (!m_bfsSlavesEmptyNextFrontiers.compareAndSet(m_ctx.getCtxData().getSlaveNodeIds().length - 1, 0)) {
                        try {
                            Thread.sleep(2);
                        } catch (InterruptedException e) {
                        }
                    }

                    fullGraphNextFrontVertexCount = m_nextFrontVertices.getAndSet(0);
                    fullGraphNextFrontEdgeCount = m_nextFrontEdges.getAndSet(0);
                }
                // --------------------------------

                // check if we are done
                if (m_terminateBfs) {
                    // #if LOGGER >= INFO
                    LOGGER.info("BFS terminated signal, last iteration level %d, total edges traversed %d, num vertices of " + "graph visited %d",
                            m_bfsLocalResult.m_totalBFSDepth, m_bfsLocalResult.m_totalVisitedVertices, m_visitedFrontier.size());
                    // #endif /* LOGGER >= INFO */

                    break;
                }
                // reset for next run
                m_terminateBfs = true;

                // don't allow remote nodes to add delegates to our frontiers to avoid
                // data race on frontier swap
                m_remoteDelegatesForNextFrontier.writeLock().lock();

                // all nodes are finished, frontier swap
                ConcurrentBitVectorHybrid tmp = m_curFrontier;
                m_curFrontier = m_nextFrontier;
                m_nextFrontier = tmp;
                m_nextFrontier.reset();
                m_curFrontier.popFrontReset();
                m_visitedFrontier.popFrontReset();

                // also swap the references of all threads!
                for (BFSThread thread : m_threads) {
                    thread.triggerFrontierSwap();
                    thread.setCurrentBFSDepthLevel(m_bfsLocalResult.m_totalBFSDepth);
                }

                // now we are good, allow new delegates from remotes
                m_remoteDelegatesForNextFrontier.writeLock().unlock();

                // #if LOGGER >= DEBUG
                LOGGER.debug("Frontier swap, new cur frontier size: %d", m_curFrontier.size());
                // #endif /* LOGGER >= DEBUG */

                // go for next run
                m_bfsLocalResult.m_totalBFSDepth++;
            }

            m_statisticsThread.shutdown();
            try {
                m_statisticsThread.join();
            } catch (final InterruptedException ignored) {
            }

            // collect further results
            m_bfsLocalResult.m_graphFullSizeVertices = fullGraphVertexCount;
            m_bfsLocalResult.m_graphFullSizeEdges = fullGraphEdgeCount;
            m_bfsLocalResult.m_graphPartitionSizeVertices = m_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId()).getVertexCount();
            m_bfsLocalResult.m_graphPartitionSizeEdges = m_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId()).getEdgeCount();
            m_bfsLocalResult.m_totalVisitedVertices = m_visitedFrontier.size();
            m_bfsLocalResult.m_totalVerticesTraversed = m_statisticsThread.getTotalVertexCount();
            m_bfsLocalResult.m_totalEdgesTraversed = m_statisticsThread.getTotalEdgeCount();
            m_bfsLocalResult.m_maxTraversedVertsPerSecond = m_statisticsThread.getMaxVerticesVisitedPerSec();
            m_bfsLocalResult.m_maxTraversedEdgesPerSecond = m_statisticsThread.getMaxEdgesTraversedPerSec();
            m_bfsLocalResult.m_avgTraversedVertsPerSecond = m_statisticsThread.getAvgVerticesPerSec();
            m_bfsLocalResult.m_avgTraversedEdgesPerSecond = m_statisticsThread.getAvgEdgesPerSec();
            m_bfsLocalResult.m_totalTimeMs = m_statisticsThread.getTotalTimeMs();

            m_statisticsThread = null;
        }

        /**
         * Shutdown the instance and cleanup resources used for the BFS run.
         */
        void shutdown() {
            // thread not shut down if execution was aborted
            if (m_statisticsThread != null) {
                m_statisticsThread.shutdown();
                try {
                    m_statisticsThread.join();
                } catch (final InterruptedException ignored) {
                }

                m_statisticsThread = null;
            }

            for (BFSThread thread : m_threads) {
                thread.exitThread();
            }

            // #if LOGGER >= DEBUG
            LOGGER.debug("Joining BFS threads...");
            // #endif /* LOGGER >= DEBUG */

            for (BFSThread thread : m_threads) {
                try {
                    thread.join();
                } catch (final InterruptedException ignored) {
                }
            }

            m_syncBFSFinished.cleanup();

            m_networkService.unregisterReceiver(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE, this);
            m_networkService.unregisterReceiver(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_TERMINATE_MESSAGE, this);
            m_networkService.unregisterReceiver(DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_RESULT_MESSAGE, this);

            // #if LOGGER >= DEBUG
            LOGGER.debug("BFS shutdown");
            // #endif /* LOGGER >= DEBUG */
        }

        @Override
        public void onIncomingMessage(final Message p_message) {
            if (p_message != null) {
                if (p_message.getType() == DXGraphMessageTypes.BFS_MESSAGES_TYPE) {
                    switch (p_message.getSubtype()) {
                        case BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE:
                            onIncomingVerticesForNextFrontierMessage((VerticesForNextFrontierMessage) p_message);
                            break;
                        case BFSMessages.SUBTYPE_BFS_TERMINATE_MESSAGE:
                            onIncomingBFSTerminateMessage((BFSTerminateMessage) p_message);
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        /**
         * Handle incoming VerticesForNextFrontierMessage messages.
         *
         * @param p_message
         *         VerticesForNextFrontierMessage to handle
         */
        private void onIncomingVerticesForNextFrontierMessage(final VerticesForNextFrontierMessage p_message) {

            // this determines whether the message comes from a node running top down or bottom up approach
            if (p_message.getNumNeighborsInBatch() > 0) {
                // bottom up approach
                VerticesForNextFrontierMessage reply = new VerticesForNextFrontierMessage(p_message.getSource(), m_vertexBatchSize);
                long vertexId = p_message.getVertex();
                long neighborId = p_message.getNeighbor();
                while (vertexId != -1) {
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
                    try {
                        m_networkService.sendMessage(reply);
                    } catch (final NetworkException e) {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Sending reply for bottom up vertices next frontier failed: %s", e);
                        // #endif /* LOGGER >= ERROR */
                    }

                    m_syncBFSFinished.incrementSentVertexMsgCountLocal();
                }
            } else {
                // top down approach

                // check if we are allowed to add, not allowed on termination ->
                // block messages that are already sent by a node starting the next iteration
                // but the current node is not ready, yet
                m_remoteDelegatesForNextFrontier.readLock().lock();

                long vertexId = p_message.getVertex();
                while (vertexId != -1) {
                    // check if visited and add to frontier if not
                    long localId = ChunkID.getLocalID(vertexId);
                    if (m_visitedFrontier.pushBack(localId)) {
                        m_nextFrontier.pushBack(localId);

                        if (m_markVisited) {
                            m_chunkMemoryService.writeInt(vertexId, 0, m_bfsLocalResult.m_totalBFSDepth);
                        }

                        // read num of edges for calculating bottom up <-> top down switching formula
                        int numEdges = m_chunkMemoryService.readInt(vertexId, 4);
                        if (numEdges != -1) {
                            m_edgeCountNextFrontier.addAndGet(numEdges);
                        } else {
                            LOGGER.error("Could not read num neighbors field of vertex 0x%X", vertexId);
                        }
                    }

                    vertexId = p_message.getVertex();
                }

                m_remoteDelegatesForNextFrontier.readLock().unlock();
            }

            m_syncBFSFinished.incrementReceivedVertexMsgCountLocal();
        }

        /**
         * Handle incoming BFSTerminateMessage messages.
         *
         * @param p_message
         *         BFSTerminateMessage to handle
         */
        private void onIncomingBFSTerminateMessage(final BFSTerminateMessage p_message) {

            // System.out.println(">>>> onIncomingBFSTerminateMessage " + NodeID.toHexString(p_message.getOwner()));

            long nextFrontVerts = p_message.getFrontierNextVertices();
            long nextFrontEdges = p_message.getFrontierNextEdges();

            if (nextFrontVerts != 0) {
                m_terminateBfs = false;
            }

            m_nextFrontVertices.addAndGet(nextFrontVerts);
            m_nextFrontEdges.addAndGet(nextFrontEdges);

            m_bfsSlavesEmptyNextFrontiers.incrementAndGet();
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
                interrupt();
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
                    str += "[Running time (ms): " + m_totalTimeMs + ']';
                    str += "[Vertices/sec: " + verticesPerSec + ']';
                    str += "[Edges/sec: " + edgesPerSec + ']';
                    str += "[MaxVerts/sec: " + m_maxVerticesPerSec + ']';
                    str += "[MaxEdges/sec: " + m_maxEdgesPerSec + ']';
                    str += "[AvgVerts/sec: " + m_avgVerticesPerSec + ']';
                    str += "[AvgEdges/sec: " + m_avgEdgesPerSec + ']';
                    str += "[VertexMsgCntLocal (" + m_syncBFSFinished.getTokenValue() + "): " + m_syncBFSFinished.getSentVertexMsgCountLocal() + '/' +
                            m_syncBFSFinished.getReceivedVertexMsgCountLocal() + ']';
                    str += "[VertexMsgCntGlobal (" + m_syncBFSFinished.getTokenValue() + "): " + m_syncBFSFinished.getSentVertexMsgCountGlobal() + '/' +
                            m_syncBFSFinished.getReceivedVertexMsgCountGlobal() + ']';
                    str += "[BFSSlavesLevelFinishedCounter: " + m_syncBFSFinished.getBFSSlavesLevelFinishedCounter() + ']';
                    str += "[BFSSlavesEmptyNextFrontiersCounter: " + m_bfsSlavesEmptyNextFrontiers.get() + ']';

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
        private VertexSimple[] m_vertexBatch;
        private int m_currentDepthLevel;
        private VerticesForNextFrontierMessage[] m_remoteMessages = new VerticesForNextFrontierMessage[NodeID.MAX_ID];

        private volatile boolean m_runIteration;
        private volatile boolean m_exitThread;
        private volatile boolean m_bottomUpIteration;
        private AtomicLong m_edgeCountNextFrontier = new AtomicLong(0);

        private AtomicLong m_sharedVertexCounter;
        private AtomicLong m_sharedEdgeCounter;

        private SyncBFSFinished m_syncBFSFinished;

        /**
         * Constructor
         *
         * @param p_id
         *         Id of the thread.
         * @param p_vertexBatchSize
         *         Number of vertices to process in a single batch.
         * @param p_vertexMessageBatchSize
         *         Number of vertices to back as a batch into a single message to
         *         send to other nodes.
         * @param p_curFrontierShared
         *         Shared instance with other threads of the current frontier.
         * @param p_nextFrontierShared
         *         Shared instance with other threads of the next frontier
         * @param p_visitedFrontierShared
         *         Shared instance with other threads of the visited frontier.
         * @param p_sharedVertexCounter
         *         Shared instance with other threads to count visited vertices
         * @param p_sharedEdgeCounter
         *         Shared instance with other threads to count traversed edges
         */
        BFSThread(final int p_id, final int p_vertexBatchSize, final int p_vertexMessageBatchSize, final ConcurrentBitVectorHybrid p_curFrontierShared,
                final ConcurrentBitVectorHybrid p_nextFrontierShared, final ConcurrentBitVectorHybrid p_visitedFrontierShared,
                final AtomicLong p_sharedVertexCounter, final AtomicLong p_sharedEdgeCounter, final SyncBFSFinished p_syncBFSFinished) {
            super("BFSThread-" + p_id);

            m_id = p_id;
            m_vertexMessageBatchSize = p_vertexMessageBatchSize;
            m_curFrontier = p_curFrontierShared;
            m_nextFrontier = p_nextFrontierShared;
            m_visitedFrontier = p_visitedFrontierShared;

            m_nodeId = m_bootService.getNodeID();
            m_vertexBatch = new VertexSimple[p_vertexBatchSize];
            for (int i = 0; i < m_vertexBatch.length; i++) {
                m_vertexBatch[i] = new VertexSimple(ChunkID.INVALID_ID);
                // performance hack: if writing back, we only write back what we changed
                m_vertexBatch[i].setWriteUserDataOnly(true);
            }

            m_sharedVertexCounter = p_sharedVertexCounter;
            m_sharedEdgeCounter = p_sharedEdgeCounter;

            m_syncBFSFinished = p_syncBFSFinished;
        }

        /**
         * Set the current BFS depth level
         *
         * @param p_curDepth
         *         Current BFS depth.
         */
        void setCurrentBFSDepthLevel(final int p_curDepth) {
            m_currentDepthLevel = p_curDepth;
        }

        /**
         * Trigger running a single iteration until all vertices of the current frontier are processed.
         *
         * @param p_bottomUpIteration
         *         BFS direction for this iteration
         */
        void runIteration(final boolean p_bottomUpIteration) {
            m_bottomUpIteration = p_bottomUpIteration;
            m_edgeCountNextFrontier.set(0);
            m_runIteration = true;
        }

        /**
         * Get the edge count of the vertices added to the next frontier in this bfs iteration.
         *
         * @return Edge count of the the vertices added to the next frontier.
         */
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
                        try {
                            Thread.sleep(2);
                        } catch (final InterruptedException ignored) {
                        }
                    }
                } while (!m_runIteration);

                // --------------------------------------------------

                int validVertsInBatch = 0;

                if (m_bottomUpIteration) {
                    m_visitedFrontier.popFrontLock();
                    for (VertexSimple vertexBatch : m_vertexBatch) {
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
                    for (VertexSimple vertexBatch : m_vertexBatch) {
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
                    short[] slaveNodeIds = m_ctx.getCtxData().getSlaveNodeIds();
                    for (short slaveNodeId : slaveNodeIds) {
                        if (slaveNodeId != m_nodeId) {
                            VerticesForNextFrontierMessage msg = m_remoteMessages[slaveNodeId & 0xFFFF];
                            if (msg != null && msg.getBatchSize() > 0) {
                                try {
                                    m_networkService.sendMessage(msg);
                                } catch (final NetworkException e) {
                                    // #if LOGGER >= ERROR
                                    LOGGER.error("Sending vertex message to node 0x%X failed", msg.getDestination());
                                    // #endif /* LOGGER >= ERROR */
                                    if (m_abortBFSOnError) {
                                        m_ctx.getSignalInterface().sendSignalToMaster(Signal.SIGNAL_ABORT);
                                        return;
                                    }
                                }

                                m_syncBFSFinished.incrementSentVertexMsgCountLocal();

                                // re-use messages
                                m_remoteMessages[slaveNodeId & 0xFFFF].reset();
                            }
                        }
                    }

                    // we are done, go to start
                    continue;
                }

                // --------------------------------------------------

                int gett = m_chunkService.getLocal(m_vertexBatch, 0, validVertsInBatch);
                if (gett != validVertsInBatch) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Error on getting vertices in BFS Thread %d: %d != %d", m_id, gett, validVertsInBatch);
                    // #endif /* LOGGER >= ERROR */
                    if (m_abortBFSOnError) {
                        m_ctx.getSignalInterface().sendSignalToMaster(Signal.SIGNAL_ABORT);
                        return;
                    }
                }

                // --------------------------------------------------

                for (int i = 0; i < validVertsInBatch; i++) {
                    // check first if visited
                    VertexSimple vertex = m_vertexBatch[i];

                    // skip vertices that were already marked invalid before
                    if (vertex.getID() == ChunkID.INVALID_ID) {
                        continue;
                    }

                    m_sharedVertexCounter.incrementAndGet();

                    long[] neighbours = vertex.getNeighbours();
                    long vertexLocalId = ChunkID.getLocalID(vertex.getID());
                    for (long neighbour : neighbours) {
                        // check if neighbors are valid, otherwise something's not ok with the data
                        if (neighbour == ChunkID.INVALID_ID) {
                            // #if LOGGER >= WARN
                            LOGGER.warn("Invalid neighbor found on vertex %s", vertex);
                            // #endif /* LOGGER >= WARN */
                            continue;
                        }

                        // don't allow access to the index chunk
                        if (ChunkID.getLocalID(neighbour) == 0) {
                            // #if LOGGER >= WARN
                            LOGGER.warn("Neighbor id refers to index chunk 0x%X, vertex %s", neighbour, vertex);
                            // #endif /* LOGGER >= WARN */
                            continue;
                        }

                        m_sharedEdgeCounter.incrementAndGet();

                        // sort by remote and local vertices
                        short neighborCreatorId = ChunkID.getCreatorID(neighbour);
                        long neighborLocalId = ChunkID.getLocalID(neighbour);

                        if (m_bottomUpIteration) {
                            if (neighborCreatorId != m_nodeId) {
                                // the child is on the current node but its parent is on a different one
                                // => inter node edge

                                // delegate to remote, fill message buffers until they are full -> send
                                VerticesForNextFrontierMessage msg = m_remoteMessages[neighborCreatorId & 0xFFFF];
                                if (msg == null) {
                                    msg = new VerticesForNextFrontierMessage(neighborCreatorId, m_vertexMessageBatchSize);

                                    m_remoteMessages[neighborCreatorId & 0xFFFF] = msg;
                                }

                                // add vertex to message batch
                                if (!msg.addVertex(vertex.getID())) {
                                    // vertex does not fit anymore, full
                                    try {
                                        m_networkService.sendMessage(msg);
                                    } catch (final NetworkException e) {
                                        // #if LOGGER >= ERROR
                                        LOGGER.error("Sending vertex message to node 0x%X failed", neighborCreatorId);
                                        // #endif /* LOGGER >= ERROR */
                                        if (m_abortBFSOnError) {
                                            m_ctx.getSignalInterface().sendSignalToMaster(Signal.SIGNAL_ABORT);
                                            return;
                                        }
                                    }

                                    m_syncBFSFinished.incrementSentVertexMsgCountLocal();

                                    msg.reset();
                                    m_remoteMessages[neighborCreatorId & 0xFFFF] = msg;
                                    msg.addVertex(vertex.getID());
                                    msg.addNeighbor(neighbour);
                                } else {
                                    msg.addNeighbor(neighbour);
                                }
                            } else {
                                // is our child connected to any of the parents
                                if (m_curFrontier.contains(neighborLocalId)) {
                                    // child -> parent relationship, got our next vertex
                                    // mark child (!) visited
                                    if (m_visitedFrontier.pushBack(vertexLocalId)) {
                                        m_nextFrontier.pushBack(vertexLocalId);

                                        // read num of edges for calculating bottom up <-> top down switching formula
                                        m_edgeCountNextFrontier.addAndGet(neighbours.length);

                                        if (m_markVertices && !m_chunkMemoryService.writeInt(vertex.getID(), 0, m_currentDepthLevel)) {
                                            LOGGER.error("Marking vertex 0x%X failed", vertexLocalId);
                                        }

                                        // we don't have to continue with any other neighbors
                                        // for all neighbors (possible parents) of child
                                        break;
                                    }
                                }
                            }
                        } else {
                            if (neighborCreatorId != m_nodeId) {
                                // delegate to remote, fill message buffers until they are full -> send
                                VerticesForNextFrontierMessage msg = m_remoteMessages[neighborCreatorId & 0xFFFF];
                                if (msg == null) {
                                    msg = new VerticesForNextFrontierMessage(neighborCreatorId, m_vertexMessageBatchSize);

                                    m_remoteMessages[neighborCreatorId & 0xFFFF] = msg;
                                }

                                // add vertex to message batch
                                if (!msg.addVertex(neighbour)) {
                                    // neighbor does not fit anymore, full
                                    try {
                                        m_networkService.sendMessage(msg);
                                    } catch (final NetworkException e) {
                                        // #if LOGGER >= ERROR
                                        LOGGER.error("Sending vertex message to node 0x%X failed", neighborCreatorId);
                                        // #endif /* LOGGER >= ERROR */
                                        if (m_abortBFSOnError) {
                                            m_ctx.getSignalInterface().sendSignalToMaster(Signal.SIGNAL_ABORT);
                                            return;
                                        }
                                    }

                                    m_syncBFSFinished.incrementSentVertexMsgCountLocal();

                                    // re-use messages
                                    msg.reset();
                                    m_remoteMessages[msg.getDestination() & 0xFFFF] = msg;
                                    msg.addVertex(neighbour);
                                }
                            } else {
                                // mark visited and add to next if not visited so far
                                if (m_visitedFrontier.pushBack(neighborLocalId)) {
                                    m_nextFrontier.pushBack(neighborLocalId);

                                    if (m_markVertices && !m_chunkMemoryService.writeInt(neighbour, 0, m_currentDepthLevel)) {
                                        LOGGER.error("Marking vertex 0x%X failed", vertexLocalId);
                                    }

                                    // read num of edges for calculating bottom up <-> top down switching formula
                                    int numEdges = m_chunkMemoryService.readInt(neighbour, 4);
                                    if (numEdges != -1) {
                                        m_edgeCountNextFrontier.addAndGet(numEdges);
                                    } else {
                                        LOGGER.error("Could not read num neighbors field of vertex 0x%X", neighbour);
                                    }

                                }

                            }
                        }
                    }
                }
            }
        }
    }
}

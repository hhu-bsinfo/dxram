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

package de.hhu.bsinfo.dxgraph.load;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraph.data.GraphPartitionIndex;
import de.hhu.bsinfo.dxgraph.data.VertexSimple;
import de.hhu.bsinfo.dxgraph.load.oel.OrderedEdgeList;
import de.hhu.bsinfo.dxgraph.load.oel.OrderedEdgeListBinaryFileThreadBuffering;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * TaskScript to load a graph from a partitioned ordered edge list.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class GraphLoadOrderedEdgeListTask implements Task {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoadOrderedEdgeListTask.class.getSimpleName());

    @Expose
    private String m_path = "./";
    @Expose
    private int m_vertexBatchSize = 100;
    @Expose
    private boolean m_filterDupEdges;
    @Expose
    private boolean m_filterSelfLoops;

    private TaskContext m_ctx;
    private ChunkService m_chunkService;

    /**
     * Default constructor
     */
    public GraphLoadOrderedEdgeListTask() {

    }

    /**
     * Constructor
     *
     * @param p_path
     *         Path containing the graph data to load
     * @param p_vertexBatchSize
     *         Size of a vertex batch for the loading process
     * @param p_filterDupEdges
     *         Check for and filter duplicate edges per vertex
     * @param p_filterSelfLoops
     *         Check for and filter self loops per vertex
     */
    public GraphLoadOrderedEdgeListTask(final String p_path, final int p_vertexBatchSize, final boolean p_filterDupEdges, final boolean p_filterSelfLoops) {
        m_path = p_path;
        m_vertexBatchSize = p_vertexBatchSize;
        m_filterDupEdges = p_filterDupEdges;
        m_filterSelfLoops = p_filterSelfLoops;
    }

    /**
     * Set the number of vertices to buffer with one load call.
     *
     * @param p_batchSize
     *         Number of vertices to buffer.
     */
    public void setLoadVertexBatchSize(final int p_batchSize) {
        m_vertexBatchSize = p_batchSize;
    }

    /**
     * Set the path that contains the graph data.
     *
     * @param p_path
     *         Path with graph data files.
     */
    public void setLoadPath(final String p_path) {
        m_path = p_path;

        // trim / at the end
        if (m_path.charAt(m_path.length() - 1) == '/') {
            m_path = m_path.substring(0, m_path.length() - 1);
        }
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        m_ctx = p_ctx;
        m_chunkService = m_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        TemporaryStorageService temporaryStorageService = m_ctx.getDXRAMServiceAccessor().getService(TemporaryStorageService.class);
        NameserviceService nameserviceService = m_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

        // look for the graph partitioned index of the current compute group
        long chunkIdPartitionIndex =
                nameserviceService.getChunkID(GraphLoadPartitionIndexTask.MS_PART_INDEX_IDENT + m_ctx.getCtxData().getComputeGroupId(), 5000);
        if (chunkIdPartitionIndex == ChunkID.INVALID_ID) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not find partition index for current compute group %d", m_ctx.getCtxData().getComputeGroupId());
            // #endif /* LOGGER >= ERROR */
            return -1;
        }

        GraphPartitionIndex graphPartitionIndex = new GraphPartitionIndex();
        graphPartitionIndex.setID(chunkIdPartitionIndex);

        // get the index
        if (!temporaryStorageService.get(graphPartitionIndex)) {
            // #if LOGGER >= ERROR
            LOGGER.error("Getting partition index from temporary memory failed");
            // #endif /* LOGGER >= ERROR */
            return -2;
        }

        OrderedEdgeList graphPartitionOel = setupOrderedEdgeListForCurrentSlave(m_path, graphPartitionIndex);
        if (graphPartitionOel == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Setting up graph partition for current slave failed");
            // #endif /* LOGGER >= ERROR */
            return -3;
        }

        // #if LOGGER >= INFO
        LOGGER.info("Chunkservice status BEFORE load:\n%s", m_chunkService.getStatus());
        // #endif /* LOGGER >= INFO */

        if (!loadGraphPartition(graphPartitionOel, graphPartitionIndex)) {
            // #if LOGGER >= ERROR
            LOGGER.error("Loading graph partition failed");
            // #endif /* LOGGER >= ERROR */
            return -4;
        }

        // #if LOGGER >= INFO
        LOGGER.info("Chunkservice status AFTER load:\n%s", m_chunkService.getStatus());
        // #endif /* LOGGER >= INFO */

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        switch (p_signal) {
            case SIGNAL_ABORT: {
                // ignore signal here
                break;
            }
            default:
                break;
        }
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_path.length());
        p_exporter.writeBytes(m_path.getBytes(StandardCharsets.US_ASCII));
        p_exporter.writeInt(m_vertexBatchSize);
        p_exporter.writeBoolean(m_filterDupEdges);
        p_exporter.writeBoolean(m_filterSelfLoops);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_path = p_importer.readString(m_path);
        m_vertexBatchSize = p_importer.readInt(m_vertexBatchSize);
        m_filterDupEdges = p_importer.readBoolean(m_filterDupEdges);
        m_filterSelfLoops = p_importer.readBoolean(m_filterSelfLoops);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + m_path.length() + Integer.BYTES + Byte.BYTES * 2;
    }

    /**
     * Setup an edge list instance for the current slave node.
     *
     * @param p_path
     *         Path with indexed graph data partitions.
     * @param p_graphPartitionIndex
     *         Loaded partition index of the graph
     * @return OrderedEdgeList instance giving access to the list found for this slave or null on error.
     */
    private OrderedEdgeList setupOrderedEdgeListForCurrentSlave(final String p_path, final GraphPartitionIndex p_graphPartitionIndex) {
        OrderedEdgeList orderedEdgeList = null;

        // check if directory exists
        File tmpFile = new File(p_path);
        if (!tmpFile.exists()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot setup edge lists, path does not exist: %s", p_path);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        if (!tmpFile.isDirectory()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot setup edge lists, path is not a directory: %s", p_path);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // iterate files in dir, filter by pattern
        File[] files = tmpFile.listFiles((p_dir, p_name) -> {
            String[] tokens = p_name.split("\\.");

            // looking for format xxx.oel.<slave id>
            if (tokens.length > 1) {
                if ("boel".equals(tokens[1])) {
                    return true;
                }
            }

            return false;
        });

        // add filtered files
        // #if LOGGER >= DEBUG
        LOGGER.debug("Setting up oel for current slave, iterating files in %s", p_path);
        // #endif /* LOGGER >= DEBUG */

        for (File file : files) {
            long startOffset = p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId()).getFileStartOffset();
            long endOffset;

            // last partition
            if (m_ctx.getCtxData().getSlaveId() + 1 >= p_graphPartitionIndex.getTotalPartitionCount()) {
                endOffset = Long.MAX_VALUE;
            } else {
                endOffset = p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId() + 1).getFileStartOffset();
            }

            // #if LOGGER >= INFO
            LOGGER.info("Partition for slave %dgraph data file: start %d, end %d", m_ctx.getCtxData().getSlaveId(), startOffset, endOffset);
            // #endif /* LOGGER >= INFO */

            // get the first vertex id of the partition to load
            long startVertexId = 0;
            for (int i = 0; i < m_ctx.getCtxData().getSlaveId(); i++) {
                startVertexId += p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId()).getVertexCount();
            }

            orderedEdgeList =
                    new OrderedEdgeListBinaryFileThreadBuffering(file.getAbsolutePath(), m_vertexBatchSize * 1000, startOffset, endOffset, m_filterDupEdges,
                            m_filterSelfLoops, p_graphPartitionIndex.calcTotalVertexCount(), startVertexId);
            break;
        }

        return orderedEdgeList;
    }

    /**
     * Load a graph partition (single threaded).
     *
     * @param p_orderedEdgeList
     *         Graph partition to load.
     * @param p_graphPartitionIndex
     *         Index for all partitions to rebase vertex ids to current node.
     * @return True if loading successful, false on error.
     */

    private boolean loadGraphPartition(final OrderedEdgeList p_orderedEdgeList, final GraphPartitionIndex p_graphPartitionIndex) {
        VertexSimple[] vertexBuffer = new VertexSimple[m_vertexBatchSize];
        int readCount;

        GraphPartitionIndex.Entry currentPartitionIndexEntry = p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId());
        if (currentPartitionIndexEntry == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot load graph, missing partition index entry for partition %d", m_ctx.getCtxData().getSlaveId());
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        float previousProgress = 0.0f;

        long totalVerticesLoaded = 0;
        long totalEdgesLoaded = 0;

        // #if LOGGER >= INFO
        LOGGER.info("Loading started, target vertex/edge count of partition %d: %d/%d", currentPartitionIndexEntry.getPartitionId(),
                currentPartitionIndexEntry.getVertexCount(), currentPartitionIndexEntry.getEdgeCount());
        // #endif /* LOGGER >= INFO */

        while (true) {
            readCount = 0;
            while (readCount < vertexBuffer.length) {
                VertexSimple vertex = p_orderedEdgeList.readVertex();
                if (vertex == null) {
                    break;
                }

                // re-basing of neighbors needed for multiple files
                // offset tells us how much to add
                // also add current node ID
                long[] neighbours = vertex.getNeighbours();
                if (!p_graphPartitionIndex.rebaseGlobalVertexIdToLocalPartitionVertexId(neighbours)) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Rebasing of neighbors of %s failed, out of vertex id range of graph: %s", vertex, Arrays.toString(neighbours));
                    // #endif /* LOGGER >= ERROR */
                }

                // for now: check if we exceed the max number of neighbors that fit into a chunk
                // this needs to be changed later to split the neighbor list and have a linked list
                // we don't get this very often, so there aren't any real performance issues
                if (neighbours.length > 134217660) {
                    // #if LOGGER >= WARNING
                    LOGGER.warn("Neighbor count of vertex %s exceeds total number of neighbors that fit into a " + "single vertex; will be truncated", vertex);
                    // #endif /* LOGGER >= WARNING */

                    vertex.setNeighbourCount(134217660);
                }

                vertexBuffer[readCount] = vertex;
                readCount++;
                totalEdgesLoaded += neighbours.length;
            }

            if (readCount == 0) {
                break;
            }

            // trim array on unused elements
            if (readCount < vertexBuffer.length) {
                vertexBuffer = Arrays.copyOf(vertexBuffer, readCount);
            }

            m_chunkService.create((DataStructure[]) vertexBuffer);

            int count = m_chunkService.put((DataStructure[]) vertexBuffer);
            if (count != readCount) {
                // #if LOGGER >= ERROR
                LOGGER.error("Putting vertex data for chunks failed: %d != %d", count, readCount);
                // #endif /* LOGGER >= ERROR */
                // return false;
            }

            totalVerticesLoaded += readCount;

            float curProgress = (float) totalVerticesLoaded / currentPartitionIndexEntry.getVertexCount();
            if (curProgress - previousProgress > 0.01) {
                previousProgress = curProgress;
                // #if LOGGER >= INFO
                LOGGER.info("Loading progress: %d", (int) (curProgress * 100));
                // #endif /* LOGGER >= INFO */
            }
        }

        // #if LOGGER >= INFO
        LOGGER.info("Loading done, vertex/edge count: %d/%d", totalVerticesLoaded, totalEdgesLoaded);
        // #endif /* LOGGER >= INFO */

        // filtering removes edges, so this would always fail
        if (!m_filterSelfLoops && !m_filterDupEdges) {
            if (currentPartitionIndexEntry.getVertexCount() != totalVerticesLoaded || currentPartitionIndexEntry.getEdgeCount() != totalEdgesLoaded) {
                // #if LOGGER >= ERROR
                LOGGER.error("Loading failed, vertex/edge count (%d/%d) does not match data in graph partition " + "index (%d/%d)", totalVerticesLoaded,
                        totalEdgesLoaded, currentPartitionIndexEntry.getVertexCount(), currentPartitionIndexEntry.getEdgeCount());
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        } else {
            // #if LOGGER >= INFO
            LOGGER.info("Graph was filtered during loadin: duplicate edges %b, self loops %b", m_filterDupEdges, m_filterSelfLoops);
            // #endif /* LOGGER >= INFO */
        }

        return true;
    }
}

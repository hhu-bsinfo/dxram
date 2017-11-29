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
import java.util.ArrayList;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraph.data.GraphPartitionIndex;
import de.hhu.bsinfo.dxgraph.data.GraphRootList;
import de.hhu.bsinfo.dxgraph.load.oel.OrderedEdgeListRoots;
import de.hhu.bsinfo.dxgraph.load.oel.OrderedEdgeListRootsBinaryFile;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * TaskScript to load a list of root vertex ids for BFS entry points.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class GraphLoadBFSRootListTask implements Task {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoadBFSRootListTask.class.getSimpleName());

    public static final String MS_BFS_ROOTS = "BFS";

    @Expose
    private String m_path = "./";

    /**
     * Default constructor
     */
    public GraphLoadBFSRootListTask() {

    }

    /**
     * Constructor
     *
     * @param p_path
     *         Path containing a root list to load
     */
    public GraphLoadBFSRootListTask(final String p_path) {
        m_path = p_path;
    }

    /**
     * Set the path where one or multiple partition index files are stored.
     *
     * @param p_path
     *         Path where the files are located
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

        // we don't have to execute this on all slaves
        // slave 0 will do this for the whole compute group and
        // store the result. every other slave can simply grab the
        // root list from chunk memory
        if (p_ctx.getCtxData().getSlaveId() == 0) {
            TemporaryStorageService temporaryStorageService = p_ctx.getDXRAMServiceAccessor().getService(TemporaryStorageService.class);
            NameserviceService nameserviceService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

            // look for the graph partitioned index of the current compute group
            long chunkIdPartitionIndex =
                    nameserviceService.getChunkID(GraphLoadPartitionIndexTask.MS_PART_INDEX_IDENT + p_ctx.getCtxData().getComputeGroupId(), 5000);
            if (chunkIdPartitionIndex == ChunkID.INVALID_ID) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not find partition index for current compute group %d", p_ctx.getCtxData().getComputeGroupId());
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

            OrderedEdgeListRoots orderedEdgeListRoots = setupOrderedEdgeListRoots(m_path);
            if (orderedEdgeListRoots == null) {
                // #if LOGGER >= ERROR
                LOGGER.error("Setting up ordered edge list roots failed");
                // #endif /* LOGGER >= ERROR */
                return -3;
            }

            GraphRootList rootList = loadRootList(orderedEdgeListRoots, graphPartitionIndex);
            if (rootList == null) {
                // #if LOGGER >= ERROR
                LOGGER.error("Loading root list failed");
                // #endif /* LOGGER >= ERROR */
                return -4;
            }

            rootList.setID(temporaryStorageService.generateStorageId(MS_BFS_ROOTS + p_ctx.getCtxData().getComputeGroupId()));

            // store the root list for our current compute group
            if (!temporaryStorageService.create(rootList)) {
                // #if LOGGER >= ERROR
                LOGGER.error("Creating chunk for root list failed");
                // #endif /* LOGGER >= ERROR */
                return -5;
            }

            if (!temporaryStorageService.put(rootList)) {
                // #if LOGGER >= ERROR
                LOGGER.error("Putting root list failed");
                // #endif /* LOGGER >= ERROR */
                return -6;
            }

            // register chunk at nameservice that other slaves can find it
            nameserviceService.register(rootList, MS_BFS_ROOTS + p_ctx.getCtxData().getComputeGroupId());

            // #if LOGGER >= INFO
            LOGGER.info("Successfully loaded and stored root list, nameservice entry name %s:\n%s", MS_BFS_ROOTS + p_ctx.getCtxData().getComputeGroupId(),
                    rootList);
            // #endif /* LOGGER >= INFO */
        }

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        switch (p_signal) {
            case SIGNAL_ABORT: {
                // ignore signal here
                break;
            }
        }
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(m_path);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_path = p_importer.readString(m_path);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + m_path.length();
    }

    /**
     * Setup a root node list instance.
     *
     * @param p_path
     *         Path with the root list.
     * @return OrderedEdgeListRoots instance giving access to the list found for this slave or null on error.
     */
    private OrderedEdgeListRoots setupOrderedEdgeListRoots(final String p_path) {
        OrderedEdgeListRoots orderedEdgeListRoots = null;

        // check if directory exists
        File tmpFile = new File(p_path);
        if (!tmpFile.exists()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot setup order ededge list roots, path does not exist: %s", p_path);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        if (!tmpFile.isDirectory()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Cannot setup ordered edge list roots, path is not a directory: %s", p_path);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // iterate files in dir, filter by pattern
        File[] files = tmpFile.listFiles((p_dir, p_name) -> {
            String[] tokens = p_name.split("\\.");

            // looking for format xxx.broots
            if (tokens.length > 1) {
                System.out.println(tokens[1]);
                if ("broots".equals(tokens[1])) {
                    return true;
                }
            }

            return false;
        });

        // add filtered files
        // #if LOGGER >= DEBUG
        LOGGER.debug("Setting up root oel, iterating files in %s", p_path);
        // #endif /* LOGGER >= DEBUG */

        if (files != null) {
            for (File file : files) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Found root list: %s", file);
                // #endif /* LOGGER >= DEBUG */

                orderedEdgeListRoots = new OrderedEdgeListRootsBinaryFile(file.getAbsolutePath());
                break;
            }
        }

        return orderedEdgeListRoots;
    }

    /**
     * Load the root list.
     *
     * @param p_orderedEdgeRootList
     *         Root list to load.
     * @param p_graphPartitionIndex
     *         Index of all partitions to rebase vertex ids of all roots.
     * @return Root list instance on success, null on error.
     */
    private GraphRootList loadRootList(final OrderedEdgeListRoots p_orderedEdgeRootList, final GraphPartitionIndex p_graphPartitionIndex) {

        ArrayList<Long> roots = new ArrayList<>();
        while (true) {
            long root = p_orderedEdgeRootList.getRoot();
            if (root == ChunkID.INVALID_ID) {
                break;
            }

            long vertexId = p_graphPartitionIndex.rebaseGlobalVertexIdToLocalPartitionVertexId(root);
            if (vertexId == ChunkID.INVALID_ID) {
                LOGGER.error("Rebasing of 0x%X failed out of vertex id range of graph", root);
            }
            roots.add(vertexId);
        }

        GraphRootList rootList = new GraphRootList(ChunkID.INVALID_ID, roots.size());
        for (int i = 0; i < roots.size(); i++) {
            rootList.getRoots()[i] = roots.get(i);
        }

        return rootList;
    }
}


package de.hhu.bsinfo.dxgraph.load;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxcompute.ms.TaskPayload;
import de.hhu.bsinfo.dxgraph.GraphTaskPayloads;
import de.hhu.bsinfo.dxgraph.data.GraphPartitionIndex;
import de.hhu.bsinfo.dxgraph.data.GraphRootList;
import de.hhu.bsinfo.dxgraph.load.oel.OrderedEdgeListRoots;
import de.hhu.bsinfo.dxgraph.load.oel.OrderedEdgeListRootsBinaryFile;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Task to load a list of root vertex ids for BFS entry points.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class GraphLoadBFSRootListTaskPayload extends TaskPayload {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoadBFSRootListTaskPayload.class.getSimpleName());

    public static final String MS_BFS_ROOTS = "BFS";

    @Expose
    private String m_path = "./";

    /**
     * Constructor
     * @param p_numReqSlaves
     *            Number of slaves required to run this task
     * @param p_path
     *            Path containing a root list to load
     */
    public GraphLoadBFSRootListTaskPayload(final short p_numReqSlaves, final String p_path) {
        super(GraphTaskPayloads.TYPE, GraphTaskPayloads.SUBTYPE_GRAPH_LOAD_BFS_ROOTS, p_numReqSlaves);
        m_path = p_path;
    }

    /**
     * Set the path where one or multiple partition index files are stored.
     * @param p_path
     *            Path where the files are located
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
            TemporaryStorageService m_temporaryStorageService = p_ctx.getDXRAMServiceAccessor().getService(TemporaryStorageService.class);
            NameserviceService nameserviceService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

            // look for the graph partitioned index of the current compute group
            long chunkIdPartitionIndex =
                    nameserviceService.getChunkID(GraphLoadPartitionIndexTaskPayload.MS_PART_INDEX_IDENT + p_ctx.getCtxData().getComputeGroupId(), 5000);
            if (chunkIdPartitionIndex == ChunkID.INVALID_ID) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not find partition index for current compute group %d", p_ctx.getCtxData().getComputeGroupId());
                // #endif /* LOGGER >= ERROR */
                return -1;
            }

            GraphPartitionIndex graphPartitionIndex = new GraphPartitionIndex();
            graphPartitionIndex.setID(chunkIdPartitionIndex);

            // get the index
            if (!m_temporaryStorageService.get(graphPartitionIndex)) {
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

            rootList.setID(m_temporaryStorageService.generateStorageId(MS_BFS_ROOTS + p_ctx.getCtxData().getComputeGroupId()));

            // store the root list for our current compute group
            if (!m_temporaryStorageService.create(rootList)) {
                // #if LOGGER >= ERROR
                LOGGER.error("Creating chunk for root list failed");
                // #endif /* LOGGER >= ERROR */
                return -5;
            }

            if (!m_temporaryStorageService.put(rootList)) {
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
        super.exportObject(p_exporter);

        p_exporter.writeInt(m_path.length());
        p_exporter.writeBytes(m_path.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);

        int strLength = p_importer.readInt();
        byte[] tmp = new byte[strLength];
        p_importer.readBytes(tmp);
        m_path = new String(tmp, StandardCharsets.US_ASCII);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + Integer.BYTES + m_path.length();
    }

    /**
     * Setup a root node list instance.
     * @param p_path
     *            Path with the root list.
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

            // looking for format xxx.roel
                if (tokens.length > 1) {
                    if (tokens[1].equals("broots")) {
                        return true;
                    }
                }

                return false;
            });

        // add filtered files
        // #if LOGGER >= DEBUG
        LOGGER.debug("Setting up root oel, iterating files in %s", p_path);
        // #endif /* LOGGER >= DEBUG */

        for (File file : files) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("Found root list: %s", file);
            // #endif /* LOGGER >= DEBUG */

            orderedEdgeListRoots = new OrderedEdgeListRootsBinaryFile(file.getAbsolutePath());
            break;
        }

        return orderedEdgeListRoots;
    }

    /**
     * Load the root list.
     * @param p_orderedEdgeRootList
     *            Root list to load.
     * @param p_graphPartitionIndex
     *            Index of all partitions to rebase vertex ids of all roots.
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

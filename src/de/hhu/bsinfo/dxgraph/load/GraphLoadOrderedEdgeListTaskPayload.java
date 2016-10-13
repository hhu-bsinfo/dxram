
package de.hhu.bsinfo.dxgraph.load;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxgraph.GraphTaskPayloads;
import de.hhu.bsinfo.dxgraph.data.GraphPartitionIndex;
import de.hhu.bsinfo.dxgraph.data.VertexSimple;
import de.hhu.bsinfo.dxgraph.load.oel.OrderedEdgeList;
import de.hhu.bsinfo.dxgraph.load.oel.OrderedEdgeListBinaryFileThreadBuffering;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Task to load a graph from a partitioned ordered edge list.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class GraphLoadOrderedEdgeListTaskPayload extends AbstractTaskPayload {

	private static final Argument MS_ARG_PATH =
			new Argument("graphPath", null, false, "Path containing the graph data to load.");
	private static final Argument MS_ARG_VERTEX_BATCH_SIZE =
			new Argument("vertexBatchSize", null, false, "Size of a vertex batch for the loading process.");
	private static final Argument MS_ARG_FILTER_DUP_EDGES =
			new Argument("filterDupEdges", null, false, "Check for and filter duplicate edges per vertex.");
	private static final Argument MS_ARG_FILTER_SELF_LOOPS =
			new Argument("filterSelfLoops", null, false, "Check for and filter self loops per vertex.");

	private TaskContext m_ctx;
	private LoggerService m_loggerService;
	private ChunkService m_chunkService;

	private String m_path = "./";
	private int m_vertexBatchSize = 100;
	private boolean m_filterDupEdges;
	private boolean m_filterSelfLoops;

	/**
	 * Constructor
	 */
	public GraphLoadOrderedEdgeListTaskPayload() {
		super(GraphTaskPayloads.TYPE, GraphTaskPayloads.SUBTYPE_GRAPH_LOAD_OEL);
	}

	/**
	 * Set the number of vertices to buffer with one load call.
	 *
	 * @param p_batchSize Number of vertices to buffer.
	 */
	public void setLoadVertexBatchSize(final int p_batchSize) {
		m_vertexBatchSize = p_batchSize;
	}

	/**
	 * Set the path that contains the graph data.
	 *
	 * @param p_path Path with graph data files.
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
		m_loggerService = m_ctx.getDXRAMServiceAccessor().getService(LoggerService.class);
		m_chunkService = m_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
		TemporaryStorageService temporaryStorageService =
				m_ctx.getDXRAMServiceAccessor().getService(TemporaryStorageService.class);
		NameserviceService nameserviceService = m_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);

		// look for the graph partitioned index of the current compute group
		long chunkIdPartitionIndex = nameserviceService
				.getChunkID(GraphLoadPartitionIndexTaskPayload.MS_PART_INDEX_IDENT
						+ m_ctx.getCtxData().getComputeGroupId(), 5000);
		if (chunkIdPartitionIndex == ChunkID.INVALID_ID) {
			// #if LOGGER >= ERROR
			m_loggerService.error(getClass(),
					"Could not find partition index for current compute group "
							+ m_ctx.getCtxData().getComputeGroupId());
			// #endif /* LOGGER >= ERROR */
			return -1;
		}

		GraphPartitionIndex graphPartitionIndex = new GraphPartitionIndex();
		graphPartitionIndex.setID(chunkIdPartitionIndex);

		// get the index
		if (!temporaryStorageService.get(graphPartitionIndex)) {
			// #if LOGGER >= ERROR
			m_loggerService.error(getClass(), "Getting partition index from temporary memory failed.");
			// #endif /* LOGGER >= ERROR */
			return -2;
		}

		OrderedEdgeList graphPartitionOel = setupOrderedEdgeListForCurrentSlave(m_path, graphPartitionIndex);
		if (graphPartitionOel == null) {
			// #if LOGGER >= ERROR
			m_loggerService.error(getClass(), "Setting up graph partition for current slave failed.");
			// #endif /* LOGGER >= ERROR */
			return -3;
		}

		// #if LOGGER >= INFO
		m_loggerService.info(getClass(),
				"Chunkservice status BEFORE load:\n" + m_chunkService.getStatus());
		// #endif /* LOGGER >= INFO */

		if (!loadGraphPartition(graphPartitionOel, graphPartitionIndex)) {
			// #if LOGGER >= ERROR
			m_loggerService.error(getClass(), "Loading graph partition failed.");
			// #endif /* LOGGER >= ERROR */
			return -4;
		}

		// #if LOGGER >= INFO
		m_loggerService.info(getClass(),
				"Chunkservice status AFTER load:\n" + m_chunkService.getStatus());
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
	public void terminalCommandRegisterArguments(final ArgumentList p_argumentList) {
		super.terminalCommandRegisterArguments(p_argumentList);

		p_argumentList.setArgument(MS_ARG_PATH);
		p_argumentList.setArgument(MS_ARG_VERTEX_BATCH_SIZE);
		p_argumentList.setArgument(MS_ARG_FILTER_DUP_EDGES);
		p_argumentList.setArgument(MS_ARG_FILTER_SELF_LOOPS);
	}

	@Override
	public void terminalCommandCallbackForArguments(final ArgumentList p_argumentList) {
		super.terminalCommandCallbackForArguments(p_argumentList);

		m_path = p_argumentList.getArgumentValue(MS_ARG_PATH, String.class);
		m_vertexBatchSize = p_argumentList.getArgumentValue(MS_ARG_VERTEX_BATCH_SIZE, Integer.class);
		m_filterDupEdges = p_argumentList.getArgumentValue(MS_ARG_FILTER_DUP_EDGES, Boolean.class);
		m_filterSelfLoops = p_argumentList.getArgumentValue(MS_ARG_FILTER_SELF_LOOPS, Boolean.class);
	}

	@Override
	public void exportObject(final Exporter p_exporter) {
		super.exportObject(p_exporter);

		p_exporter.writeInt(m_path.length());
		p_exporter.writeBytes(m_path.getBytes(StandardCharsets.US_ASCII));
		p_exporter.writeInt(m_vertexBatchSize);
		p_exporter.writeByte((byte) (m_filterDupEdges ? 1 : 0));
		p_exporter.writeByte((byte) (m_filterSelfLoops ? 1 : 0));
	}

	@Override
	public void importObject(final Importer p_importer) {
		super.importObject(p_importer);

		int strLength = p_importer.readInt();
		byte[] tmp = new byte[strLength];
		p_importer.readBytes(tmp);
		m_path = new String(tmp, StandardCharsets.US_ASCII);
		m_vertexBatchSize = p_importer.readInt();
		m_filterDupEdges = p_importer.readByte() > 0;
		m_filterSelfLoops = p_importer.readByte() > 0;
	}

	@Override
	public int sizeofObject() {
		return super.sizeofObject() + Integer.BYTES + m_path.length() + Integer.BYTES + Byte.BYTES * 2;
	}

	/**
	 * Setup an edge list instance for the current slave node.
	 *
	 * @param p_path                Path with indexed graph data partitions.
	 * @param p_graphPartitionIndex Loaded partition index of the graph
	 * @return OrderedEdgeList instance giving access to the list found for this slave or null on error.
	 */
	private OrderedEdgeList setupOrderedEdgeListForCurrentSlave(final String p_path,
			final GraphPartitionIndex p_graphPartitionIndex) {
		OrderedEdgeList orderedEdgeList = null;

		// check if directory exists
		File tmpFile = new File(p_path);
		if (!tmpFile.exists()) {
			// #if LOGGER >= ERROR
			m_loggerService.error(getClass(), "Cannot setup edge lists, path does not exist: " + p_path);
			// #endif /* LOGGER >= ERROR */
			return null;
		}

		if (!tmpFile.isDirectory()) {
			// #if LOGGER >= ERROR
			m_loggerService.error(getClass(), "Cannot setup edge lists, path is not a directory: " + p_path);
			// #endif /* LOGGER >= ERROR */
			return null;
		}

		// iterate files in dir, filter by pattern
		File[] files = tmpFile.listFiles((p_dir, p_name) -> {
			String[] tokens = p_name.split("\\.");

			// looking for format xxx.oel.<slave id>
			if (tokens.length > 1) {
				if (tokens[1].equals("boel")) {
					return true;
				}
			}

			return false;
		});

		// add filtered files
		// #if LOGGER >= DEBUG
		m_loggerService.debug(getClass(), "Setting up oel for current slave, iterating files in " + p_path);
		// #endif /* LOGGER >= DEBUG */

		for (File file : files) {
			long startOffset =
					p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId()).getFileStartOffset();
			long endOffset;

			// last partition
			if (m_ctx.getCtxData().getSlaveId() + 1 >= p_graphPartitionIndex.getTotalPartitionCount()) {
				endOffset = Long.MAX_VALUE;
			} else {
				endOffset = p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId() + 1)
						.getFileStartOffset();
			}

			// #if LOGGER >= INFO
			m_loggerService.info(getClass(),
					"Partition for slave " + m_ctx.getCtxData().getSlaveId() + "graph data file: start " + startOffset
							+ ", end "
							+ endOffset);
			// #endif /* LOGGER >= INFO */

			// get the first vertex id of the partition to load
			long startVertexId = 0;
			for (int i = 0; i < m_ctx.getCtxData().getSlaveId(); i++) {
				startVertexId +=
						p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId()).getVertexCount();
			}

			orderedEdgeList = new OrderedEdgeListBinaryFileThreadBuffering(file.getAbsolutePath(),
					m_vertexBatchSize * 1000,
					startOffset,
					endOffset,
					m_filterDupEdges,
					m_filterSelfLoops,
					p_graphPartitionIndex.calcTotalVertexCount(),
					startVertexId);
			break;
		}

		return orderedEdgeList;
	}

	/**
	 * Load a graph partition (single threaded).
	 *
	 * @param p_orderedEdgeList     Graph partition to load.
	 * @param p_graphPartitionIndex Index for all partitions to rebase vertex ids to current node.
	 * @return True if loading successful, false on error.
	 */

	private boolean loadGraphPartition(final OrderedEdgeList p_orderedEdgeList,
			final GraphPartitionIndex p_graphPartitionIndex) {
		VertexSimple[] vertexBuffer = new VertexSimple[m_vertexBatchSize];
		int readCount;

		GraphPartitionIndex.Entry currentPartitionIndexEntry =
				p_graphPartitionIndex.getPartitionIndex(m_ctx.getCtxData().getSlaveId());
		if (currentPartitionIndexEntry == null) {
			// #if LOGGER >= ERROR
			m_loggerService.error(getClass(),
					"Cannot load graph, missing partition index entry for partition " + m_ctx.getCtxData()
							.getSlaveId());
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		float previousProgress = 0.0f;

		long totalVerticesLoaded = 0;
		long totalEdgesLoaded = 0;

		// #if LOGGER >= INFO
		m_loggerService.info(getClass(), "Loading started, target vertex/edge count of partition "
				+ currentPartitionIndexEntry.getPartitionId() + ": " + currentPartitionIndexEntry.getVertexCount() + "/"
				+ currentPartitionIndexEntry.getEdgeCount());
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
					m_loggerService.error(getClass(),
							"Rebasing of neighbors of " + vertex + " failed, out of vertex id range of graph: " + Arrays
									.toString(neighbours));
					// #endif /* LOGGER >= ERROR */
				}

				// for now: check if we exceed the max number of neighbors that fit into a chunk
				// this needs to be changed later to split the neighbor list and have a linked list
				// we don't get this very often, so there aren't any real performance issues
				if (neighbours.length > 134217660) {
					// #if LOGGER >= WARNING
					m_loggerService.warn(getClass(), "Neighbor count of vertex " + vertex + " exceeds total number"
							+ "of neighbors that fit into a single vertex; will be truncated");
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

			// fill in null paddings for unused elements
			for (int i = readCount; i < vertexBuffer.length; i++) {
				vertexBuffer[i] = null;
			}

			int count = m_chunkService.create((DataStructure[]) vertexBuffer);
			if (count != readCount) {
				// #if LOGGER >= ERROR
				m_loggerService.error(getClass(), "Creating chunks for vertices failed: " + count + " != " + readCount);
				// #endif /* LOGGER >= ERROR */
				//return false;
			}

			count = m_chunkService.put((DataStructure[]) vertexBuffer);
			if (count != readCount) {
				// #if LOGGER >= ERROR
				m_loggerService.error(getClass(),
						"Putting vertex data for chunks failed: " + count + " != " + readCount);
				// #endif /* LOGGER >= ERROR */
				//return false;
			}

			totalVerticesLoaded += readCount;

			float curProgress = ((float) totalVerticesLoaded) / currentPartitionIndexEntry.getVertexCount();
			if (curProgress - previousProgress > 0.01) {
				previousProgress = curProgress;
				// #if LOGGER >= INFO
				m_loggerService.info(getClass(), "Loading progress: " + (int) (curProgress * 100) + "%");
				// #endif /* LOGGER >= INFO */
			}
		}

		// #if LOGGER >= INFO
		m_loggerService.info(getClass(),
				"Loading done, vertex/edge count: " + totalVerticesLoaded + "/" + totalEdgesLoaded);
		// #endif /* LOGGER >= INFO */

		// filtering removes edges, so this would always fail
		if (!m_filterSelfLoops && !m_filterDupEdges) {
			if (currentPartitionIndexEntry.getVertexCount() != totalVerticesLoaded
					|| currentPartitionIndexEntry.getEdgeCount() != totalEdgesLoaded) {
				// #if LOGGER >= ERROR
				m_loggerService.error(getClass(),
						"Loading failed, vertex/edge count (" + totalVerticesLoaded + "/" + totalEdgesLoaded
								+ ") does not match data in graph partition index (" + currentPartitionIndexEntry
								.getVertexCount() + "/" + currentPartitionIndexEntry.getEdgeCount() + ")");
				// #endif /* LOGGER >= ERROR */
				return false;
			}
		} else {
			// #if LOGGER >= INFO
			m_loggerService.info(getClass(),
					"Graph was filtered during loadin: duplicate edges " + m_filterDupEdges + ", self loops "
							+ m_filterSelfLoops);
			// #endif /* LOGGER >= INFO */
		}

		return true;
	}
}


package de.hhu.bsinfo.dxgraph.load;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxgraph.GraphTaskPayloads;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxgraph.load.oel.OrderedEdgeList;
import de.hhu.bsinfo.dxgraph.load.oel.OrderedEdgeListBinaryFileThreadBuffering;
import de.hhu.bsinfo.dxgraph.load.oel.OrderedEdgeListTextFileThreadBuffering;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.term.TerminalDelegate;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Task to load a graph from a partitioned ordered edge list.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class GraphLoadOrderedEdgeListTaskPayload extends AbstractTaskPayload {

	private LoggerService m_loggerService;
	private ChunkService m_chunkService;

	private String m_path = new String("./");
	private int m_vertexBatchSize = 100;

	/**
	 * Constructor
	 */
	public GraphLoadOrderedEdgeListTaskPayload() {
		super(GraphTaskPayloads.TYPE, GraphTaskPayloads.SUBTYPE_GRAPH_LOAD_OEL);
	}

	/**
	 * Set the number of vertices to buffer with one load call.
	 * @param p_batchSize
	 *            Number of vertices to buffer.
	 */
	public void setLoadVertexBatchSize(final int p_batchSize) {
		m_vertexBatchSize = p_batchSize;
	}

	/**
	 * Set the path that contains the graph data.
	 * @param p_path
	 *            Path with graph data files.
	 */
	public void setLoadPath(final String p_path) {
		m_path = p_path;

		// trim / at the end
		if (m_path.charAt(m_path.length() - 1) == '/') {
			m_path = m_path.substring(0, m_path.length() - 1);
		}
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		m_loggerService = p_dxram.getService(LoggerService.class);
		m_chunkService = p_dxram.getService(ChunkService.class);
		NameserviceService nameserviceService = p_dxram.getService(NameserviceService.class);

		// look for the graph partitioned index of the current compute group
		long chunkIdPartitionIndex = nameserviceService
				.getChunkID(GraphLoadPartitionIndexTaskPayload.MS_PART_INDEX_IDENT + getComputeGroupId(), 5000);
		if (chunkIdPartitionIndex == ChunkID.INVALID_ID) {
			m_loggerService.error(getClass(),
					"Could not find partition index for current compute group " + getComputeGroupId());
			return -1;
		}

		GraphPartitionIndex graphPartitionIndex = new GraphPartitionIndex();
		graphPartitionIndex.setID(chunkIdPartitionIndex);

		// get the index
		if (m_chunkService.get(graphPartitionIndex) != 1) {
			m_loggerService.error(getClass(), "Getting partition index from chunk memory failed.");
			return -2;
		}

		OrderedEdgeList graphPartitionOel = setupOrderedEdgeListForCurrentSlave(m_path);
		if (graphPartitionOel == null) {
			m_loggerService.error(getClass(), "Setting up graph partition for current slave failed.");
			return -3;
		}

		if (!loadGraphPartition(graphPartitionOel, graphPartitionIndex)) {
			m_loggerService.error(getClass(), "Loading graph partition failed.");
			return -4;
		}

		return 0;
	}

	@Override
	public boolean terminalCommandCallbackForParameters(final TerminalDelegate p_delegate) {
		m_path = p_delegate.promptForUserInput("graphPath");
		m_vertexBatchSize = Integer.parseInt(p_delegate.promptForUserInput("vertexBatchSize"));
		return true;
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		int size = super.exportObject(p_exporter, p_size);

		p_exporter.writeInt(m_path.length());
		p_exporter.writeBytes(m_path.getBytes(StandardCharsets.US_ASCII));
		p_exporter.writeInt(m_vertexBatchSize);

		return size + Integer.BYTES + m_path.length() + Integer.BYTES;
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {
		int size = super.importObject(p_importer, p_size);

		int strLength = p_importer.readInt();
		byte[] tmp = new byte[strLength];
		p_importer.readBytes(tmp);
		m_path = new String(tmp, StandardCharsets.US_ASCII);
		m_vertexBatchSize = p_importer.readInt();

		return size + Integer.BYTES + m_path.length() + Integer.BYTES;
	}

	@Override
	public int sizeofObject() {
		return super.sizeofObject() + Integer.BYTES + m_path.length() + Integer.BYTES;
	}

	/**
	 * Setup an edge list instance for the current slave node.
	 * @param p_path
	 *            Path with indexed graph data partitions.
	 * @return OrderedEdgeList instance giving access to the list found for this slave or null on error.
	 */
	private OrderedEdgeList setupOrderedEdgeListForCurrentSlave(final String p_path) {
		OrderedEdgeList orderedEdgeList = null;

		// check if directory exists
		File tmpFile = new File(p_path);
		if (!tmpFile.exists()) {
			m_loggerService.error(getClass(), "Cannot setup edge lists, path does not exist: " + p_path);
			return null;
		}

		if (!tmpFile.isDirectory()) {
			m_loggerService.error(getClass(), "Cannot setup edge lists, path is not a directory: " + p_path);
			return null;
		}

		// iterate files in dir, filter by pattern
		File[] files = tmpFile.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File p_dir, final String p_name) {
				String[] tokens = p_name.split("\\.");

				// looking for format xxx.oel.<slave id>
				if (tokens.length > 1) {
					if (tokens[1].equals("oel") || tokens[1].equals("boel")) {
						return true;
					}
				}

				return false;
			}
		});

		// add filtered files
		m_loggerService.debug(getClass(), "Setting up oel for current slave, iterating files in " + p_path);
		for (File file : files) {
			String[] tokens = file.getName().split("\\.");

			// looking for format xxx.oel.<slave id>
			if (tokens.length > 2) {
				if (Integer.parseInt(tokens[2]) == getSlaveId()) {
					if (tokens[1].equals("oel")) {
						m_loggerService.debug(getClass(), "Found partition for slave: " + file);
						orderedEdgeList = new OrderedEdgeListTextFileThreadBuffering(file.getAbsolutePath(),
								m_vertexBatchSize * 1000);
						break;
					} else if (tokens[1].equals("boel")) {
						m_loggerService.debug(getClass(), "Found partition for slave: " + file);
						orderedEdgeList = new OrderedEdgeListBinaryFileThreadBuffering(file.getAbsolutePath(),
								m_vertexBatchSize * 1000);
						break;
					}
				}
			}
		}

		return orderedEdgeList;
	}

	/**
	 * Load a graph partition (single threaded).
	 * @param p_orderedEdgeList
	 *            Graph partition to load.
	 * @param p_graphPartitionIndex
	 *            Index for all partitions to rebase vertex ids to current node.
	 * @return True if loading successful, false on error.
	 */
	private boolean loadGraphPartition(final OrderedEdgeList p_orderedEdgeList,
			final GraphPartitionIndex p_graphPartitionIndex) {
		Vertex[] vertexBuffer = new Vertex[m_vertexBatchSize];
		int readCount = 0;

		GraphPartitionIndex.Entry currentPartitionIndexEntry = p_graphPartitionIndex.getPartitionIndex(getSlaveId());
		if (currentPartitionIndexEntry == null) {
			m_loggerService.error(getClass(),
					"Cannot load graph, missing partition index entry for partition " + getSlaveId());
			return false;
		}

		float previousProgress = 0.0f;

		long totalVerticesLoaded = 0;
		long totalEdgesLoaded = 0;

		m_loggerService.info(getClass(), "Loading started, target vertex/edge count of partition "
				+ currentPartitionIndexEntry.getPartitionId() + ": " + currentPartitionIndexEntry.getVertexCount() + "/"
				+ currentPartitionIndexEntry.getEdgeCount());

		while (true) {
			readCount = 0;
			while (readCount < vertexBuffer.length) {
				Vertex vertex = p_orderedEdgeList.readVertex();
				if (vertex == null) {
					break;
				}

				// re-basing of neighbors needed for multiple files
				// offset tells us how much to add
				// also add current node ID
				long[] neighbours = vertex.getNeighbours();
				p_graphPartitionIndex.rebaseGlobalVertexIdToLocalPartitionVertexId(neighbours);

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

			int count = m_chunkService.create(vertexBuffer);
			if (count != readCount) {
				m_loggerService.error(getClass(), "Creating chunks for vertices failed: " + count + " != " + readCount);
				return false;
			}

			count = m_chunkService.put(vertexBuffer);
			if (count != readCount) {
				m_loggerService.error(getClass(),
						"Putting vertex data for chunks failed: " + count + " != " + readCount);
				return false;
			}

			totalVerticesLoaded += readCount;

			float curProgress = ((float) totalVerticesLoaded) / currentPartitionIndexEntry.getVertexCount();
			if (curProgress - previousProgress > 0.01) {
				previousProgress = curProgress;
				m_loggerService.info(getClass(), "Loading progress: " + (int) (curProgress * 100) + "%");
			}
		}

		m_loggerService.info(getClass(),
				"Loading done, vertex/edge count: " + totalVerticesLoaded + "/" + totalEdgesLoaded);

		if (currentPartitionIndexEntry.getVertexCount() != totalVerticesLoaded
				|| currentPartitionIndexEntry.getEdgeCount() != totalEdgesLoaded) {
			m_loggerService.error(getClass(),
					"Loading failed, vertex/edge count does not match data in graph partition index");
			return false;
		}

		return true;
	}
}

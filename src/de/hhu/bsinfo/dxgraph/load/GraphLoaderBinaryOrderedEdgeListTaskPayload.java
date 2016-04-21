
package de.hhu.bsinfo.dxgraph.load;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import de.hhu.bsinfo.dxgraph.GraphTaskPayloads;
import de.hhu.bsinfo.dxgraph.data.Vertex2;
import de.hhu.bsinfo.dxgraph.load2.RebaseVertexID;
import de.hhu.bsinfo.dxgraph.load2.RebaseVertexIDLocal;
import de.hhu.bsinfo.dxgraph.load2.oel.OrderedEdgeList;
import de.hhu.bsinfo.dxgraph.load2.oel.OrderedEdgeListBinaryFileThreadBuffering;
import de.hhu.bsinfo.dxgraph.load2.oel.OrderedEdgeListRoots;
import de.hhu.bsinfo.dxgraph.load2.oel.OrderedEdgeListRootsTextFile;
import de.hhu.bsinfo.dxgraph.load2.oel.OrderedEdgeListTextFileThreadBuffering;
import de.hhu.bsinfo.utils.Pair;

public class GraphLoaderBinaryOrderedEdgeListTaskPayload extends AbstractGraphLoaderTaskPayload {

	private int m_vertexBatchSize = 100;

	private long m_totalVerticesLoaded = 0;
	private long m_totalEdgesLoaded = 0;
	private long[] m_roots = null;

	/**
	 * Constructor
	 */
	public GraphLoaderBinaryOrderedEdgeListTaskPayload() {
		super(GraphTaskPayloads.TYPE, GraphTaskPayloads.SUBTYPE_LOAD_BOEL);
	}

	public void setLoadVertexBatchSize(final int p_batchSize) {
		m_vertexBatchSize = p_batchSize;
	}

	// TODO import/export results (total vertices loaded, total edges loaded, roots? -> separate task and add roots to
	// nameservice?)

	@Override
	protected int loadGraphData(final String p_path) {

		Pair<List<OrderedEdgeList>, OrderedEdgeListRoots> edgeLists = setupEdgeLists(p_path);

		// we have to assume that the data order matches
		// the nodeIdx/localIdx sorting

		// add offset with each file we processed so we can concat multiple files
		long vertexIDOffset = 0;
		boolean somethingLoaded = false;
		for (OrderedEdgeList edgeList : edgeLists.first()) {
			somethingLoaded = true;
			m_loggerService.info(getClass(), "Loading from edge list " + edgeList);
			if (!load(edgeList, new RebaseVertexIDLocal(m_bootService.getNodeID(), vertexIDOffset)))
				return false;
			vertexIDOffset += edgeList.getTotalVertexCount();
		}

		if (!loadRoots(edgeLists.second(), new RebaseVertexIDLocal(m_bootService.getNodeID(), 0))) {
			m_loggerService.warn(getClass(), "Loading roots failed.");
		}

		if (!somethingLoaded) {
			m_loggerService.warn(getClass(), "There were no ordered edge lists to load.");
		}

		return 0;
	}

	// returns edge list sorted by nodeIdx and localIdx
	protected Pair<List<OrderedEdgeList>, OrderedEdgeListRoots> setupEdgeLists(final String p_path) {
		Pair<List<OrderedEdgeList>, OrderedEdgeListRoots> lists =
				new Pair<List<OrderedEdgeList>, OrderedEdgeListRoots>(new ArrayList<OrderedEdgeList>(), null);

		// check if directory
		File tmpFile = new File(p_path);
		if (!tmpFile.exists()) {
			m_loggerService.error(getClass(), "Cannot setup edge lists, path does not exist: " + p_path);
			return lists;
		}

		if (!tmpFile.isDirectory()) {
			m_loggerService.error(getClass(), "Cannot setup edge lists, path is not a directory: " + p_path);
			return lists;
		}

		// iterate files in dir, filter by pattern
		File[] files = tmpFile.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File p_dir, final String p_name) {
				String[] tokens = p_name.split("\\.");

				// looking for format xxx.oel or xxx.oel.<nidx>
				if (tokens.length > 1) {
					if (tokens[1].equals("oel") || tokens[1].equals("boel") || tokens[1].equals("roel")) {
						return true;
					}
				}

				return false;
			}
		});

		// add filtered files
		for (File file : files) {
			String[] tokens = file.getName().split("\\.");

			// looking for format xxx.oel or xxx.oel.<nidx>
			if (tokens.length > 1) {
				if (tokens[1].equals("oel")) {
					lists.m_first.add(new OrderedEdgeListTextFileThreadBuffering(file.getAbsolutePath(),
							m_vertexBatchSize * 1000));
				} else if (tokens[1].equals("boel")) {
					lists.m_first.add(new OrderedEdgeListBinaryFileThreadBuffering(file.getAbsolutePath(),
							m_vertexBatchSize * 1000));
				} else if (tokens[1].equals("roel")) {
					lists.m_second = new OrderedEdgeListRootsTextFile(file.getAbsolutePath());
				}
			}
		}

		// make sure our list is sorted by nodeIdx/localIdx
		lists.m_first.sort(new Comparator<OrderedEdgeList>() {
			@Override
			public int compare(final OrderedEdgeList p_lhs, final OrderedEdgeList p_rhs) {
				if (p_lhs.getNodeIndex() < p_rhs.getNodeIndex()) {
					return -1;
				} else if (p_lhs.getNodeIndex() > p_rhs.getNodeIndex()) {
					return 1;
				} else {
					return 0;
				}
			}
		});

		return lists;
	}

	protected boolean load(final OrderedEdgeList p_orderedEdgeList, final RebaseVertexID p_rebase) {
		Vertex2[] vertexBuffer = new Vertex2[m_vertexBatchSize];
		int readCount = 0;

		long verticesProcessed = 0;
		float previousProgress = 0.0f;

		m_totalVerticesLoaded = p_orderedEdgeList.getTotalVertexCount();

		m_loggerService.info(getClass(), "Loading started, vertex count: " + m_totalVerticesLoaded);

		while (true) {
			readCount = 0;
			while (readCount < vertexBuffer.length) {
				Vertex2 vertex = p_orderedEdgeList.readVertex();
				if (vertex == null) {
					break;
				}

				// re-basing of neighbors needed for multiple files
				// offset tells us how much to add
				// also add current node ID
				long[] neighbours = vertex.getNeighbours();
				p_rebase.rebase(neighbours);

				vertexBuffer[readCount] = vertex;
				readCount++;
				m_totalEdgesLoaded += neighbours.length;
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

			verticesProcessed += readCount;

			float curProgress = ((float) verticesProcessed) / m_totalVerticesLoaded;
			if (curProgress - previousProgress > 0.01) {
				previousProgress = curProgress;
				m_loggerService.info(getClass(), "Loading progress: " + (int) (curProgress * 100) + "%");
			}
		}

		m_loggerService.info(getClass(), "Loading done, vertex count: " + m_totalVerticesLoaded);

		return true;
	}

	protected boolean loadRoots(final OrderedEdgeListRoots p_orderedEdgeListRoots, final RebaseVertexID p_rebase) {
		ArrayList<Long> tmp = new ArrayList<Long>();
		m_loggerService.info(getClass(), "Loading roots started");

		while (true) {
			long root = p_orderedEdgeListRoots.getRoot();
			if (root == -1) {
				break;
			}

			tmp.add(p_rebase.rebase(root));
		}

		m_roots = new long[tmp.size()];
		for (int i = 0; i < tmp.size(); i++) {
			m_roots[i] = tmp.get(i);
		}

		m_loggerService.info(getClass(), "Loading roots done, count: " + m_roots.length);
		return true;
	}
}

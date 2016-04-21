
package de.hhu.bsinfo.dxgraph.load2.oel;

import java.util.concurrent.ConcurrentLinkedQueue;

import de.hhu.bsinfo.dxgraph.data.Vertex2;

public abstract class OrderedEdgeListThreadBuffering extends Thread implements OrderedEdgeList {

	private int m_nodeIndex = -1;
	private long m_fileVertexCount = -1;
	private int m_bufferLimit = 1000;

	private ConcurrentLinkedQueue<Vertex2> m_vertexBuffer = new ConcurrentLinkedQueue<>();

	// expecting filename: "xxx.oel" for single file or "xxx.oel.0", "xxx.oel.1" etc for split
	// for a single file, node index default to 0
	public OrderedEdgeListThreadBuffering(final String p_path, final int p_bufferLimit) {
		super("OrderedEdgeListFileBuffering " + p_path);
		m_bufferLimit = p_bufferLimit;

		String file = p_path;

		int lastIndexPath = file.lastIndexOf('/');
		if (lastIndexPath != -1) {
			file = file.substring(lastIndexPath + 1);
		}

		String[] tokens = file.split("\\.");
		if (tokens.length < 3) {
			m_nodeIndex = 0;
		} else {
			m_nodeIndex = Integer.parseInt(tokens[2]);
		}

		setupFile(p_path);

		m_fileVertexCount = readTotalVertexCount(p_path);

		start();
	}

	@Override
	public int getNodeIndex() {
		return m_nodeIndex;
	}

	@Override
	public long getTotalVertexCount() {
		return m_fileVertexCount;
	}

	@Override
	public Vertex2 readVertex() {
		while (true) {
			Vertex2 vertex = m_vertexBuffer.poll();
			if (vertex != null) {
				return vertex;
			}

			if (vertex == null && this.isAlive()) {
				Thread.yield();
			} else {
				return null;
			}
		}
	}

	@Override
	public void run() {
		while (true) {
			// don't flood memory
			if (m_vertexBuffer.size() > m_bufferLimit) {
				Thread.yield();
			}

			Vertex2 vertex = readFileVertex();
			if (vertex == null) {
				return;
			}

			m_vertexBuffer.add(vertex);
		}
	}

	protected abstract void setupFile(final String p_path);

	protected abstract long readTotalVertexCount(final String p_path);

	protected abstract Vertex2 readFileVertex();
}

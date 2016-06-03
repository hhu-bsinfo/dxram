
package de.hhu.bsinfo.dxgraph.load.oel;

import java.util.concurrent.ConcurrentLinkedQueue;

import de.hhu.bsinfo.dxgraph.data.Vertex;

/**
 * Base class running a buffered reader of vertex data in a separate thread to speed up loading.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
abstract class AbstractOrderedEdgeListThreadBuffering extends Thread implements OrderedEdgeList {

	private int m_bufferLimit = 1000;
	private ConcurrentLinkedQueue<Vertex> m_vertexBuffer = new ConcurrentLinkedQueue<>();

	/**
	 * Constructor
	 * @param p_path
	 *            Filepath to the ordered edge list file.
	 * @param p_bufferLimit
	 *            Max number of vertices to keep buffered for reading.
	 */
	AbstractOrderedEdgeListThreadBuffering(final String p_path, final int p_bufferLimit) {
		super("OrderedEdgeListFileBuffering " + p_path);
		m_bufferLimit = p_bufferLimit;

		String file = p_path;

		int lastIndexPath = file.lastIndexOf('/');
		if (lastIndexPath != -1) {
			file = file.substring(lastIndexPath + 1);
		}

		setupFile(p_path);

		start();
	}

	@Override
	public Vertex readVertex() {
		while (true) {
			Vertex vertex = m_vertexBuffer.poll();
			if (vertex != null) {
				return vertex;
			}

			if (this.isAlive()) {
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

			Vertex vertex = readFileVertex();
			if (vertex == null) {
				return;
			}

			m_vertexBuffer.add(vertex);
		}
	}

	/**
	 * Setup the file to read from.
	 * @param p_path
	 *            Filepath to the ordered edge list file.
	 */
	protected abstract void setupFile(final String p_path);

	/**
	 * Read a single vertex from the file.
	 * @return Vertex read from the file or null if eof.
	 */
	protected abstract Vertex readFileVertex();
}


package de.hhu.bsinfo.dxgraph.conv;

import java.util.Queue;

import de.hhu.bsinfo.utils.Pair;

/**
 * Thread copying the buffered data by the file reader threads and putting it into the vertex storage.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
public class BufferToStorageThread extends ConverterThread {
	private VertexStorage m_storage;
	private boolean m_isDirected;
	private Queue<Pair<Long, Long>> m_buffer;
	private boolean m_running = true;

	/**
	 * Constructor
	 * @param p_id
	 *            Thread id (0 based).
	 * @param p_storage
	 *            VertexStorage instance to put the data to.
	 * @param p_isDirected
	 *            Directed or undirected edges.
	 * @param p_buffer
	 *            Buffer filled with input data from the file readers (shared).
	 */
	public BufferToStorageThread(final int p_id, final VertexStorage p_storage, final boolean p_isDirected,
			final Queue<Pair<Long, Long>> p_buffer) {
		super("BufferToStorage " + p_id);

		m_storage = p_storage;
		m_isDirected = p_isDirected;
		m_buffer = p_buffer;
	}

	/**
	 * Set the thread running/shutdown.
	 * @param p_running
	 *            False to shutdown.
	 */
	public void setRunning(final boolean p_running) {
		m_running = p_running;
	}

	@Override
	public void run() {
		while (true) {
			Pair<Long, Long> pair = m_buffer.poll();
			if (!m_running && pair == null) {
				break;
			}

			if (pair != null) {
				long srcVertexId = m_storage.getVertexId(pair.first());
				long destVertexId = m_storage.getVertexId(pair.second());

				m_storage.putNeighbour(srcVertexId, destVertexId);
				// if we got directed edges as inputs, make sure we create undirected output
				if (m_isDirected) {
					m_storage.putNeighbour(destVertexId, srcVertexId);
				}
			} else {
				Thread.yield();
			}
		}
	}
}

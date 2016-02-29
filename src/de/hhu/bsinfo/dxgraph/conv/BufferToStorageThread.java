package de.hhu.bsinfo.dxgraph.conv;

import java.util.concurrent.ConcurrentLinkedQueue;

import de.hhu.bsinfo.utils.Pair;

public class BufferToStorageThread extends ConverterThread
{
	private VertexStorage m_storage = null;
	private boolean m_isDirected = false;
	private ConcurrentLinkedQueue<Pair<Long, Long>> m_buffer = null;
	private boolean m_running = true;
	
	public BufferToStorageThread(final int p_id, final VertexStorage p_storage, final boolean p_isDirected, final ConcurrentLinkedQueue<Pair<Long, Long>> p_buffer) {
		super("BufferToStorage " + p_id);
		
		m_storage = p_storage;
		m_isDirected = p_isDirected;
		m_buffer = p_buffer;
	}
	
	public void setRunning(final boolean running) {
		m_running = running;
	}
	
	@Override
	public void run() {
		while (true)
		{
			if (!m_running && m_buffer.isEmpty())
				break;
			
			Pair<Long, Long> pair = m_buffer.poll();
			if (pair != null)
			{
				long srcVertexId = m_storage.getVertexId(pair.first());
				long destVertexId = m_storage.getVertexId(pair.second());
				
				m_storage.putNeighbour(srcVertexId, destVertexId);
				// if we got directed edges as inputs, make sure we create undirected output
				if (m_isDirected) {
					m_storage.putNeighbour(destVertexId, srcVertexId);
				}
			}
			else
				Thread.yield();
		}
	}
}

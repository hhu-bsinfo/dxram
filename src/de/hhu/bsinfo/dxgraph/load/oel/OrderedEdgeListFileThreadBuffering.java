package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.hhu.bsinfo.dxgraph.data.Vertex2;

public class OrderedEdgeListFileThreadBuffering extends Thread implements OrderedEdgeList {
	
	private int m_nodeIndex = -1;
	private BufferedReader m_file = null;
	private long m_fileVertexCount = -1;
	private int m_bufferLimit = 1000;
	
	private ConcurrentLinkedQueue<Vertex2> m_vertexBuffer = new ConcurrentLinkedQueue<>();
	
	// expecting filename: "xxx.oel" for single file or "xxx.oel.0", "xxx.oel.1" etc for split
	// for a single file, node index default to 0
	public OrderedEdgeListFileThreadBuffering(final String p_path, final int p_bufferLimit)
	{
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
			
		try {
			m_file = new BufferedReader(new FileReader(p_path));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Cannot load graph from file '" + p_path + "', does not exist.");
		}
		
		// first line is the total vertex/line count of the file
		try {
			m_fileVertexCount = Long.parseLong(m_file.readLine());
		} catch (NumberFormatException | IOException e) {
			throw new RuntimeException("Cannot read vertex count (first line) from file '" + p_path + ".");
		}
		
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
		while (true)
		{
			Vertex2 vertex = m_vertexBuffer.poll();
			if (vertex != null)
				return vertex;
			
			if (vertex == null && this.isAlive()) {
				Thread.yield();
			}
			else
				return null;
		}
	}
	
	@Override
	public void run() {
		while (true) {
			// don't flood memory
			if (m_vertexBuffer.size() > m_bufferLimit) {
				Thread.yield();
			}
			
			Vertex2 vertex = new Vertex2();
			
			String line;
			try {
				line = m_file.readLine();
			} catch (IOException e) {
				return;
			}
			// eof
			if (line == null)
				return;
			
			// empty line = vertex with no neighbours
			if (!line.isEmpty())
			{
				String[] neighboursStr = line.split(",");
				vertex.setNeighbourCount(neighboursStr.length);
				long[] neighbours = vertex.getNeighbours();
				for (int i = 0; i < neighboursStr.length; i++) {
					neighbours[i] = Long.parseLong(neighboursStr[i]);
				}
			}
			
			m_vertexBuffer.add(vertex);
		}
	}
}

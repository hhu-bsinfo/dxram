package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import de.hhu.bsinfo.dxgraph.data.Vertex;

public class OrderedEdgeListFile implements OrderedEdgeList {
	
	private int m_nodeIndex = -1;
	private RandomAccessFile m_file = null;
	private long m_fileVertexCount = -1;
	
	// expecting filename: "xxx.oel" for single file or "xxx.oel.0", "xxx.oel.1" etc for split
	// for a single file, node index default to 0
	public OrderedEdgeListFile(final String p_path)
	{
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
			m_file = new RandomAccessFile(p_path, "r");
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Cannot load graph from file '" + p_path + "', does not exist.");
		}
		
		// first line is the total vertex/line count of the file
		try {
			m_fileVertexCount = Long.parseLong(m_file.readLine());
		} catch (NumberFormatException | IOException e) {
			throw new RuntimeException("Cannot read vertex count (first line) from file '" + p_path + ".");
		}
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
	public Vertex readVertex() {
		Vertex vertex = new Vertex();
		
		String line;
		try {
			line = m_file.readLine();
		} catch (IOException e) {
			return null;
		}
		// eof
		if (line == null)
			return null;
		
		// empty line = vertex with no neighbours
		if (!line.isEmpty())
		{
			String[] neighbours = line.split(",");
			for (String v : neighbours)
			{
				vertex.getNeighbours().add(Long.parseLong(v));
			}
		}
		
		return vertex;
	}
}

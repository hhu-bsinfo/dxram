package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import de.hhu.bsinfo.dxgraph.data.Vertex2;

public class OrderedEdgeListTextFileThreadBuffering extends OrderedEdgeListThreadBuffering {
	
	private BufferedReader m_file;
	
	public OrderedEdgeListTextFileThreadBuffering(final String p_path, final int p_bufferLimit)
	{
		super(p_path, p_bufferLimit);
	}

	@Override
	protected void setupFile(String p_path) {
		try {
			m_file = new BufferedReader(new FileReader(p_path));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Cannot load graph from file '" + p_path + "', does not exist.");
		}
	}

	@Override
	protected long readTotalVertexCount(String p_path) {
		// first line is the total vertex/line count of the file
		try {
			return Long.parseLong(m_file.readLine());
		} catch (NumberFormatException | IOException e) {
			throw new RuntimeException("Cannot read vertex count (first line) from file '" + p_path + ".");
		}
	}

	@Override
	protected Vertex2 readFileVertex() {
		Vertex2 vertex = new Vertex2();
		
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
			String[] neighboursStr = line.split(",");
			vertex.setNeighbourCount(neighboursStr.length);
			long[] neighbours = vertex.getNeighbours();
			for (int i = 0; i < neighboursStr.length; i++) {
				neighbours[i] = Long.parseLong(neighboursStr[i]);
			}
		}
		
		return vertex;
	}
}

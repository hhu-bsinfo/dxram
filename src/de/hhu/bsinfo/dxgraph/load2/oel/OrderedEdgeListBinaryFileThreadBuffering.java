
package de.hhu.bsinfo.dxgraph.load2.oel;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import de.hhu.bsinfo.dxgraph.data.Vertex2;

public class OrderedEdgeListBinaryFileThreadBuffering extends OrderedEdgeListThreadBuffering {

	private DataInputStream m_file;

	public OrderedEdgeListBinaryFileThreadBuffering(final String p_path, final int p_bufferLimit) {
		super(p_path, p_bufferLimit);
	}

	@Override
	protected void setupFile(final String p_path) {
		try {
			m_file = new DataInputStream(new BufferedInputStream(new FileInputStream(p_path)));
		} catch (final FileNotFoundException e) {
			throw new RuntimeException("Cannot load graph from file '" + p_path + "', does not exist.");
		}
	}

	@Override
	protected long readTotalVertexCount(final String p_path) {
		// first long is total vertex count
		try {
			return m_file.readLong();
		} catch (final NumberFormatException | IOException e) {
			throw new RuntimeException("Cannot read vertex count (first line) from file '" + p_path + ".");
		}
	}

	@Override
	protected Vertex2 readFileVertex() {
		Vertex2 vertex = new Vertex2();

		try {
			int count = m_file.readInt();
			vertex.setNeighbourCount(count);
			long[] neighbours = vertex.getNeighbours();
			for (int i = 0; i < neighbours.length; i++) {
				neighbours[i] = m_file.readLong();
			}
		} catch (final IOException e1) {
			return null;
		}

		return vertex;
	}
}

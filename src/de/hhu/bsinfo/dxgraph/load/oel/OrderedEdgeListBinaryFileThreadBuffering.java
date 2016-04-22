
package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import de.hhu.bsinfo.dxgraph.data.Vertex;

/**
 * Implementation reading vertex data from a buffer filled by a separate file reading thread.
 * The vertex data is stored in binary format.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class OrderedEdgeListBinaryFileThreadBuffering extends AbstractOrderedEdgeListThreadBuffering {

	private DataInputStream m_file;

	/**
	 * Constructor
	 * @param p_path
	 *            Filepath of the file to read.
	 * @param p_bufferLimit
	 *            Max vertices to keep buffered.
	 */
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
	protected Vertex readFileVertex() {
		Vertex vertex = new Vertex();

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

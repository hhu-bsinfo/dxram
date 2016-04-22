
package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import de.hhu.bsinfo.dxgraph.data.Vertex;

/**
 * Implementation reading vertex data from a buffer filled by a separate file reading thread.
 * The vertex data is stored in text format.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class OrderedEdgeListTextFileThreadBuffering extends AbstractOrderedEdgeListThreadBuffering {

	private BufferedReader m_file;

	/**
	 * Constructor
	 * @param p_path
	 *            Filepath of the file to read.
	 * @param p_bufferLimit
	 *            Max vertices to keep buffered.
	 */
	public OrderedEdgeListTextFileThreadBuffering(final String p_path, final int p_bufferLimit) {
		super(p_path, p_bufferLimit);
	}

	@Override
	protected void setupFile(final String p_path) {
		try {
			m_file = new BufferedReader(new FileReader(p_path));
		} catch (final FileNotFoundException e) {
			throw new RuntimeException("Cannot load graph from file '" + p_path + "', does not exist.");
		}
	}

	@Override
	protected Vertex readFileVertex() {
		Vertex vertex = new Vertex();

		String line;
		try {
			line = m_file.readLine();
		} catch (final IOException e) {
			return null;
		}
		// eof
		if (line == null) {
			return null;
		}

		// empty line = vertex with no neighbours
		if (!line.isEmpty()) {
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

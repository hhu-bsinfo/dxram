
package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import de.hhu.bsinfo.dxgraph.data.Vertex;

/**
 * Implementation reading vertex data from a buffer filled by a separate file reading thread.
 * The vertex data is stored in binary format.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class OrderedEdgeListBinaryFileThreadBuffering extends AbstractOrderedEdgeListThreadBuffering {

	private RandomAccessFile m_file;

	/**
	 * Constructor
	 *
	 * @param p_path        Filepath of the file to read.
	 * @param p_bufferLimit Max vertices to keep buffered.
	 */
	public OrderedEdgeListBinaryFileThreadBuffering(final String p_path, final int p_bufferLimit,
			final long p_partitionStartOffset, final long p_partitionEndOffset) {
		super(p_path, p_bufferLimit, p_partitionStartOffset, p_partitionEndOffset);
	}

	@Override
	protected void setupFile(final String p_path) {
		try {
			m_file = new RandomAccessFile(p_path, "r");
			m_file.seek(m_partitionStartOffset);
			if (m_partitionEndOffset == Long.MAX_VALUE) {
				m_partitionEndOffset = m_file.length();
			}
		} catch (final FileNotFoundException e) {
			throw new RuntimeException("Cannot load graph from file '" + p_path + "', does not exist.");
		} catch (final IOException e) {
			throw new RuntimeException(
					"Seeking to position " + m_partitionStartOffset + " on file '" + p_path + "' failed");
		}
	}

	@Override
	protected Vertex readFileVertex() {
		Vertex vertex = new Vertex();

		try {
			if (m_file.getFilePointer() < m_partitionEndOffset) {
				int count = m_file.readInt();
				vertex.setNeighbourCount(count);
				long[] neighbours = vertex.getNeighbours();
				for (int i = 0; i < neighbours.length; i++) {
					neighbours[i] = m_file.readLong();
				}
			} else {
				return null;
			}
		} catch (final IOException e1) {
			return null;
		}

		return vertex;
	}
}

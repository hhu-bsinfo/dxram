
package de.hhu.bsinfo.dxgraph.load.oel;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
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

	private DataInputStream m_file;
	private long m_position;

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
			RandomAccessFile file = new RandomAccessFile(p_path, "r");
			file.seek(m_partitionStartOffset);
			m_file = new DataInputStream(new BufferedInputStream(new FileInputStream(file.getFD())));
			if (m_partitionEndOffset == Long.MAX_VALUE) {
				m_partitionEndOffset = file.length();
			}
			m_position = m_partitionStartOffset;
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
			if (m_position < m_partitionEndOffset) {
				int count = Integer.reverseBytes(m_file.readInt());
				m_position += Integer.BYTES;
				vertex.setNeighbourCount(count);
				long[] neighbours = vertex.getNeighbours();
				for (int i = 0; i < neighbours.length; i++) {
					neighbours[i] = Long.reverseBytes(m_file.readLong());
				}
				m_position += Long.BYTES * count;
			} else {
				return null;
			}
		} catch (final IOException e1) {
			return null;
		}

		return vertex;
	}
}

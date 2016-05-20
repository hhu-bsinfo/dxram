
package de.hhu.bsinfo.dxgraph.conv;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Multi threaded converter, expecting edge list in text form separated by spaces
 * and outputting an ordered edge list (text form).
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
public final class ConverterTextEdgeListToOel extends AbstractConverter {
	/**
	 * Constructor
	 */
	private ConverterTextEdgeListToOel() {
		super("Convert a text edge list to an ordered edge list (text file)");
	}

	/**
	 * Main entry point.
	 *
	 * @param p_args Console arguments.
	 */
	public static void main(final String[] p_args) {
		AbstractMain main = new ConverterTextEdgeListToOel();
		main.run(p_args);
	}

	@Override
	protected AbstractFileReaderThread createReaderInstance(final String p_inputPath,
			final BinaryEdgeBuffer p_buffer) {
		return new FileReaderTextThread(p_inputPath, p_buffer);
	}

	@Override
	protected AbstractFileWriterThread createWriterInstance(final String p_outputPath, final int p_id,
			final long p_idRangeStartIncl,
			final long p_idRangeEndExcl, final VertexStorage p_storage) {
		return new FileWriterTextThread(p_outputPath, p_id, p_idRangeStartIncl, p_idRangeEndExcl, p_storage);
	}

	@Override
	protected VertexStorage createVertexStorageInstance(final int p_vertexIdOffset) {
		return new VertexStorageTextSimple();
	}

	@Override
	protected void convertBFSRootList(final String p_outputPath, final String p_inputRootFile,
			final VertexStorage p_storage) {
		// TODO
	}

	/**
	 * File reader for the text edge list graph data.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
	 */
	private static class FileReaderTextThread extends AbstractFileReaderThread {
		/**
		 * Constructor
		 *
		 * @param p_inputPath Path of the file to read.
		 * @param p_buffer    Shared buffer to read the data to.
		 */
		FileReaderTextThread(final String p_inputPath, final BinaryEdgeBuffer p_buffer) {
			super(p_inputPath, p_buffer);
		}

		@Override
		public int parse() {
			BufferedReader reader;
			try {
				reader = new BufferedReader(new FileReader(m_inputPath));
			} catch (final FileNotFoundException e1) {
				System.out.println("Opening buffered reader failed: " + e1.getMessage());
				return -1;
			}
			long fileSize = 0;
			try {
				RandomAccessFile raf = new RandomAccessFile(m_inputPath, "r");
				fileSize = raf.length();
				raf.close();
			} catch (final IOException ignored) {
			}

			System.out.println("Caching input of edge list " + m_inputPath);

			long lineCount = 0;
			long readByteCount = 0;
			while (true) {
				String line;
				try {
					line = reader.readLine();
				} catch (final IOException e) {
					try {
						reader.close();
					} catch (final IOException ignored) {
					}
					System.out.println("Reading line failed: " + e.getMessage());
					return -2;
				}

				lineCount++;
				// eof
				if (line == null) {
					break;
				}

				readByteCount += line.length() + 1;

				String[] tokens = line.split(" ");
				if (tokens.length != 2) {
					System.out.println("Invalid token count " + tokens.length + " in line " + lineCount + ", skipping");
					continue;
				}

				long srcNode = Long.parseLong(tokens[0]);
				long destNode = Long.parseLong(tokens[1]);

				m_buffer.pushBack(srcNode, destNode);
				updateProgress("ByteDataPos", readByteCount, fileSize);
			}

			try {
				reader.close();
			} catch (final IOException ignored) {
			}

			return 0;
		}
	}
}

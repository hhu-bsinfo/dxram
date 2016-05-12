
package de.hhu.bsinfo.dxgraph.conv;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Single threaded converter, expecting edge list in binary form:
 * 8 bytes source nodeId and 8 bytes destination node id.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
public abstract class AbstractBinaryEdgeListTo extends AbstractConverter {
	/**
	 * Constructor
	 *
	 * @param p_description Description for the converter.
	 */
	protected AbstractBinaryEdgeListTo(final String p_description) {
		super(p_description);
	}

	@Override
	protected AbstractFileReaderThread createReaderInstance(final String p_inputPath,
			final BinaryEdgeBuffer p_buffer) {
		return new FileReaderBinaryThread(p_inputPath, p_buffer);
	}

	@Override
	protected void convertBFSRootList(final String p_outputPath, final String p_inputRootFile,
			final VertexStorage p_storage) {
		// adjust output path
		String outputPath = p_outputPath;

		if (!outputPath.endsWith("/")) {
			outputPath += "/";
		}

		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(outputPath + "out.roel"));
		} catch (final IOException e1) {
			System.out.println("Opening buffered reader failed: " + e1.getMessage());
			return;
		}

		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(p_inputRootFile, "r");
		} catch (final FileNotFoundException e) {
			System.out.println("Opening input file " + p_inputRootFile + " failed: " + e.getMessage());
			try {
				writer.close();
			} catch (final IOException e1) {
			}
			return;
		}

		ByteBuffer buffer = ByteBuffer.allocate(1024 * 8 * 2);
		buffer.order(ByteOrder.LITTLE_ENDIAN);

		try {
			long fileLength = file.length();
			long bytesRead = 0;
			do {
				int read = file.read(buffer.array());
				if (read == -1) {
					break;
				}

				bytesRead += read;
				buffer.limit(read);

				while (buffer.hasRemaining()) {
					Long node = buffer.getLong();

					long vertexId = p_storage.getVertexId(node);

					writer.write(Long.toString(vertexId) + "\n");
				}

				writer.flush();
				buffer.clear();
			} while (bytesRead < fileLength);

			file.close();
		} catch (final IOException e) {
			System.out.println("Reading from input file failed: " + e.getMessage());
		}
	}

	/**
	 * File reader for the binary edge list graph data.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
	 */
	protected static class FileReaderBinaryThread extends AbstractFileReaderThread {
		/**
		 * Constructor
		 *
		 * @param p_inputPath Path of the file to read.
		 * @param p_buffer    Shared buffer to read the data to.
		 */
		FileReaderBinaryThread(final String p_inputPath, final BinaryEdgeBuffer p_buffer) {
			super(p_inputPath, p_buffer);
		}

		@Override
		public int parse() {
			RandomAccessFile file = null;
			try {
				file = new RandomAccessFile(m_inputPath, "r");
			} catch (final FileNotFoundException e) {
				System.out.println("Opening input file " + m_inputPath + " failed: " + e.getMessage());
				return -1;
			}

			ByteBuffer buffer = ByteBuffer.allocate(32 * 1024);
			buffer.order(ByteOrder.LITTLE_ENDIAN);

			try {
				long fileLength = file.length();
				long bytesRead = 0;
				do {
					int read = file.read(buffer.array());

					if (read == -1) {
						break;
					}

					bytesRead += read;
					buffer.limit(read);

					// size() performs really bad here, so try to call it very rarely
					// by adding an addition
					while (buffer.hasRemaining()) {
						long srcNode = buffer.getLong();
						long destNode = buffer.getLong();

						while (!m_buffer.pushBack(srcNode, destNode)) {
							Thread.yield();
						}

						updateProgress("BinaryDataReading", bytesRead, fileLength);
					}

					buffer.clear();
				} while (bytesRead < fileLength);

				file.close();
			} catch (final IOException e) {
				System.out.println("Reading from input file failed: " + e.getMessage());
				return -2;
			}

			// wait until queue is empty
			//			int queueSize = 0;
			do {
				//				queueSize = m_bufferQueue.size();
				//				System.out.println("BufferQueue remaining: " + m_bufferQueue.size());
				//				System.out.flush();
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {
					e.printStackTrace();
				}
			} while (!m_buffer.isEmpty());

			return 0;
		}
	}
}

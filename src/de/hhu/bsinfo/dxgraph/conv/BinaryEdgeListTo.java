
package de.hhu.bsinfo.dxgraph.conv;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Queue;

import de.hhu.bsinfo.utils.Pair;

/**
 * Single threaded converter, expecting edge list in binary form:
 * 8 bytes source nodeId and 8 bytes destination node id.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.02.16
 */
public abstract class BinaryEdgeListTo extends Converter {
	protected BinaryEdgeListTo(final String p_description) {
		super(p_description);
	}

	@Override
	protected FileReaderThread createReaderInstance(final String p_inputPath,
			final Queue<Pair<Long, Long>> p_bufferQueue, final int p_maxQueueSize) {
		return new FileReaderBinaryThread(p_inputPath, p_bufferQueue, p_maxQueueSize);
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
			} catch (final IOException e1) {}
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

	protected static class FileReaderBinaryThread extends FileReaderThread {
		public FileReaderBinaryThread(final String p_inputPath, final Queue<Pair<Long, Long>> p_bufferQueue,
				final int p_maxQueueSize) {
			super(p_inputPath, p_bufferQueue, p_maxQueueSize);
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

					// size() performs really bad here, so try to call it very rarely
					// by adding an addition
					int size = m_bufferQueue.size();
					while (buffer.hasRemaining()) {
						Long srcNode = buffer.getLong();
						Long destNode = buffer.getLong();

						if (size > m_maxQueueSize) {
							Thread.yield();
							size = m_bufferQueue.size();
						}

						m_bufferQueue.add(new Pair<Long, Long>(srcNode, destNode));
						size++;

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
			int queueSize = 0;
			do {
				queueSize = m_bufferQueue.size();
				System.out.println("BufferQueue remaining: " + m_bufferQueue.size());
				System.out.flush();
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e) {
					e.printStackTrace();
				}
			} while (queueSize > 0);

			return 0;
		}
	}
}

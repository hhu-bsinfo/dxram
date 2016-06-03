
package de.hhu.bsinfo.menet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * NIO interface. Every access to the channels is done here.
 * @author Marc Ewert 03.09.2014
 */
final class NIOInterface {

	// Attributes
	private int m_incomingBufferSize;
	private int m_outgoingBufferSize;
	private int m_flowControlWindowSize;

	private ByteBuffer m_readBuffer;
	private ByteBuffer m_writeBuffer;

	/**
	 * Creates an instance of NIOInterface
	 * @param p_incomingBufferSize
	 *            the size of incoming buffer
	 * @param p_outgoingBufferSize
	 *            the size of outgoing buffer
	 * @param p_flowControlWindowSize
	 *            the maximal number of ByteBuffer to schedule for sending/receiving
	 */
	NIOInterface(final int p_incomingBufferSize, final int p_outgoingBufferSize, final int p_flowControlWindowSize) {
		m_incomingBufferSize = p_incomingBufferSize;
		m_outgoingBufferSize = p_outgoingBufferSize;
		m_flowControlWindowSize = p_flowControlWindowSize;

		m_readBuffer = ByteBuffer.allocateDirect(p_incomingBufferSize);
		m_writeBuffer = ByteBuffer.allocateDirect(m_outgoingBufferSize);
	}

	/**
	 * Creates a new connection
	 * @param p_nodeMap
	 *            the node map
	 * @param p_messageDirectory
	 *            the message directory
	 * @param p_channel
	 *            the channel of the connection
	 * @param p_messageCreator
	 *            the message creator
	 * @param p_nioSelector
	 *            the NIOSelector
	 * @param p_numberOfBuffers
	 *            the number of buffers to schedule
	 * @throws IOException
	 *             if the connection could not be created
	 * @return the new NIOConnection
	 */
	protected NIOConnection initIncomingConnection(final NodeMap p_nodeMap, final MessageDirectory p_messageDirectory, final SocketChannel p_channel,
			final MessageCreator p_messageCreator, final NIOSelector p_nioSelector, final int p_numberOfBuffers)
			throws IOException {
		NIOConnection connection = null;
		ByteBuffer buffer;

		m_readBuffer.clear();

		if (p_channel.read(m_readBuffer) == -1) {
			p_channel.keyFor(p_nioSelector.getSelector()).cancel();
			p_channel.close();
		} else {
			m_readBuffer.flip();

			connection = new NIOConnection(m_readBuffer.getShort(), p_nodeMap, p_messageDirectory, p_channel, p_messageCreator,
					p_nioSelector, p_numberOfBuffers, m_incomingBufferSize, m_outgoingBufferSize, m_flowControlWindowSize);
			p_channel.register(p_nioSelector.getSelector(), SelectionKey.OP_READ, connection);

			if (m_readBuffer.hasRemaining()) {
				buffer = ByteBuffer.allocate(m_readBuffer.remaining());
				buffer.put(m_readBuffer);
				buffer.flip();

				connection.addIncoming(buffer);
			}
		}

		return connection;
	}

	/**
	 * Reads from the given connection
	 * m_buffer needs to be synchronized externally
	 * @param p_connection
	 *            the Connection
	 * @throws IOException
	 *             if the data could not be read
	 * @return whether reading from channel was successful or not (connection is closed then)
	 */
	protected boolean read(final NIOConnection p_connection) throws IOException {
		boolean ret = true;
		long readBytes = 0;
		ByteBuffer buffer;

		m_readBuffer.clear();
		while (true) {
			try {
				readBytes = p_connection.getChannel().read(m_readBuffer);
			} catch (final ClosedChannelException e) {
				// Channel is closed -> ignore
				break;
			}
			if (readBytes == -1) {
				// Connection closed
				ret = false;
				break;
			} else if (readBytes == 0 && m_readBuffer.position() != 0) {
				// There is nothing more to read at the moment
				m_readBuffer.flip();
				buffer = ByteBuffer.allocate(m_readBuffer.limit());
				buffer.put(m_readBuffer);
				buffer.rewind();

				p_connection.addIncoming(buffer);

				break;
			}
		}

		return ret;
	}

	/**
	 * Writes to the given connection
	 * @param p_connection
	 *            the connection
	 * @return whether all data could be written or data is left
	 * @throws IOException
	 *             if the data could not be written
	 */
	protected boolean write(final NIOConnection p_connection) throws IOException {
		boolean ret = true;
		int bytes;
		int size;
		ByteBuffer buffer;
		ByteBuffer view;
		ByteBuffer slice;
		ByteBuffer buf;

		buffer = p_connection.getOutgoingBytes(m_writeBuffer, m_outgoingBufferSize);
		if (buffer != null) {
			if (buffer.remaining() > m_outgoingBufferSize) {
				// The write-Method for NIO SocketChannels is very slow for large buffers to write regardless of
				// the length of the actual written data -> simulate a smaller buffer by slicing it
				outerloop: while (buffer.remaining() > 0) {
					size = Math.min(buffer.remaining(), m_outgoingBufferSize);
					view = buffer.slice();
					view.limit(size);

					int tries = 0;
					while (view.remaining() > 0) {
						try {
							bytes = p_connection.getChannel().write(view);
							if (bytes == 0) {
								if (++tries == 1000) {
									// Read-buffer on the other side is full. Abort writing and schedule buffer for next write
									buffer.position(buffer.position() + size - view.remaining());

									if (buffer == m_writeBuffer) {
										// Copy buffer to avoid manipulation of scheduled data
										slice = buffer.slice();
										buf = ByteBuffer.allocateDirect(slice.remaining());
										buf.put(slice);
										buf.rewind();

										p_connection.addBuffer(buf);
									} else {
										p_connection.addBuffer(buffer);
									}
									ret = false;
									break outerloop;
								}
							} else {
								tries = 0;
							}
						} catch (final ClosedChannelException e) {
							// Channel is closed -> ignore
							break;
						}
					}
					buffer.position(buffer.position() + size);
				}
			} else {
				int tries = 0;
				while (buffer.remaining() > 0) {
					try {
						bytes = p_connection.getChannel().write(buffer);
						if (bytes == 0) {
							if (++tries == 1000) {
								// Read-buffer on the other side is full. Abort writing and schedule buffer for next write
								if (buffer == m_writeBuffer) {
									// Copy buffer to avoid manipulation of scheduled data
									slice = buffer.slice();
									buf = ByteBuffer.allocateDirect(slice.remaining());
									buf.put(slice);
									buf.rewind();

									p_connection.addBuffer(buf);
								} else {
									p_connection.addBuffer(buffer);
								}
								ret = false;
								break;
							}
						} else {
							tries = 0;
						}
					} catch (final ClosedChannelException e) {
						// Channel is closed -> ignore
						break;
					}
				}
			}
			// ThroughputStatistic.getInstance().outgoingExtern(writtenBytes - length);
		}
		if (ret) {
			ret = !p_connection.dataLeftToWrite();
		}

		return ret;
	}

	/**
	 * Finishes the connection process for the given connection
	 * @param p_connection
	 *            the connection
	 * @throws IOException
	 *             if connection could not be finalized
	 */
	protected static void connect(final NIOConnection p_connection) {
		try {
			if (p_connection.getChannel().isConnectionPending()) {
				p_connection.getChannel().finishConnect();
				p_connection.connected();
			}
		} catch (final IOException e) {/* ignore */}
	}

}

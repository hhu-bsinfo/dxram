
package de.uniduesseldorf.dxram.core.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import de.uniduesseldorf.dxram.core.io.InputHelper;

/**
 * NIO interface. Every access to the channels is done here.
 * @author Marc Ewert 03.09.2014
 */
final class NIOInterface {

	// Attributes
	private static ByteBuffer m_readBuffer = ByteBuffer.allocateDirect(NIOConnectionCreator.RECEIVE_BYTES);
	private static ByteBuffer m_writeBuffer = ByteBuffer.allocateDirect(NIOConnectionCreator.SEND_BYTES);

	/**
	 * Hidden constructor as this class only contains static members and methods
	 */
	private NIOInterface() {}

	/**
	 * Creates a new connection
	 * @param p_channel
	 *            the channel of the connection
	 * @param p_nioSelector
	 *            the NIOSelector
	 * @throws IOException
	 *             if the connection could not be created
	 * @return the new NIOConnection
	 */
	protected static NIOConnection initIncomingConnection(final SocketChannel p_channel, final NIOSelector p_nioSelector) throws IOException {
		NIOConnection connection = null;
		ByteBuffer buffer;

		m_readBuffer.clear();

		if (p_channel.read(m_readBuffer) == -1) {
			p_channel.keyFor(p_nioSelector.getSelector()).cancel();
			p_channel.close();
		} else {
			m_readBuffer.flip();

			connection = new NIOConnection(InputHelper.readNodeID(m_readBuffer), p_channel, p_nioSelector);
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
	protected static boolean read(final NIOConnection p_connection) throws IOException {
		boolean ret = true;
		long readBytes = 0;
		ByteBuffer buffer;

		try {
			m_readBuffer.clear();
			while (m_readBuffer.position() < m_readBuffer.capacity()) {
				// m_readBuffer.limit(m_readBuffer.position() + Math.min(NIOConnectionCreator.INCOMING_BUFFER_SIZE, m_readBuffer.remaining()));

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
					m_readBuffer.limit(m_readBuffer.position());
					m_readBuffer.position(0);

					buffer = ByteBuffer.allocate(m_readBuffer.limit());
					buffer.put(m_readBuffer);
					buffer.flip();
					p_connection.addIncoming(buffer);
					break;
				}
			}
		} catch (final IOException e) {
			System.out.println("WARN::Could not read from channel (" + p_connection.getDestination() + ")");
			throw e;
		}

		return ret;
	}

	/**
	 * Writes to the given connection
	 * @param p_connection
	 *            the connection
	 * @throws IOException
	 *             if the data could not be written
	 * @return whether the interest should be set to read again or not
	 */
	protected static boolean write(final NIOConnection p_connection) throws IOException {
		boolean ret = true;
		int length = 0;
		int bytes;
		int size;
		ByteBuffer view;
		ByteBuffer buffer;

		buffer = p_connection.getOutgoingBytes(m_writeBuffer, NIOConnectionCreator.SEND_BYTES);
		try {
			if (buffer != null) {
				if (buffer.remaining() > NIOConnectionCreator.SEND_BYTES) {
					// The write-Method for NIO SocketChannels is very slow for large buffers to write regardless of
					// the length of the actual written data -> simulate a smaller buffer by slicing it
					outerloop: while (buffer.remaining() > 0) {
						size = Math.min(buffer.remaining(), NIOConnectionCreator.SEND_BYTES);
						view = buffer.slice();
						view.limit(size);

						length = view.remaining();
						int tries = 0;
						while (length > 0) {
							try {
								bytes = p_connection.getChannel().write(view);
								length -= bytes;

								if (bytes == 0) {
									if (++tries == 10000) {
										System.out.println("Cannot write buffer because receive buffer has not been read for a while.");
										buffer.position(buffer.position() + size - length);
										view = buffer.slice();
										p_connection.addBuffer(view);
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
					length = buffer.remaining();
					int tries = 0;
					while (length > 0) {
						try {
							bytes = p_connection.getChannel().write(buffer);
							length -= bytes;

							if (bytes == 0) {
								if (++tries == 10000) {
									System.out.println("Cannot write buffer because receive buffer has not been read for a while.");
									p_connection.addBuffer(buffer);
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
		} catch (final IOException e) {
			System.out.println("WARN::Could not write to channel (" + p_connection.getDestination() + ")");
			throw e;
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

package de.hhu.bsinfo.ethnet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * NIO interface. Every access to the channels is done here.
 *
 * @author Marc Ewert, marc.ewert@hhu.de, 03.09.2014
 */
class NIOInterface {

    private static final Logger LOGGER = LogManager.getFormatterLogger(NIOInterface.class.getSimpleName());

    // Attributes
    private int m_outgoingBufferSize;

    private ByteBuffer m_readBuffer;
    private ByteBuffer m_writeBuffer;

    /**
     * Creates an instance of NIOInterface
     *
     * @param p_incomingBufferSize
     *     the size of incoming buffer
     * @param p_outgoingBufferSize
     *     the size of outgoing buffer
     */
    NIOInterface(final int p_incomingBufferSize, final int p_outgoingBufferSize) {
        m_outgoingBufferSize = p_outgoingBufferSize;

        m_readBuffer = ByteBuffer.allocateDirect(p_incomingBufferSize);
        m_writeBuffer = ByteBuffer.allocateDirect(m_outgoingBufferSize);
    }

    /**
     * Finishes the connection process for the given connection
     *
     * @param p_connection
     *     the connection
     */
    protected static void connect(final NIOConnection p_connection) {
        try {
            if (p_connection.getChannel().isConnectionPending()) {
                p_connection.getChannel().finishConnect();
                p_connection.connected();
            }
        } catch (final IOException e) { /* ignore */ }
    }

    /**
     * Reads from the given connection
     * m_buffer needs to be synchronized externally
     *
     * @param p_connection
     *     the Connection
     * @return whether reading from channel was successful or not (connection is closed then)
     * @throws IOException
     *     if the data could not be read
     */
    protected boolean read(final NIOConnection p_connection) throws IOException {
        boolean ret = true;
        long readBytes;
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
     *
     * @param p_connection
     *     the connection
     * @return whether all data could be written or data is left
     * @throws IOException
     *     if the data could not be written
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
                outerloop:
                while (buffer.remaining() > 0) {
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

                                    if (buffer.equals(m_writeBuffer)) {
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
                                if (buffer.equals(m_writeBuffer)) {
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
     * Reads the NodeID of the remote node that creates this new connection
     *
     * @param p_channel
     *     the channel of the connection
     * @param p_nioSelector
     *     the NIOSelector
     * @return the NodeID
     * @throws IOException
     *     if the connection could not be created
     */
    short readRemoteNodeID(final SocketChannel p_channel, final NIOSelector p_nioSelector) throws IOException {
        short ret;
        int bytes;
        int counter = 0;
        ByteBuffer buffer = ByteBuffer.allocate(2);

        m_readBuffer.clear();
        while (counter < buffer.capacity()) {
            bytes = p_channel.read(buffer);
            if (bytes == -1) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not read remote NodeID from new incoming connection!");
                // #endif /* LOGGER >= ERROR */
                p_channel.keyFor(p_nioSelector.getSelector()).cancel();
                p_channel.close();
                return -1;
            }
            counter += bytes;
        }
        buffer.flip();
        ret = buffer.getShort();

        return ret;
    }

}

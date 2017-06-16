package de.hhu.bsinfo.net.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.AbstractPipeIn;
import de.hhu.bsinfo.net.core.DataReceiver;
import de.hhu.bsinfo.net.core.MessageCreator;
import de.hhu.bsinfo.net.core.MessageDirectory;
import de.hhu.bsinfo.net.core.RequestMap;

/**
 * Created by nothaas on 6/9/17.
 */
public class NIOPipeIn extends AbstractPipeIn {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractPipeIn.class.getSimpleName());

    private SocketChannel m_incomingChannel;
    private final BufferPool m_bufferPool;
    private final MessageCreator m_messageCreator;
    private final ByteBuffer m_flowControlBytes;

    private final NIOConnection m_parentConnection;

    public NIOPipeIn(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl,
            final MessageDirectory p_messageDirectory, final RequestMap p_requestMap, final DataReceiver p_dataReceiver, final BufferPool p_bufferPool,
            final MessageCreator p_messageCreator, final NIOConnection p_parentConnection) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_messageDirectory, p_requestMap, p_dataReceiver);

        m_incomingChannel = null;
        m_bufferPool = p_bufferPool;
        m_messageCreator = p_messageCreator;
        m_flowControlBytes = ByteBuffer.allocateDirect(Integer.BYTES);

        m_parentConnection = p_parentConnection;
    }

    public SocketChannel getChannel() {
        return m_incomingChannel;
    }

    public void bindIncomingChannel(final SocketChannel p_channel) {
        m_incomingChannel = p_channel;
    }

    /**
     * Reads from the given connection
     * m_buffer needs to be synchronized externally
     *
     * @return whether reading from channel was successful or not (connection is closed then)
     * @throws IOException
     *         if the data could not be read
     */
    public boolean read() throws IOException {
        boolean ret = true;
        long readBytes;
        ByteBuffer buffer;

        buffer = m_bufferPool.getBuffer();

        while (true) {
            readBytes = m_incomingChannel.read(buffer);
            if (readBytes == -1) {
                // Connection closed
                ret = false;
                break;
            } else if (readBytes == 0 && buffer.position() != 0 || readBytes >= m_bufferPool.getOSBufferSize() * 0.9) {
                // There is nothing more to read at the moment
                buffer.limit(buffer.position());
                buffer.rewind();

                // #if LOGGER >= TRACE
                LOGGER.trace("Posting receive buffer (limit %d) to connection 0x%X", buffer.limit(), getDestinationNodeId());
                // #endif /* LOGGER >= TRACE */

                // Avoid congestion by not allowing more than m_numberOfBuffers buffers to be cached for reading
                while (!m_messageCreator.pushJob(m_parentConnection, buffer)) {
                    // #if LOGGER == TRACE
                    LOGGER.trace("Network-Selector: Job queue is full!");
                    // #endif /* LOGGER == TRACE */

                    Thread.yield();
                }

                break;
            }
        }

        return ret;
    }

    /**
     * Write flow control data
     */
    public void writeFlowControlBytes() throws IOException {
        int bytes = 0;
        int tries = 0;

        m_flowControlBytes.rewind();
        m_flowControlBytes.putInt(getFlowControl().getAndResetFlowControlData());
        m_flowControlBytes.rewind();

        while (bytes < Integer.BYTES && ++tries < 1000) {
            // Send flow control bytes over incoming channel as this is unused
            bytes += m_incomingChannel.write(m_flowControlBytes);
        }

        if (tries == 1000) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not send flow control data!");
            // #endif /* LOGGER >= ERROR */
        }
    }

    @Override
    public boolean isOpen() {
        return m_incomingChannel != null && m_incomingChannel.isOpen();
    }

    @Override
    public void returnProcessedBuffer(final ByteBuffer p_buffer) {
        m_bufferPool.returnBuffer(p_buffer);
    }
}

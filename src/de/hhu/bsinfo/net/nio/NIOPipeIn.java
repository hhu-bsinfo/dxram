package de.hhu.bsinfo.net.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.AbstractPipeIn;
import de.hhu.bsinfo.net.core.DataReceiver;
import de.hhu.bsinfo.net.core.MessageCreator;
import de.hhu.bsinfo.net.core.MessageDirectory;
import de.hhu.bsinfo.net.core.NetworkException;
import de.hhu.bsinfo.net.core.RequestMap;

/**
 * Created by nothaas on 6/9/17.
 */
public class NIOPipeIn extends AbstractPipeIn {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractPipeIn.class.getSimpleName());

    private SocketChannel m_incomingChannel;
    private final NIOBufferPool m_bufferPool;
    private final MessageCreator m_messageCreator;
    private final ByteBuffer m_flowControlBytes;

    private final NIOConnection m_parentConnection;

    private NIOMessageImporterCollection m_importers;

    private ByteBuffer m_currentBuffer;

    public NIOPipeIn(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl,
            final MessageDirectory p_messageDirectory, final RequestMap p_requestMap, final DataReceiver p_dataReceiver, final NIOBufferPool p_bufferPool,
            final MessageCreator p_messageCreator, final NIOConnection p_parentConnection) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_messageDirectory, p_requestMap, p_dataReceiver, false);

        m_incomingChannel = null;
        m_bufferPool = p_bufferPool;
        m_messageCreator = p_messageCreator;
        m_flowControlBytes = ByteBuffer.allocateDirect(Integer.BYTES);

        m_parentConnection = p_parentConnection;

        m_importers = new NIOMessageImporterCollection();
    }

    public SocketChannel getChannel() {
        return m_incomingChannel;
    }

    public void bindIncomingChannel(final SocketChannel p_channel) {
        m_incomingChannel = p_channel;
    }

    public void processBuffer(final ByteBuffer p_buffer) throws NetworkException {
        m_currentBuffer = p_buffer;

        processBuffer(m_currentBuffer.remaining());
    }

    public void returnProcessedBuffer(final ByteBuffer p_buffer) {
        m_bufferPool.returnBuffer(p_buffer);
    }

    @Override
    public boolean isOpen() {
        return m_incomingChannel != null && m_incomingChannel.isOpen();
    }

    /**
     * Reads from the given connection
     * m_buffer needs to be synchronized externally
     *
     * @return whether reading from channel was successful or not (connection is closed then)
     * @throws IOException
     *         if the data could not be read
     */
    boolean read() throws IOException {
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
                LOGGER.trace("Posting receive buffer (limit %d) to connection 0x%X", buffer.limit(), getDestinationNodeID());
                // #endif /* LOGGER >= TRACE */

                // Avoid congestion by not allowing more than m_numberOfBuffers buffers to be cached for reading
                while (!m_messageCreator.pushJob(m_parentConnection, buffer, -1, -1)) {
                    // #if LOGGER == TRACE
                    LOGGER.trace("Network-Selector: Job queue is full!");
                    // #endif /* LOGGER == TRACE */

                    System.out.println("Network-Selector: Job queue is full!");

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
    void writeFlowControlBytes() throws IOException {
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
    protected AbstractMessageImporter getImporter(final boolean p_overflow) {
        NIOMessageImporter importer;
        importer = (NIOMessageImporter) m_importers.getImporter(m_currentBuffer, p_overflow);
        return importer;
    }

    @Override
    protected void returnImporter(final AbstractMessageImporter p_importer, final boolean p_finished) {
        // write back updated position from importer to ByteBuffer
        m_currentBuffer.position(m_currentBuffer.position() + p_importer.getNumberOfReadBytes());
        m_importers.returnImporter(p_importer, true);
    }
}

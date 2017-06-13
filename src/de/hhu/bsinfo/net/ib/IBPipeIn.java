package de.hhu.bsinfo.net.ib;

import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.AbstractPipeIn;
import de.hhu.bsinfo.net.core.DataReceiver;
import de.hhu.bsinfo.net.core.MessageCreator;
import de.hhu.bsinfo.net.core.MessageDirectory;
import de.hhu.bsinfo.net.core.RequestMap;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBPipeIn extends AbstractPipeIn {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBPipeIn.class.getSimpleName());

    private final IBBufferPool m_bufferPool;
    private final MessageCreator m_messageCreator;

    private final IBConnection m_parentConnection;

    public IBPipeIn(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl,
            final MessageDirectory p_messageDirectory, final RequestMap p_requestMap, final DataReceiver p_dataReceiver, final IBBufferPool p_bufferPool,
            final MessageCreator p_messageCreator, final IBConnection p_parentConnection) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_messageDirectory, p_requestMap, p_dataReceiver);

        m_bufferPool = p_bufferPool;
        m_messageCreator = p_messageCreator;
        m_parentConnection = p_parentConnection;
    }

    public void handleFlowControlData(final int p_confirmedBytes) {
        getFlowControl().handleFlowControlData(p_confirmedBytes);
    }

    public void processReceivedBuffer(final ByteBuffer p_buffer, final int p_length) {
        p_buffer.limit(p_length);
        p_buffer.rewind();

        // #if LOGGER >= TRACE
        LOGGER.trace("Posting receive buffer (limit %d) to connection 0x%X", p_buffer.limit(), getDestinationNodeId());
        // #endif /* LOGGER >= TRACE */

        // Avoid congestion by not allowing more than m_numberOfBuffers buffers to be cached for reading
        while (!m_messageCreator.pushJob(m_parentConnection, p_buffer)) {
            // #if LOGGER == TRACE
            LOGGER.trace("Network-Selector: Job queue is full!");
            // #endif /* LOGGER == TRACE */

            Thread.yield();
        }
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void returnProcessedBuffer(final ByteBuffer p_buffer) {
        m_bufferPool.returnBuffer(p_buffer);
    }
}

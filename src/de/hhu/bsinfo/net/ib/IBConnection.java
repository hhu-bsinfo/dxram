package de.hhu.bsinfo.net.ib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.MessageHandlers;
import de.hhu.bsinfo.net.core.AbstractConnection;
import de.hhu.bsinfo.net.core.AbstractExporterPool;
import de.hhu.bsinfo.net.core.MessageDirectory;
import de.hhu.bsinfo.net.core.RequestMap;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBConnection extends AbstractConnection<IBPipeIn, IBPipeOut> {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBConnection.class.getSimpleName());

    private final IBWriteInterestManager m_interestManager;

    IBConnection(final short p_ownNodeId, final short p_destinationNodeId, final int p_outBufferSize, final int p_flowControlWindowSize,
            final float p_flowControlWindowThreshold, final MessageDirectory p_messageDirectory, final RequestMap p_requestMap,
            final AbstractExporterPool p_exporterPool, final MessageHandlers p_messageHandlers, final IBWriteInterestManager p_writeInterestManager) {
        super(p_ownNodeId);

        long sendBufferAddr = JNIIbdxnet.getSendBufferAddress(p_destinationNodeId);
        if (sendBufferAddr == -1) {
            // might happen on disconnect or if connection is not established in the ibnet subsystem
            throw new IllegalStateException();
        }

        IBFlowControl flowControl = new IBFlowControl(p_destinationNodeId, p_flowControlWindowSize, p_flowControlWindowThreshold, p_writeInterestManager);
        IBOutgoingRingBuffer outgoingBuffer = new IBOutgoingRingBuffer(sendBufferAddr, p_outBufferSize, p_exporterPool);
        IBPipeIn pipeIn = new IBPipeIn(p_ownNodeId, p_destinationNodeId, flowControl, p_messageDirectory, p_requestMap, p_messageHandlers);
        IBPipeOut pipeOut = new IBPipeOut(p_ownNodeId, p_destinationNodeId, flowControl, outgoingBuffer, p_writeInterestManager);

        setPipes(pipeIn, pipeOut);

        m_interestManager = p_writeInterestManager;
    }

    @Override
    public void close(final boolean p_force) {
        setClosingTimestamp();

        if (!p_force) {
            if (!getPipeOut().isOutgoingQueueEmpty()) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Waiting for all scheduled messages to be sent over to be closed connection!");
                // #endif /* LOGGER >= DEBUG */
                long start = System.currentTimeMillis();
                while (!getPipeOut().isOutgoingQueueEmpty()) {
                    Thread.yield();

                    if (System.currentTimeMillis() - start > 2000) {
                        // #if LOGGER >= ERROR
                        LOGGER.debug("Waiting for all scheduled messages to be sent over aborted, timeout");
                        // #endif /* LOGGER >= ERROR */
                        break;
                    }
                }
            }
        }

        // flush any remaining interests
        m_interestManager.nodeDisconnected(getOwnNodeID());
    }

    @Override
    public void wakeup() {
        // nothing to do here
    }
}

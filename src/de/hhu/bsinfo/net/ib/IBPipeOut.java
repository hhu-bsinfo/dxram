package de.hhu.bsinfo.net.ib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.AbstractOutgoingRingBuffer;
import de.hhu.bsinfo.net.core.AbstractPipeOut;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBPipeOut extends AbstractPipeOut {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBPipeOut.class.getSimpleName());

    private final IBWriteInterestManager m_writeInterestManager;

    public IBPipeOut(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl,
            final AbstractOutgoingRingBuffer p_outgoingBuffer, final IBWriteInterestManager p_writeInterestManager) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_outgoingBuffer);
        m_writeInterestManager = p_writeInterestManager;
    }

    long getNextBuffer() {
        return ((IBOutgoingRingBuffer) getOutgoingQueue()).popFront();
    }

    int getFlowControlToWrite() {
        return getFlowControl().getAndResetFlowControlData();
    }

    @Override
    protected boolean isOpen() {
        return true;
    }

    @Override
    protected void bufferPosted(final int p_size) {
        m_writeInterestManager.pushBackDataInterest(getDestinationNodeID());
    }
}

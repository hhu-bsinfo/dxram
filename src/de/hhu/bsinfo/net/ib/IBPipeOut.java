package de.hhu.bsinfo.net.ib;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.AbstractPipeOut;
import de.hhu.bsinfo.net.core.OutgoingRingBuffer;

/**
 * Pipe out implementation (current node -> remote write) for IB
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.06.2017
 */
class IBPipeOut extends AbstractPipeOut {
    private final IBWriteInterestManager m_writeInterestManager;

    /**
     * Constructor
     *
     * @param p_ownNodeId
     *         Node id of the current node
     * @param p_destinationNodeId
     *         Node id of the remote node connected to
     * @param p_flowControl
     *         Flow control instance
     * @param p_outgoingBuffer
     *         Outgoing (ring) buffer instance
     * @param p_writeInterestManager
     *         Write interest manager instance
     */
    IBPipeOut(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl, final OutgoingRingBuffer p_outgoingBuffer,
            final IBWriteInterestManager p_writeInterestManager) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_outgoingBuffer);
        m_writeInterestManager = p_writeInterestManager;
    }

    /**
     * Get the next available buffer/chunk of data to write to the connection
     *
     * @return Long holding the current relative position of the front pointer
     * (lower 32-bit) and the relative position of the back pointer
     * (higher 32-bit) of the ring buffer
     */
    long getNextBuffer() {
        return ((IBOutgoingRingBuffer) getOutgoingQueue()).popFront();
    }

    /**
     * Get flow control data to write to the connection
     *
     * @return Flow control bytes
     */
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

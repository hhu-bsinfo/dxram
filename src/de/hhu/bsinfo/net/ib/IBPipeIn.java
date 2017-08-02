package de.hhu.bsinfo.net.ib;

import de.hhu.bsinfo.net.MessageHandlers;
import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.AbstractPipeIn;
import de.hhu.bsinfo.net.core.MessageDirectory;
import de.hhu.bsinfo.net.core.RequestMap;

/**
 * Pipe in implementation (remote -> current node write) for IB
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.06.2017
 */
class IBPipeIn extends AbstractPipeIn {

    /**
     * Constructor
     *
     * @param p_ownNodeId
     *         Node id of the current node
     * @param p_destinationNodeId
     *         Node id of the destination this pipe is connected to
     * @param p_flowControl
     *         Flow control instance
     * @param p_messageDirectory
     *         Message directory instance
     * @param p_requestMap
     *         Request map instance
     * @param p_messageHandlers
     *         Message handlers instance
     */
    IBPipeIn(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl, final MessageDirectory p_messageDirectory,
            final RequestMap p_requestMap, final MessageHandlers p_messageHandlers) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_messageDirectory, p_requestMap, p_messageHandlers);
    }

    /**
     * Handle incoming flow control data from the remote connection
     *
     * @param p_confirmedBytes
     *         Bytes confirmed/processed and sent by the remote to use to handle
     */
    void handleFlowControlData(final int p_confirmedBytes) {
        getFlowControl().handleFlowControlData(p_confirmedBytes);
    }

    @Override
    public void returnProcessedBuffer(final Object p_obj, final long p_bufferHandle) {
        // p_obj unused
        JNIIbdxnet.returnRecvBuffer(p_bufferHandle);
    }

    @Override
    public boolean isOpen() {
        return true;
    }
}

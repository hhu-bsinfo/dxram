package de.hhu.bsinfo.net.ib;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.AbstractPipeIn;
import de.hhu.bsinfo.net.core.DataReceiver;
import de.hhu.bsinfo.net.core.MessageDirectory;
import de.hhu.bsinfo.net.core.RequestMap;

/**
 * Created by nothaas on 6/13/17.
 */
class IBPipeIn extends AbstractPipeIn {

    IBPipeIn(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl, final MessageDirectory p_messageDirectory,
            final RequestMap p_requestMap, final DataReceiver p_dataReceiver) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_messageDirectory, p_requestMap, p_dataReceiver);
    }

    void handleFlowControlData(final int p_confirmedBytes) {
        getFlowControl().handleFlowControlData(p_confirmedBytes);
    }

    @Override
    public void returnProcessedBuffer(final Object p_unused, final long p_bufferHandle) {
        JNIIbdxnet.returnRecvBuffer(p_bufferHandle);
    }

    @Override
    public boolean isOpen() {
        return true;
    }
}

package de.hhu.bsinfo.net.ib;

import de.hhu.bsinfo.net.core.AbstractConnection;
import de.hhu.bsinfo.net.core.DataReceiver;
import de.hhu.bsinfo.net.core.MessageDirectory;
import de.hhu.bsinfo.net.core.RequestMap;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBConnection extends AbstractConnection<IBPipeIn, IBPipeOut> {
    IBConnection(final short p_ownNodeId, final short p_destinationNodeId, final int p_outBufferSize, final int p_flowControlWindowSize,
            final MessageDirectory p_messageDirectory, final RequestMap p_requestMap, final DataReceiver p_dataReceiver,
            final IBWriteInterestManager p_writeInterestManager) {
        super(p_ownNodeId);

        long sendBufferAddr = JNIIbdxnet.getSendBufferAddress(p_destinationNodeId);
        if (sendBufferAddr == -1) {
            // TODO happens on disconnect or if connection is not established in the ibnet subsystem
            throw new IllegalStateException();
        }

        IBFlowControl flowControl = new IBFlowControl(p_destinationNodeId, p_flowControlWindowSize, p_writeInterestManager);
        IBOutgoingRingBuffer outgoingBuffer = new IBOutgoingRingBuffer(sendBufferAddr, p_outBufferSize);
        IBPipeIn pipeIn = new IBPipeIn(p_ownNodeId, p_destinationNodeId, flowControl, p_messageDirectory, p_requestMap, p_dataReceiver);
        IBPipeOut pipeOut = new IBPipeOut(p_ownNodeId, p_destinationNodeId, flowControl, outgoingBuffer, p_writeInterestManager);

        setPipes(pipeIn, pipeOut);
    }

    @Override
    public void close(final boolean p_force) {
        setClosingTimestamp();

        // TODO writeInterestManager needs a force/non force way to flush?

        // TODO needs hook to IBNet
    }

    @Override
    public void wakeup() {
        // nothing to do here
    }
}

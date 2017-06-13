package de.hhu.bsinfo.net.ib;

import de.hhu.bsinfo.net.core.AbstractConnection;
import de.hhu.bsinfo.net.core.DataReceiver;
import de.hhu.bsinfo.net.core.MessageCreator;
import de.hhu.bsinfo.net.core.MessageDirectory;
import de.hhu.bsinfo.net.core.RequestMap;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBConnection extends AbstractConnection<IBPipeIn, IBPipeOut> {
    IBConnection(final short p_ownNodeId, final short p_destinationNodeId, final int p_bufferSize, final int p_flowControlWindowSize,
            final MessageCreator p_messageCreator, final MessageDirectory p_messageDirectory, final RequestMap p_requestMap, final DataReceiver p_dataReceiver,
            final IBBufferPool p_bufferPool) {
        super(p_ownNodeId, p_messageCreator);

        IBFlowControl flowControl = new IBFlowControl(p_destinationNodeId, p_flowControlWindowSize);
        IBPipeIn pipeIn =
                new IBPipeIn(p_ownNodeId, p_destinationNodeId, flowControl, p_messageDirectory, p_requestMap, p_dataReceiver, p_bufferPool, p_messageCreator,
                        this);
        IBPipeOut pipeOut = new IBPipeOut(p_ownNodeId, p_destinationNodeId, p_bufferSize, flowControl);

        setPipes(pipeIn, pipeOut);
    }

    @Override
    public void close(final boolean p_force) {
        setClosingTimestamp();

        // TODO needs hook to IBNet
    }

    @Override
    public void wakeup() {
        // nothing to do here
    }
}

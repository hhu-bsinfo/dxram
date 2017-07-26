package de.hhu.bsinfo.net.nio;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.NetworkException;

/**
 * Created by nothaas on 6/12/17.
 */
public class NIOFlowControl extends AbstractFlowControl {

    private final NIOSelector m_nioSelector;
    private final ChangeOperationsRequest m_flowControlOperation;

    NIOFlowControl(final short p_destinationNodeId, final int p_flowControlWindowSize, final NIOSelector p_nioSelector, final NIOConnection p_connection) {
        super(p_destinationNodeId, p_flowControlWindowSize);

        m_nioSelector = p_nioSelector;
        m_flowControlOperation = new ChangeOperationsRequest(p_connection, NIOSelector.FLOW_CONTROL);
    }

    @Override
    public void flowControlWrite() throws NetworkException {
        m_nioSelector.changeOperationInterestAsync(m_flowControlOperation);
    }
}

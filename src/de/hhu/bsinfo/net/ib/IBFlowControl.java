package de.hhu.bsinfo.net.ib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.NetworkException;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBFlowControl extends AbstractFlowControl {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBFlowControl.class.getSimpleName());

    protected IBFlowControl(final short p_destinationNodeId, final int p_flowControlWindowSize) {
        super(p_destinationNodeId, p_flowControlWindowSize);
    }

    @Override
    public void flowControlWrite() throws NetworkException {
        if (!JNIIBnet.postFlowControlData(getDestinationNodeId(), getAndResetFlowControlData())) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not send flow control data to node 0x%X!", getDestinationNodeId());
            // #endif /* LOGGER >= ERROR */
        }
    }
}

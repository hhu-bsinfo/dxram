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

    private final IBWriteInterestManager m_writeInterestManager;

    protected IBFlowControl(final short p_destinationNodeId, final int p_flowControlWindowSize, final IBWriteInterestManager p_writeInterestManager) {
        super(p_destinationNodeId, p_flowControlWindowSize);
        m_writeInterestManager = p_writeInterestManager;
    }

    @Override
    public void flowControlWrite() throws NetworkException {
        m_writeInterestManager.pushBackInterest(getDestinationNodeId());
    }
}

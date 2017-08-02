package de.hhu.bsinfo.net.ib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.NetworkException;

/**
 * Flow control implementation for IB
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.06.2017
 */
class IBFlowControl extends AbstractFlowControl {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBFlowControl.class.getSimpleName());

    private final IBWriteInterestManager m_writeInterestManager;

    /**
     * Constructor
     *
     * @param p_destinationNodeId
     *         Node id of the destination connected to for this flow control
     * @param p_flowControlWindowSize
     *         Window size of the flow control (i.e. when to send a flow control msg)
     * @param p_flowControlWindowThreshold
     *         Threshold to exceed before sending a flow control msg
     * @param p_writeInterestManager
     *         Write interest manager instance
     */
    IBFlowControl(final short p_destinationNodeId, final int p_flowControlWindowSize, final float p_flowControlWindowThreshold,
            final IBWriteInterestManager p_writeInterestManager) {
        super(p_destinationNodeId, p_flowControlWindowSize, p_flowControlWindowThreshold);
        m_writeInterestManager = p_writeInterestManager;
    }

    @Override
    public void flowControlWrite() throws NetworkException {
        m_writeInterestManager.pushBackFcInterest(getDestinationNodeId());
    }
}

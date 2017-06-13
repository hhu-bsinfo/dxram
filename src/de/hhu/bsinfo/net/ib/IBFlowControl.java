package de.hhu.bsinfo.net.ib;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.NetworkException;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBFlowControl extends AbstractFlowControl {
    protected IBFlowControl(short p_destinationNodeId, int p_flowControlWindowSize) {
        super(p_destinationNodeId, p_flowControlWindowSize);
    }

    @Override
    public void flowControlWrite() throws NetworkException {

    }
}

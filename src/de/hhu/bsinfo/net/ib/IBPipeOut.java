package de.hhu.bsinfo.net.ib;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.AbstractPipeOut;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBPipeOut extends AbstractPipeOut {
    public IBPipeOut(short p_ownNodeId, short p_destinationNodeId, int p_bufferSize, AbstractFlowControl p_flowControl) {
        super(p_ownNodeId, p_destinationNodeId, p_bufferSize, p_flowControl);
    }

    @Override
    protected boolean isOpen() {
        return false;
    }

    @Override
    protected void bufferPosted() {

    }
}

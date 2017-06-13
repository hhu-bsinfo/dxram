package de.hhu.bsinfo.ethnet.ib;

import de.hhu.bsinfo.ethnet.core.AbstractConnection;
import de.hhu.bsinfo.ethnet.core.MessageCreator;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBConnection extends AbstractConnection<IBPipeIn, IBPipeOut> {
    protected IBConnection(short p_ownNodeId, MessageCreator p_messageCreator) {
        super(p_ownNodeId, p_messageCreator);
    }

    @Override
    public void close(boolean p_force) {

    }

    @Override
    public void wakeup() {

    }
}

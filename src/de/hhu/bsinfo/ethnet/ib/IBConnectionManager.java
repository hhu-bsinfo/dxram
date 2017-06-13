package de.hhu.bsinfo.ethnet.ib;

import de.hhu.bsinfo.ethnet.core.AbstractConnection;
import de.hhu.bsinfo.ethnet.core.AbstractConnectionManager;
import de.hhu.bsinfo.ethnet.core.NetworkException;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBConnectionManager extends AbstractConnectionManager {

    protected IBConnectionManager(int p_maxConnections) {
        super(p_maxConnections);
    }

    @Override
    protected AbstractConnection createConnection(short p_destination, AbstractConnection p_existingConnection) throws NetworkException {
        return null;
    }

    @Override
    protected void closeConnection(AbstractConnection p_connection, boolean p_removeConnection) {

    }
}

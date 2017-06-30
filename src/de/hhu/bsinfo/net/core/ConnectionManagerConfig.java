package de.hhu.bsinfo.net.core;

import de.hhu.bsinfo.net.NetworkSystemConfig;

public abstract class ConnectionManagerConfig extends NetworkSystemConfig {
    public abstract int getMaxConnections();

    public abstract int getFlowControlWindow();

    public abstract int getConnectionTimeout();
}

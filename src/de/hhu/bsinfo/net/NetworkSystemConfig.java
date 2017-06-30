package de.hhu.bsinfo.net;

public abstract class NetworkSystemConfig {
    public abstract short getOwnNodeId();

    public abstract int getNumMessageHandlerThreads();

    public abstract int getRequestTimeOut();

    public abstract int getBufferSize();

    public abstract int getRequestMapSize();
}

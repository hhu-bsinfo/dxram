package de.hhu.bsinfo.dxnet;

public abstract class NetworkSystemConfig {
    public abstract short getOwnNodeId();

    public abstract int getNumMessageHandlerThreads();

    public abstract int getRequestTimeOut();

    public abstract int getBufferSize();

    public abstract int getRequestMapSize();

    public abstract boolean getExporterPoolType();
}

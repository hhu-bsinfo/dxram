package de.hhu.bsinfo.net.ib;

import com.google.auto.value.AutoValue;

import de.hhu.bsinfo.net.core.ConnectionManagerConfig;

@AutoValue
public abstract class IBConnectionManagerConfig extends ConnectionManagerConfig {
    public static Builder builder() {
        return new AutoValue_IBConnectionManagerConfig.Builder();
    }

    public abstract int getMaxRecvReqs();

    public abstract int getFlowControlMaxRecvReqs();

    public abstract int getSendThreads();

    public abstract int getRecvThreads();

    public abstract boolean getEnableSignalHandler();

    public abstract boolean getEnableDebugThread();

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setOwnNodeId(final short p_val);

        public abstract Builder setNumMessageHandlerThreads(final int p_val);

        public abstract Builder setRequestTimeOut(final int p_val);

        public abstract Builder setBufferSize(final int p_val);

        public abstract Builder setRequestMapSize(final int p_val);

        public abstract Builder setMaxConnections(final int p_val);

        public abstract Builder setFlowControlWindow(final int p_val);

        public abstract Builder setConnectionTimeout(final int p_val);

        public abstract Builder setMaxRecvReqs(final int p_val);

        public abstract Builder setFlowControlMaxRecvReqs(final int p_val);

        public abstract Builder setSendThreads(final int p_val);

        public abstract Builder setRecvThreads(final int p_val);

        public abstract Builder setEnableSignalHandler(final boolean p_val);

        public abstract Builder setEnableDebugThread(final boolean p_val);

        public abstract IBConnectionManagerConfig build();
    }
}

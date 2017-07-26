package de.hhu.bsinfo.net.nio;

import com.google.auto.value.AutoValue;

import de.hhu.bsinfo.net.core.ConnectionManagerConfig;

@AutoValue
public abstract class NIOConnectionManagerConfig extends ConnectionManagerConfig {
    public static Builder builder() {
        return new AutoValue_NIOConnectionManagerConfig.Builder();
    }

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

        public abstract Builder setExporterPoolType(final boolean p_val);

        public abstract NIOConnectionManagerConfig build();
    }
}

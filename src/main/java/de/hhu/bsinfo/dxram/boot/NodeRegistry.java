/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.boot;


import com.google.common.collect.Sets;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.*;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonRootName;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class NodeRegistry implements ServiceCacheListener {

    private final static Logger log = LoggerFactory.getLogger(NodeRegistry.class);

    private final CuratorFramework m_curator;
    private final Listener m_listener;

    private final JsonInstanceSerializer<NodeDetails> serializer = new JsonInstanceSerializer<>(NodeDetails.class);

    private final AtomicBoolean m_isRunning = new AtomicBoolean(false);

    private ServiceDiscovery<NodeDetails> m_serviceDiscovery;
    private ServiceInstance<NodeDetails> m_instance;
    private ServiceCache<NodeDetails> m_cache;

    private final static String BASE_PATH = "/discovery";
    private final static String SERVICE_NAME = "dxram";
    private final static String THREAD_NAME = "discovery";

    private final ConcurrentHashMap<String, NodeDetails> m_serviceMap = new ConcurrentHashMap<>();

    public interface Listener {
        void onPeerJoined(final NodeDetails p_nodeDetails);
        void onPeerLeft(final NodeDetails p_nodeDetails);
        void onSuperpeerJoined(final NodeDetails p_nodeDetails);
        void onSuperpeerLeft(final NodeDetails p_nodeDetails);
    }

    private enum ListenerEvent {
        PEER_JOINED, PEER_LEFT, SUPERPEER_JOINED, SUPERPEER_LEFT
    }

    NodeRegistry(@NotNull final CuratorFramework p_curator, @Nullable final Listener p_listener) {
        m_curator = p_curator;
        m_listener = p_listener;
    }

    public void start(final NodeDetails p_details) throws Exception {
        if (m_isRunning.get()) {
            log.warn("Registry is already running");
            return;
        }

        m_instance = p_details.toServiceInstance();

        m_serviceDiscovery = ServiceDiscoveryBuilder.builder(NodeDetails.class)
                .client(m_curator)
                .basePath(BASE_PATH)
                .serializer(serializer)
                .thisInstance(m_instance)
                .build();

        m_cache = m_serviceDiscovery.serviceCacheBuilder()
                .name(SERVICE_NAME)
                .threadFactory(runnable -> new Thread(runnable, THREAD_NAME))
                .build();

        m_cache.addListener(this);

        m_serviceDiscovery.start();
        m_cache.start();
    }

    public void close() {
        try {
            m_cache.close();
            m_serviceDiscovery.close();
        } catch (IOException p_e) {
            log.error("Closing node registry failed", p_e);
        } finally {
            CloseableUtils.closeQuietly(m_cache);
            CloseableUtils.closeQuietly(m_serviceDiscovery);
            m_isRunning.set(false);
        }
    }

    public void updateNodeDetails(NodeDetails p_details) {
        ServiceInstance<NodeDetails> serviceInstance = p_details.toServiceInstance();

        if (serviceInstance.getId().equals(m_instance.getId())) {
            m_instance = serviceInstance;
        }

        try {
            m_serviceDiscovery.updateService(serviceInstance);
        } catch (Exception p_e) {
            log.warn("Updating registry entry failed", p_e);
        }
    }

    private void notifyListener(final ListenerEvent p_event, final NodeDetails p_nodeDetails) {
        if (m_listener == null) {
            return;
        }

        switch (p_event) {
            case PEER_JOINED:
                m_listener.onPeerJoined(p_nodeDetails);
                break;
            case SUPERPEER_JOINED:
                m_listener.onSuperpeerJoined(p_nodeDetails);
                break;
            case PEER_LEFT:
                m_listener.onPeerLeft(p_nodeDetails);
                break;
            case SUPERPEER_LEFT:
                m_listener.onSuperpeerLeft(p_nodeDetails);
                break;
        }
    }

    @Override
    public void cacheChanged() {
        Set<NodeDetails> remoteDetails = m_cache.getInstances().stream()
                .map(ServiceInstance::getPayload)
                .collect(Collectors.toSet());

        Set<NodeDetails> localDetails = new HashSet<>(m_serviceMap.values());

        Sets.SetView<NodeDetails> joinedNodes = Sets.difference(remoteDetails, localDetails);
        Sets.SetView<NodeDetails> leftNodes = Sets.difference(localDetails, remoteDetails);

        for (NodeDetails details : joinedNodes) {
            m_serviceMap.put(NodeID.toHexStringShort(details.getId()), details);

            if (details.getRole().equals(NodeRole.SUPERPEER)) {
                notifyListener(ListenerEvent.SUPERPEER_JOINED, details);
            } else {
                notifyListener(ListenerEvent.PEER_JOINED, details);
            }
        }

        for (NodeDetails details : leftNodes) {
            m_serviceMap.remove(NodeID.toHexString(details.getId()));

            if (details.getRole().equals(NodeRole.SUPERPEER)) {
                notifyListener(ListenerEvent.SUPERPEER_LEFT, details);
            } else {
                notifyListener(ListenerEvent.PEER_LEFT, details);
            }
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        log.info("Curator connection state changed to {}", newState.isConnected() ? "CONNECTED" : "DISCONNECTED");
    }

    @JsonRootName("node")
    public static final class NodeDetails {

        /**
         * The node's id.
         */
        private final short m_id;

        /**
         * The node's ip address.
         */
        private final String m_ip;

        /**
         * The node's port.
         */
        private final int m_port;

        /**
         * The node's rack id.
         */
        private final short m_rack;

        /**
         * The node's switch id.
         */
        private final short m_switch;

        /**
         * The node's role.
         */
        private final NodeRole m_role;

        /**
         * Indicates whether this node is currently available.
         */
        private final boolean m_online;

        /**
         * Indicates whether this node is able to store backups.
         */
        private final boolean m_availableForBackup;

        /**
         * The node's capabilities.
         */
        private final int m_capabilities;

        @JsonCreator
        private NodeDetails(@JsonProperty("id") short p_id,
                            @JsonProperty("ip") String p_ip,
                            @JsonProperty("port") int p_port,
                            @JsonProperty("rack") short p_rack,
                            @JsonProperty("switch") short p_switch,
                            @JsonProperty("role") NodeRole p_role,
                            @JsonProperty("online") boolean p_online,
                            @JsonProperty("availableForBackup") boolean p_availableForBackup,
                            @JsonProperty("capabilities") int p_capabilities) {
            m_id = p_id;
            m_ip = p_ip;
            m_port = p_port;
            m_rack = p_rack;
            m_switch = p_switch;
            m_role = p_role;
            m_online = p_online;
            m_availableForBackup = p_availableForBackup;
            m_capabilities = p_capabilities;
        }

        public static Builder builder(final short p_id, final String p_ip, final int p_port) {
            return new Builder(p_id, p_ip, p_port);
        }

        public short getId() {
            return m_id;
        }

        public String getIp() {
            return m_ip;
        }

        public int getPort() {
            return m_port;
        }

        public short getRack() {
            return m_rack;
        }

        public short getSwitch() {
            return m_switch;
        }

        public NodeRole getRole() {
            return m_role;
        }

        public boolean isOnline() {
            return m_online;
        }

        public boolean isAvailableForBackup() {
            return m_availableForBackup;
        }

        public int getCapabilities() {
            return m_capabilities;
        }

        @Override
        public String toString() {
            return String.format("[%c|%04X](%s:%d)", m_role.getAcronym(), m_id, m_ip, m_port);
        }

        ServiceInstance<NodeDetails> toServiceInstance() {
            return new ServiceInstance<>(SERVICE_NAME, NodeID.toHexStringShort(m_id), m_ip,
                    m_port, 0, this, System.currentTimeMillis(), ServiceType.DYNAMIC, null, true);
        }

        public static class Builder {

            private final short m_id;
            private final String m_ip;
            private final int m_port;

            private short m_rack;
            private short m_switch;
            private NodeRole m_role;
            private boolean m_online;
            private boolean m_availableForBackup;
            private int m_capabilities;

            public Builder(short p_id, String p_ip, int p_port) {
                m_id = p_id;
                m_ip = p_ip;
                m_port = p_port;
            }

            public Builder withRack(short p_rack) {
                m_rack = p_rack;
                return this;
            }

            public Builder withSwitch(short p_switch) {
                m_switch = p_switch;
                return this;
            }

            public Builder withRole(NodeRole p_role) {
                m_role = p_role;
                return this;
            }

            public Builder withOnline(boolean p_online) {
                m_online = p_online;
                return this;
            }

            public Builder withAvailableForBackup(boolean p_availableForBackup) {
                m_availableForBackup = p_availableForBackup;
                return this;
            }

            public Builder withCapabilities(int p_capabilities) {
                m_capabilities = p_capabilities;
                return this;
            }

            public NodeDetails build() {
                return new NodeDetails(m_id, m_ip, m_port, m_rack, m_switch, m_role, m_online,
                        m_availableForBackup, m_capabilities);
            }
        }
    }
}

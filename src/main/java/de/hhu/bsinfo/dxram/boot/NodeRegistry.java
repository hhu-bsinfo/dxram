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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonRootName;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

@SuppressWarnings("WeakerAccess")
public class NodeRegistry implements ServiceCacheListener {
    private static final Logger LOGGER = LogManager.getFormatterLogger(NodeRegistry.class);
    private static final String BASE_PATH = "/dxram";
    private static final String SERVICE_NAME = "nodes";
    private static final String THREAD_NAME = "discovery";
    private static final String DISCOVERY_PATH = String.format("%s/%s", BASE_PATH, SERVICE_NAME);
    private final CuratorFramework m_curator;
    private final List<Listener> m_listener = new ArrayList<>();
    private final JsonInstanceSerializer<NodeDetails> m_serializer = new JsonInstanceSerializer<>(NodeDetails.class);
    private final AtomicBoolean m_isRunning = new AtomicBoolean(false);
    private final ConcurrentHashMap<Short, NodeDetails> m_serviceMap = new ConcurrentHashMap<>();
    private final ExecutorService m_executor = Executors.newSingleThreadExecutor(
            runnable -> new Thread(runnable, THREAD_NAME));
    private ServiceDiscovery<NodeDetails> m_serviceDiscovery;
    private ServiceInstance<NodeDetails> m_instance;
    private ServiceCache<NodeDetails> m_cache;

    NodeRegistry(final @NotNull CuratorFramework p_curator) {
        m_curator = p_curator;
    }

    /**
     * Add a listener
     *
     * @param p_listener
     *         Listener
     */
    public void registerListener(final Listener p_listener) {
        m_listener.add(p_listener);
    }

    /**
     * Starts the node registry and service discovery.
     *
     * @param p_details
     *         This node's details.
     * @throws Exception
     *         On ZooKeeper errors.
     */
    public void start(final NodeDetails p_details) throws Exception {
        if (m_isRunning.get()) {
            LOGGER.warn("Registry is already running");
            return;
        }

        m_serviceMap.put(p_details.getId(), p_details);

        m_instance = p_details.toServiceInstance();

        m_serviceDiscovery = ServiceDiscoveryBuilder.builder(NodeDetails.class)
                .client(m_curator)
                .basePath(BASE_PATH)
                .serializer(m_serializer)
                .thisInstance(m_instance)
                .build();

        m_cache = m_serviceDiscovery.serviceCacheBuilder()
                .name(SERVICE_NAME)
                .executorService(m_executor)
                .build();

        m_cache.addListener(this);

        // Start cache first so that it gets updated immediately
        m_cache.start();
        m_serviceDiscovery.start();
    }

    /**
     * Closes the node registry and stops service discovery.
     */
    public void close() {
        CloseableUtils.closeQuietly(m_cache);
        CloseableUtils.closeQuietly(m_serviceDiscovery);
        m_isRunning.set(false);
    }

    /**
     * Updates a specific node's details within ZooKeeper.
     *
     * @param p_details
     *         The node's details.
     */
    public void updateNodeDetails(NodeDetails p_details) {
        ServiceInstance<NodeDetails> serviceInstance = p_details.toServiceInstance();

        if (serviceInstance.getId().equals(m_instance.getId())) {
            m_instance = serviceInstance;
        }

        try {
            m_serviceDiscovery.updateService(serviceInstance);
        } catch (Exception e) {
            LOGGER.warn("Updating registry entry failed", e);
        }
    }

    /**
     * Informs the registered listener about changes.
     *
     * @param p_event
     *         The event.
     * @param p_nodeDetails
     *         The node's details.
     */
    private void notifyListener(final ListenerEvent p_event, final NodeDetails p_nodeDetails) {
        //        runOnMainThread(() -> {
        for (Listener listener : m_listener) {
            switch (p_event) {
                case PEER_JOINED:
                    LOGGER.info("Node %s with capabilities %s joined the network",
                            p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));

                    listener.onPeerJoined(p_nodeDetails);
                    break;

                case SUPERPEER_JOINED:
                    LOGGER.info("Node %s with capabilities %s joined the network",
                            p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));

                    listener.onSuperpeerJoined(p_nodeDetails);
                    break;

                case PEER_LEFT:
                    LOGGER.info("Node %s with capabilities %s left the network",
                            p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));

                    listener.onPeerLeft(p_nodeDetails);
                    break;

                case SUPERPEER_LEFT:
                    LOGGER.info("Node %s with capabilities %s left the network",
                            p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));

                    listener.onSuperpeerLeft(p_nodeDetails);
                    break;

                case NODE_UPDATED:
                    LOGGER.info("Updated node %s with capabilities %s",
                            p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));

                    listener.onNodeUpdated(p_nodeDetails);
                    break;
            }
        }
        //        });
    }

    /**
     * Returns the specified node's details.
     *
     * @param p_nodeId
     *         The node's id.
     * @return The specified node's details.
     */
    public @Nullable NodeDetails getDetails(final short p_nodeId) {
        NodeDetails details = m_serviceMap.get(p_nodeId);

        return details != null ? details : getRemoteDetails(p_nodeId);
    }

    /**
     * Retrieves the specified node's details from ZooKeeper.
     *
     * @param p_nodeId
     *         The node's id.
     * @return The specified node's details.
     */
    private NodeDetails getRemoteDetails(short p_nodeId) {
        byte[] data;

        try {
            data = m_curator.getData().forPath(
                    String.format("%s/%s", DISCOVERY_PATH, NodeID.toHexStringShort(p_nodeId)));
        } catch (Exception e) {
            return null;
        }

        try {
            return m_serializer.deserialize(data).getPayload();
        } catch (Exception e) {
            LOGGER.warn("Couldn't deserialize remote node details");
            return null;
        }
    }

    /**
     * Returns this node's details.
     *
     * @return This node's details.
     */
    public @Nullable NodeDetails getDetails() {
        NodeDetails details = m_instance.getPayload();

        if (details == null) {
            return null;
        }

        return details;
    }

    /**
     * Returns all NodeDetails this node knows of.
     *
     * @return All known NodeDetails.
     */
    public Collection<NodeDetails> getAll() {
        return m_serviceMap.values();
    }

    /**
     * Called when the cache has changed (instances added/deleted, etc.)
     */
    @Override
    public void cacheChanged() {
        LOGGER.debug("Service discovery cache changed");

        final Set<NodeDetails> remoteDetails = m_cache.getInstances().stream()
                .map(ServiceInstance::getPayload)
                .collect(Collectors.toSet());

        final Set<NodeDetails> localDetails = new HashSet<>(m_serviceMap.values());

        final Sets.SetView<NodeDetails> changedNodes = Sets.difference(remoteDetails, localDetails);
        for (NodeDetails details : changedNodes) {
            NodeDetails oldDetails = m_serviceMap.put(details.getId(), details);

            if (oldDetails != null) {
                notifyListener(ListenerEvent.NODE_UPDATED, details);
                continue;
            }

            if (details.getRole() == NodeRole.SUPERPEER) {
                notifyListener(ListenerEvent.SUPERPEER_JOINED, details);
            } else {
                notifyListener(ListenerEvent.PEER_JOINED, details);
            }
        }

        final Set<Short> remoteIds = remoteDetails.stream().map(NodeDetails::getId).collect(Collectors.toSet());
        final Set<Short> localIds = localDetails.stream().map(NodeDetails::getId).collect(Collectors.toSet());

        final Sets.SetView<Short> leftNodeIds = Sets.difference(localIds, remoteIds);
        NodeDetails leftNode;
        for (Short nodeId : leftNodeIds) {
            leftNode = m_serviceMap.get(nodeId);

            if (!leftNode.isOnline()) {
                continue;
            }

            m_serviceMap.put(nodeId, leftNode.withOnline(false));

            if (leftNode.getRole() == NodeRole.SUPERPEER) {
                notifyListener(ListenerEvent.SUPERPEER_LEFT, leftNode);
            } else {
                notifyListener(ListenerEvent.PEER_LEFT, leftNode);
            }
        }
    }

    /**
     * Called when there is a state change in the connection.
     *
     * @param p_client
     *         The client.
     * @param p_newState
     *         The new state.
     */
    @Override
    public void stateChanged(CuratorFramework p_client, ConnectionState p_newState) {
        LOGGER.debug("Curator connection state changed to {}", p_newState.isConnected() ? "CONNECTED" : "DISCONNECTED");
    }

    private enum ListenerEvent {
        PEER_JOINED, PEER_LEFT, SUPERPEER_JOINED, SUPERPEER_LEFT, NODE_UPDATED
    }

    public interface Listener {
        void onPeerJoined(final NodeDetails p_nodeDetails);

        void onPeerLeft(final NodeDetails p_nodeDetails);

        void onSuperpeerJoined(final NodeDetails p_nodeDetails);

        void onSuperpeerLeft(final NodeDetails p_nodeDetails);

        void onNodeUpdated(final NodeDetails p_nodeDetails);
    }

    @SuppressWarnings("unused")
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
         * The node's capabilities.
         */
        private final int m_capabilities;

        @JsonCreator
        private NodeDetails(
                @JsonProperty("id")
                        short p_id,
                @JsonProperty("ip")
                        String p_ip,
                @JsonProperty("port")
                        int p_port,
                @JsonProperty("rack")
                        short p_rack,
                @JsonProperty("switch")
                        short p_switch,
                @JsonProperty("role")
                        NodeRole p_role,
                @JsonProperty("online")
                        boolean p_online,
                @JsonProperty("capabilities")
                        int p_capabilities) {
            m_id = p_id;
            m_ip = p_ip;
            m_port = p_port;
            m_rack = p_rack;
            m_switch = p_switch;
            m_role = p_role;
            m_online = p_online;
            m_capabilities = p_capabilities;
        }

        public static Builder builder(final short p_id, final String p_ip, final int p_port) {
            return new Builder(p_id, p_ip, p_port);
        }

        public static NodeDetails fromByteArray(final byte[] p_bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(p_bytes);
            ByteBufferImExporter importer = new ByteBufferImExporter(buffer);

            short tmpId = 0;
            String tmpIp = "";
            int tmpPort = 0;
            short tmpRack = 0;
            short tmpSwitch = 0;
            char tmpRole = '0';
            boolean tmpOnline;
            int tmpCapabilites = 0;

            tmpId = importer.readShort(tmpId);
            tmpIp = importer.readString(tmpIp);
            tmpPort = importer.readInt(tmpPort);
            tmpRack = importer.readShort(tmpRack);
            tmpSwitch = importer.readShort(tmpSwitch);
            tmpRole = importer.readChar(tmpRole);
            tmpOnline = importer.readBoolean(false);
            tmpCapabilites = importer.readInt(tmpCapabilites);

            return new NodeDetails(tmpId, tmpIp, tmpPort, tmpRack, tmpSwitch,
                    NodeRole.getRoleByAcronym(tmpRole), tmpOnline, tmpCapabilites);
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

        public int getCapabilities() {
            return m_capabilities;
        }

        public InetSocketAddress getAddress() {
            return new InetSocketAddress(m_ip, m_port);
        }

        public NodeDetails withCapabilities(int p_capabilities) {
            return new NodeDetails(m_id, m_ip, m_port, m_rack, m_switch, m_role, m_online, p_capabilities);
        }

        public NodeDetails withOnline(boolean p_isOnline) {
            return new NodeDetails(m_id, m_ip, m_port, m_rack, m_switch, m_role, p_isOnline, m_capabilities);
        }

        @Override
        public boolean equals(Object p_o) {
            if (this == p_o) {
                return true;
            }
            if (p_o == null || getClass() != p_o.getClass()) {
                return false;
            }
            NodeDetails details = (NodeDetails) p_o;
            return m_id == details.m_id &&
                    m_port == details.m_port &&
                    m_rack == details.m_rack &&
                    m_switch == details.m_switch &&
                    m_online == details.m_online &&
                    m_capabilities == details.m_capabilities &&
                    Objects.equals(m_ip, details.m_ip) &&
                    m_role == details.m_role;
        }

        @Override
        public int hashCode() {
            return Short.hashCode(m_id);
        }

        @Override
        public String toString() {
            return String.format("[%c|%04X](%s:%d)", m_role.getAcronym(), m_id, m_ip, m_port);
        }

        ServiceInstance<NodeDetails> toServiceInstance() {
            return new ServiceInstance<>(SERVICE_NAME, NodeID.toHexStringShort(m_id), m_ip,
                    m_port, 0, this, System.currentTimeMillis(), ServiceType.DYNAMIC, null, true);
        }

        public byte[] toByteArray() {
            ByteBuffer buffer = ByteBuffer.allocate(3 * Short.BYTES + 2 * Integer.BYTES +
                    Character.BYTES + 2 * Byte.BYTES + ObjectSizeUtil.sizeofString(m_ip));
            ByteBufferImExporter exporter = new ByteBufferImExporter(buffer);

            exporter.writeShort(m_id);
            exporter.writeString(m_ip);
            exporter.writeInt(m_port);
            exporter.writeShort(m_rack);
            exporter.writeShort(m_switch);
            exporter.writeChar(m_role.getAcronym());
            exporter.writeBoolean(m_online);
            exporter.writeInt(m_capabilities);

            return buffer.array();
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

            public Builder withCapabilities(int p_capabilities) {
                m_capabilities = p_capabilities;
                return this;
            }

            public NodeDetails build() {
                return new NodeDetails(m_id, m_ip, m_port, m_rack, m_switch, m_role, m_online, m_capabilities);
            }
        }
    }
}

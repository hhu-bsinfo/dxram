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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import de.hhu.bsinfo.dxutils.Poller;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.jetbrains.annotations.Nullable;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponentConfig;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.CRC16;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Implementation of the BootComponent interface with zookeeper.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = true, supportsPeer = true)
@AbstractDXRAMComponent.Attributes(priorityInit = DXRAMComponentOrder.Init.BOOT,
        priorityShutdown = DXRAMComponentOrder.Shutdown.BOOT)
public class ZookeeperBootComponent extends AbstractBootComponent<ZookeeperBootComponentConfig> {
    private short m_id = NodeID.INVALID_ID;
    private String m_address;
    private int m_port;
    private NodeRole m_role;
    private int m_capabilities;

    private NodeRegistry.NodeDetails m_details;

    private NodeRegistry m_nodeRegistry;

    private static final int INVALID_COUNTER_VALUE = -1;
    private static final int BOOTSTRAP_COUNTER_VALUE = 1;

    private int m_counterValue = INVALID_COUNTER_VALUE;

    private CuratorFramework m_curatorClient;

    private static final RetryPolicy RETRY_POLICY = new ExponentialBackoffRetry(1000, 3);

    private static final String BASE_DIR = "/dxram";
    private static final String COUNTER_PATH = String.format("%s/counter", BASE_DIR);
    private static final String BOOTSTRAP_NODE_PATH = String.format("%s/boot", BASE_DIR);

    private static final InetSocketAddress INVALID_ADDRESS = new InetSocketAddress("255.255.255.255", 0xFFFF);

    private DistributedAtomicInteger m_counter;

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
        m_address = p_config.getEngineConfig().getAddress().getIP();
        m_port = p_config.getEngineConfig().getAddress().getPort();
        m_role = p_config.getEngineConfig().getRole();
        m_capabilities = detectNodeCapabilities(p_config);

        LOGGER.info("Initializing with address %s:%d and role %s", m_address, m_port, m_role);

        String zooKeeperAddress = String.format("%s:%d", getConfig().getConnection().getIP(),
                getConfig().getConnection().getPort());

        LOGGER.info("Connecting to ZooKeeper at %s", zooKeeperAddress);

        // Connect to Zookeeper
        m_curatorClient = CuratorFrameworkFactory.newClient(zooKeeperAddress, RETRY_POLICY);
        m_curatorClient.start();

        m_nodeRegistry = new NodeRegistry(m_curatorClient);
        m_counter = new DistributedAtomicInteger(m_curatorClient, COUNTER_PATH, RETRY_POLICY);

        // Assign a globally unique counter value to this superpeer
        if (m_role == NodeRole.SUPERPEER) {
            assignNodeId();
        }

        if (isBootstrapNode()) {
            // Start bootstrap node initialization process if this is the first superpeer
            return initializeBootstrapNode();
        } else {
            // Start normal node initialization process if this is not the first superpeer
            return initializeNormalNode();
        }
    }

    @Override
    protected boolean shutdownComponent() {
        m_nodeRegistry.close();
        m_curatorClient.close();
        return true;
    }

    /**
     * Builds this node's details.
     *
     * @return This node's details.
     */
    private NodeRegistry.NodeDetails buildNodeDetails() {
        return NodeRegistry.NodeDetails.builder(m_id, m_address, m_port)
                .withRole(m_role)
                .withRack(getConfig().getRack())
                .withSwitch(getConfig().getSwitch())
                .withOnline(true)
                .withCapabilities(m_capabilities)
                .build();
    }

    /**
     * Initializes this node as the bootstrap node.
     *
     * @return true, if initialization succeeded; false else
     */
    private boolean initializeBootstrapNode() {
        LOGGER.info("Starting bootstrap process");

        m_details = buildNodeDetails();

        try {
            m_nodeRegistry.start(m_details);
        } catch (Exception e) {
            LOGGER.error("Starting node registry failed", e);
            return false;
        }

        try {
            // Insert own node information into ZooKeeper so other nodes can find the bootstrap node
            m_curatorClient.create().creatingParentsIfNeeded().forPath(BOOTSTRAP_NODE_PATH, m_details.toByteArray());
        } catch (Exception e) {
            LOGGER.error("Creating bootstrap entry failed", e);
            return false;
        }

        LOGGER.info("Finished bootstrap process, node ID of this instance: %X", getDetails().getId());

        return true;
    }

    /**
     * Initializes this node as a normal node.
     *
     * @return true, if initialization succeeded; false else
     */
    private boolean initializeNormalNode() {
        LOGGER.info("Waiting on bootstrap node to finish initialization");

        // Wait until bootstrap node finishes initializing
        Poller.blockingPoll(this::getBootstrapDetails, 1, TimeUnit.SECONDS);

        LOGGER.info("Bootstrap node is ready");

        // Assign a globally unique counter value in case this is a peer, which hasn't assigned it yet
        if (m_counterValue == INVALID_COUNTER_VALUE) {
            assignNodeId();
        }

        m_details = buildNodeDetails();

        try {
            m_nodeRegistry.start(m_details);
        } catch (Exception e) {
            LOGGER.error("Starting node registry failed");
            return false;
        }

        LOGGER.info("Finished initialization, node ID of this instance: %X", getDetails().getId());

        return true;
    }

    /**
     * Assigns a globally unique id to this node.
     */
    private void assignNodeId() {
        AtomicValue<Integer> atomicValue;

        do {
            try {
                atomicValue = m_counter.increment();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            if (!atomicValue.succeeded()) {
                throw new IllegalStateException("Incrementing atomic counter failed");
            }

            m_counterValue = atomicValue.postValue();
            m_id = calculateNodeId();
        } while (m_nodeRegistry.getDetails(m_id) != null);

        LOGGER.info("Assigned counter value %d to this node", m_counterValue);
    }

    /**
     * Calculates this node's id based on its unique counter value.
     *
     * @return This node's id.
     */
    private short calculateNodeId() {
        int seed = 1;
        short nodeId = 0;

        for (int i = 0; i < m_counterValue; i++) {
            nodeId = CRC16.continuousHash(seed, nodeId);
            seed++;
        }

        return nodeId;
    }

    /**
     * Looks up the bootstrap node's details and returns them.
     *
     * @return The bootstrap node's details.
     */
    private @Nullable NodeRegistry.NodeDetails getBootstrapDetails() {
        byte[] bootBytes;

        try {
            bootBytes = m_curatorClient.getData().forPath(BOOTSTRAP_NODE_PATH);
        } catch (Exception e) {
            return null;
        }

        return NodeRegistry.NodeDetails.fromByteArray(bootBytes);
    }

    /**
     * Indicates if this node is responsible for the bootstrap process.
     *
     * @return True if this node is the bootstrap node; false else.
     */
    private boolean isBootstrapNode() {
        return m_counterValue == BOOTSTRAP_COUNTER_VALUE;
    }

    /**
     * Detects this node's capabilities.
     *
     * @return This node's capabilities.
     */
    private int detectNodeCapabilities(final DXRAMConfig p_config) {
        if (m_role == NodeRole.SUPERPEER) {
            return NodeCapabilities.NONE;
        }

        if (getConfig().isClient()) {
            return NodeCapabilities.COMPUTE;
        }

        ChunkComponentConfig chunkConfig = p_config.getComponentConfig(ChunkComponent.class);
        BackupComponentConfig backupConfig = p_config.getComponentConfig(BackupComponent.class);

        int capabilities = 0;

        if (chunkConfig.isChunkStorageEnabled()) {
            capabilities |= NodeCapabilities.STORAGE;
        }

        if (backupConfig.isBackupActive()) {
            capabilities |= NodeCapabilities.BACKUP_SRC;
        }

        if (backupConfig.isBackupActive() && backupConfig.isAvailableForBackup()) {
            capabilities |= NodeCapabilities.BACKUP_DST;
        }

        LOGGER.info("Detected capabilities %s", NodeCapabilities.toString(capabilities));

        return capabilities;
    }

    @Override
    public void registerRegistryListener(final NodeRegistry.Listener p_listener) {
        m_nodeRegistry.registerListener(p_listener);
    }

    @Override
    public NodeRegistry.NodeDetails getDetails() {
        return m_nodeRegistry.getDetails();
    }

    @Override
    public NodeRegistry.NodeDetails getDetails(short p_nodeId) {
        return m_nodeRegistry.getDetails(p_nodeId);
    }

    @Override
    public List<NodeRegistry.NodeDetails> getOnlineNodes() {
        return m_nodeRegistry.getAll().stream()
                .filter(NodeRegistry.NodeDetails::isOnline)
                .collect(Collectors.toList());
    }

    @Override
    public List<Short> getOnlineNodeIds() {
        return m_nodeRegistry.getAll().stream()
                .filter(NodeRegistry.NodeDetails::isOnline)
                .map(NodeRegistry.NodeDetails::getId)
                .collect(Collectors.toList());
    }

    @Override
    public List<Short> getOnlinePeerIds() {
        return m_nodeRegistry.getAll().stream()
                .filter(NodeRegistry.NodeDetails::isOnline)
                .filter(node -> node.getRole() == NodeRole.PEER)
                .map(NodeRegistry.NodeDetails::getId)
                .collect(Collectors.toList());
    }

    @Override
    public List<Short> getOnlineSuperpeerIds() {
        return m_nodeRegistry.getAll().stream()
                .filter(NodeRegistry.NodeDetails::isOnline)
                .filter(node -> node.getRole() == NodeRole.SUPERPEER)
                .map(NodeRegistry.NodeDetails::getId)
                .collect(Collectors.toList());
    }

    @Override
    public List<BackupPeer> getAvailableBackupPeers() {
        return m_nodeRegistry.getAll().stream()
                .filter(node -> node.getRole() == NodeRole.PEER && node.isOnline() &&
                        NodeCapabilities.supports(node.getCapabilities(), NodeCapabilities.BACKUP_DST) &&
                        node.getId() != m_id)
                .map(node -> new BackupPeer(node.getId(), node.getRack(), node.getSwitch()))
                .collect(Collectors.toList());
    }

    @Override
    public List<Short> getSupportingNodes(final int p_capabilities) {
        return m_nodeRegistry.getAll().stream()
                .filter(node -> NodeCapabilities.supportsAll(node.getCapabilities(), p_capabilities))
                .map(NodeRegistry.NodeDetails::getId)
                .collect(Collectors.toList());
    }

    @Override
    public void updateNodeCapabilities(int p_capibilities) {
        NodeRegistry.NodeDetails oldDetails = m_nodeRegistry.getDetails();

        if (oldDetails == null) {
            throw new IllegalStateException("Lost own node information");
        }

        m_nodeRegistry.updateNodeDetails(oldDetails.withCapabilities(p_capibilities));
    }

    @Override
    public short getBootstrapId() {
        NodeRegistry.NodeDetails bootstrapDetails = getBootstrapDetails();

        if (bootstrapDetails == null) {
            return NodeID.INVALID_ID;
        }

        return bootstrapDetails.getId();
    }

    @Override
    public short getNodeId() {
        return getDetails().getId();
    }

    @Override
    public NodeRole getNodeRole() {
        return getDetails().getRole();
    }

    @Override
    public short getRack() {
        return getDetails().getRack();
    }

    @Override
    public short getSwitch() {
        return getDetails().getSwitch();
    }

    @Override
    public int getNumberOfAvailableSuperpeers() {
        return getOnlineSuperpeerIds().size();
    }

    @Override
    public InetSocketAddress getNodeAddress(short p_nodeId) {
        NodeRegistry.NodeDetails details = getDetails(p_nodeId);

        if (details == null) {
            LOGGER.warn("Couldn't find node 0x%04X", p_nodeId);
            return INVALID_ADDRESS;
        }

        return details.getAddress();
    }

    @Override
    public NodeRole getNodeRole(short p_nodeId) {
        NodeRegistry.NodeDetails details = getDetails(p_nodeId);

        if (details == null) {
            LOGGER.warn("Couldn't find node 0x%04X", p_nodeId);
            return null;
        }

        return details.getRole();
    }

    @Override
    public boolean isNodeOnline(short p_nodeId) {
        NodeRegistry.NodeDetails details = getDetails(p_nodeId);

        if (details == null) {
            LOGGER.warn("Couldn't find node 0x%04X", p_nodeId);
            return false;
        }

        return details.isOnline();
    }

    @Override
    public int getNodeCapabilities(short p_nodeId) {
        NodeRegistry.NodeDetails details = getDetails(p_nodeId);

        if (details == null) {
            LOGGER.warn("Couldn't find node 0x%04X", p_nodeId);
            return NodeCapabilities.INVALID;
        }

        return details.getCapabilities();
    }
}

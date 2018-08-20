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

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponentConfig;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponentConfig;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.CRC16;
import de.hhu.bsinfo.dxutils.NodeID;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of the BootComponent interface with zookeeper.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
@SuppressWarnings("WeakerAccess")
public class ZookeeperBootComponent extends AbstractBootComponent<ZookeeperBootComponentConfig>
        implements NodeRegistry.Listener {

    private DXRAMContext.Config m_contextConfig;

    private short m_id = NodeID.INVALID_ID;
    private String m_address;
    private int m_port;
    private NodeRole m_role;

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

    /**
     * Constructor
     */
    public ZookeeperBootComponent() {
        super(DXRAMComponentOrder.Init.BOOT, DXRAMComponentOrder.Shutdown.BOOT, ZookeeperBootComponentConfig.class);
    }

    /**
     * Called when the component is initialized. Setup data structures, get dependent components, read settings etc.
     *
     * @param p_config Configuration instance provided by the engine.
     * @return True if initialing was successful, false otherwise.
     */
    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {
        m_contextConfig = p_config;
        m_address = m_contextConfig.getEngineConfig().getAddress().getIP();
        m_port = m_contextConfig.getEngineConfig().getAddress().getPort();
        m_role = m_contextConfig.getEngineConfig().getRole();

        LOGGER.info("Initializing with address %s:%d and role %s", m_address, m_port, m_role);

        String zooKeeperAddress = String.format("%s:%d", m_config.getConnection().getIP(), m_config.getConnection().getPort());

        LOGGER.info("Connecting to ZooKeeper at %s", zooKeeperAddress);

        // Connect to Zookeeper
        m_curatorClient = CuratorFrameworkFactory.newClient(zooKeeperAddress, RETRY_POLICY);
        m_curatorClient.start();

        m_nodeRegistry = new NodeRegistry(m_curatorClient, this);
        m_counter = new DistributedAtomicInteger(m_curatorClient, COUNTER_PATH, RETRY_POLICY);

        // Assign a globally unique counter value to this superpeer
        if (m_role.equals(NodeRole.SUPERPEER)) {
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

    /**
     * Finish initialization of this component when all services are running.
     *
     * @return True if finishing initialization was successful, false otherwise.
     */
    @Override
    public boolean finishInitComponent() {
        if (m_role == NodeRole.SUPERPEER) {
            return true;
        }

        return true;
    }

    /**
     * Check if the component supports the superpeer node role.
     *
     * @return True if supporting, false otherwise.
     */
    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    /**
     * Check if the component supports the peer node role.
     *
     * @return True if supporting, false otherwise.
     */
    @Override
    protected boolean supportsPeer() {
        return true;
    }

    /**
     * Shut down this component.
     *
     * @return True if shutting down was successful, false otherwise.
     */
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
                .withRack(m_config.getRack())
                .withSwitch(m_config.getSwitch())
                .withOnline(true)
                .withCapabilities(detectNodeCapabilities())
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
        } catch (Exception p_e) {
            LOGGER.error("Starting node registry failed", p_e);
            return false;
        }

        try {
            // Insert own node information into ZooKeeper so other nodes can find the bootstrap node
            m_curatorClient.create().creatingParentsIfNeeded().forPath(BOOTSTRAP_NODE_PATH, m_details.toByteArray());
        } catch (Exception p_e) {
            LOGGER.error("Creating bootstrap entry failed", p_e);
            return false;
        }

        LOGGER.info("Finished bootstrap process");

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
        NodeRegistry.NodeDetails bootstrapDetails = getBootstrapDetails();

        while(bootstrapDetails == null) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException p_e) {
                // Ignored
            }

            bootstrapDetails = getBootstrapDetails();
        }

        LOGGER.info("Bootstrap node is ready");

        // Assign a globally unique counter value in case this is a peer, which hasn't assigned it yet
        if (m_counterValue == INVALID_COUNTER_VALUE) {
            assignNodeId();
        }

        m_details = buildNodeDetails();

        try {
            m_nodeRegistry.start(m_details);
        } catch (Exception p_e) {
            LOGGER.error("Starting node registry failed");
            return false;
        }

        return true;
    }

    /**
     * Assigns a globally unique id to this node.
     */
    private void assignNodeId() {
        AtomicValue<Integer> atomicValue;

        try {
            atomicValue = m_counter.increment();
        } catch (Exception p_e) {
            throw new RuntimeException(p_e);
        }

        if (!atomicValue.succeeded()) {
            throw new IllegalStateException("Incrementing atomic counter failed");
        }

        m_counterValue = atomicValue.postValue();
        m_id = calculateNodeId();

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

        for(int i = 0; i < m_counterValue; i++) {
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
    @Nullable
    private NodeRegistry.NodeDetails getBootstrapDetails() {
        byte[] bootBytes;

        try {
            bootBytes = m_curatorClient.getData().forPath(BOOTSTRAP_NODE_PATH);
        } catch (Exception p_e) {
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
    private int detectNodeCapabilities() {
        if (m_role == NodeRole.SUPERPEER) {
            return NodeCapabilities.NONE;
        }

        if (m_config.isClient()) {
            return NodeCapabilities.COMPUTE;
        }

        MemoryManagerComponentConfig memoryConfig = m_contextConfig.getComponentConfig(MemoryManagerComponentConfig.class);
        BackupComponentConfig backupConfig = m_contextConfig.getComponentConfig(BackupComponentConfig.class);

        int capabilities = 0;

        if (memoryConfig.getKeyValueStoreSize().getBytes() > 0L) {
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
                .filter(node -> node.getRole().equals(NodeRole.PEER))
                .map(NodeRegistry.NodeDetails::getId)
                .collect(Collectors.toList());
    }

    @Override
    public List<Short> getOnlineSuperpeerIds() {
        return m_nodeRegistry.getAll().stream()
                .filter(NodeRegistry.NodeDetails::isOnline)
                .filter(node -> node.getRole().equals(NodeRole.SUPERPEER))
                .map(NodeRegistry.NodeDetails::getId)
                .collect(Collectors.toList());
    }

    @Override
    public List<BackupPeer> getAvailableBackupPeers() {
        return m_nodeRegistry.getAll().stream()
                .filter(node -> node.getRole().equals(NodeRole.PEER) && node.isOnline() && node.isAvailableForBackup())
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

    @Override
    public void onPeerJoined(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Node %s with capabilities %s joined the network",
                p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));
    }

    @Override
    public void onPeerLeft(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Node %s with capabilities %s left the network",
                p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));
    }

    @Override
    public void onSuperpeerJoined(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Node %s with capabilities %s joined the network",
                p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));
    }

    @Override
    public void onSuperpeerLeft(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Node %s with capabilities %s left the network",
                p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));
    }

    @Override
    public void onNodeUpdated(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Updated node %s with capabilities %s",
                p_nodeDetails, NodeCapabilities.toString(p_nodeDetails.getCapabilities()));
    }
}

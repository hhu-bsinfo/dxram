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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import de.hhu.bsinfo.dxram.job.JobComponent;
import de.hhu.bsinfo.dxram.job.JobComponentConfig;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeServiceConfig;
import de.hhu.bsinfo.dxutils.Poller;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
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

    /**
     * This node's unique id.
     */
    private short m_id = NodeID.INVALID_ID;

    /**
     * This node's ip address.
     */
    private String m_address;

    /**
     * This node's port.
     */
    private int m_port;

    /**
     * This node's node role.
     */
    private NodeRole m_role;

    /**
     * This node's capabilities.
     */
    private int m_capabilities;

    /**
     * The node details belonging to this node.
     */
    private NodeRegistry.NodeDetails m_details;

    /**
     * The node registry containing the cluster's node details.
     */
    private NodeRegistry m_nodeRegistry;

    /**
     * An atomic integer used for generating unique identifiers.
     */
    private DistributedAtomicInteger m_counter;

    /**
     * The curator client used for ZooKeeper connections.
     */
    private CuratorFramework m_curatorClient;

    /**
     * The retry policy used for ZooKeeper connections.
     */
    private RetryPolicy m_retryPolicy;

    /**
     * This node's unique counter value.
     */
    private int m_counterValue = INVALID_COUNTER_VALUE;

    /**
     * The ZooKeeper Server instance.
     */
    private final ZooKeeperServerMain m_zooKeeperServer = new ZooKeeperServerMain();

    /**
     * The ZooKeeper server configuration.
     */
    private final ServerConfig m_zooKeeperServerConfig = new ServerConfig();

    /**
     * A Thread running the ZooKeeper server asynchronously.
     */
    private final Thread m_zooKeeperServerThread = new Thread(() -> {
        try {
            m_zooKeeperServer.runFromConfig(m_zooKeeperServerConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }, "ZooKeeperServer");

    private static final int INVALID_COUNTER_VALUE = -1;
    private static final int RETRY_INTERVAL = 1000;
    private static final String BASE_DIR = "/dxram";
    private static final String COUNTER_PATH = String.format("%s/counter", BASE_DIR);
    private static final String BOOTSTRAP_NODE_PATH = String.format("%s/boot", BASE_DIR);
    private static final InetSocketAddress INVALID_ADDRESS = new InetSocketAddress("255.255.255.255", 0xFFFF);

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
        m_address = p_config.getEngineConfig().getAddress().getIP();
        m_port = p_config.getEngineConfig().getAddress().getPort();
        m_role = p_config.getEngineConfig().getRole();
        m_capabilities = detectNodeCapabilities(p_config);
        m_retryPolicy = new RetryUntilElapsed((int) getConfig().getTimeout().getMs(), RETRY_INTERVAL);

        // Start ZooKeeper server on the bootstrap node
        if (getConfig().isBootstrap()) {
            initializeZooKeeperServer();
        }

        LOGGER.debug("Searching free port with initial value of %d", m_port);

        m_port = getNextOpenPort(m_port);

        LOGGER.debug("Initializing with address %s:%d and role %s", m_address, m_port, m_role);

        establishCuratorConnection();
        configureCurator();

        // Assign an id to this node using ZooKeeper
        assignNodeId();

        if (getConfig().isBootstrap()) {
            // Start bootstrap node initialization process if this is the first superpeer
            return initializeBootstrapNode();
        } else {
            // Start normal node initialization process if this is not the first superpeer
            return initializeNormalNode();
        }
    }

    private static int getNextOpenPort(final int p_start) {
        int currentPort = p_start;
        while(true) {
            try (ServerSocket socket = new ServerSocket(currentPort)) {
                return socket.getLocalPort();
            } catch (IOException e) {
                currentPort++;
            }
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

    private void configureCurator() {
        m_nodeRegistry = new NodeRegistry(m_curatorClient);
        m_counter = new DistributedAtomicInteger(m_curatorClient, COUNTER_PATH, m_retryPolicy);
    }

    private void establishCuratorConnection() {
        // Create ZooKeeper connection string
        String zooKeeperAddress = String.format("%s:%d", getConfig().getConnection().getIP(),
                getConfig().getConnection().getPort());

        LOGGER.debug("Connecting to ZooKeeper at %s", zooKeeperAddress);

        // Connect to Zookeeper
        m_curatorClient = CuratorFrameworkFactory.newClient(zooKeeperAddress, m_retryPolicy);
        m_curatorClient.start();

        try {
            // Wait until the connection is established
            m_curatorClient.blockUntilConnected();
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted while waiting for ZooKeeper connection");
        }
    }

    private String[] getZooKeeperConfiguration() {
        return new String[] {
                String.valueOf(getConfig().getConnection().getPort()), // ZooKeeper client port
                getConfig().getDataDir(), // ZooKeeper data directory
                String.valueOf(2000), // ZooKeeper tick time
                String.valueOf(1000) // Max client connections
        };
    }

    private void deleteZooKeeperData() {
        LOGGER.debug("Deleting ZooKeeper data directory");
        Path path = Paths.get(getConfig().getDataDir());

        if (!Files.exists(path)) {
            return;
        }

        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path p_file, BasicFileAttributes p_attrs) throws IOException {
                    Files.delete(p_file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path p_dir, IOException p_exception) throws IOException {
                    Files.delete(p_dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            LOGGER.warn("Couldn't delete all files within ZooKeeper's data directory");
        }
    }

    private void initializeZooKeeperServer() {
        deleteZooKeeperData();
        m_zooKeeperServerConfig.parse(getZooKeeperConfiguration());
        LOGGER.debug("Starting ZooKeeper server on port %d",
                m_zooKeeperServerConfig.getClientPortAddress().getPort());
        m_zooKeeperServerThread.start();
    }

    /**
     * Initializes this node as the bootstrap node.
     *
     * @return true, if initialization succeeded; false else
     */
    private boolean initializeBootstrapNode() {
        LOGGER.info("Running in bootstrap mode");

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

        LOGGER.info("Assigned node ID %X to this instance", getDetails().getId());

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

        LOGGER.debug("Bootstrap node is ready");

        m_details = buildNodeDetails();

        try {
            m_nodeRegistry.start(m_details);
        } catch (Exception e) {
            LOGGER.error("Starting node registry failed");
            return false;
        }

        LOGGER.info("Assigned node ID %X to this instance", getDetails().getId());

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

        LOGGER.debug("Assigned counter value %d to this node", m_counterValue);
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
     * Detects this node's capabilities.
     *
     * @return This node's capabilities.
     */
    private int detectNodeCapabilities(final DXRAMConfig p_config) {
        if (m_role == NodeRole.SUPERPEER) {
            return NodeCapabilities.NONE;
        }

        ChunkComponentConfig chunkConfig = p_config.getComponentConfig(ChunkComponent.class);
        BackupComponentConfig backupConfig = p_config.getComponentConfig(BackupComponent.class);
        JobComponentConfig jobComponentConfig = p_config.getComponentConfig(JobComponent.class);

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

        if (jobComponentConfig.isEnabled()) {
            capabilities |= NodeCapabilities.COMPUTE;
        }

        LOGGER.debug("Detected capabilities %s", NodeCapabilities.toString(capabilities));

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

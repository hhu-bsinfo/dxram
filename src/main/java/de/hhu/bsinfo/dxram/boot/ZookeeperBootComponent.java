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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.boot.NodesConfiguration.NodeEntry;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lookup.events.NodeJoinEvent;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.BloomFilter;
import de.hhu.bsinfo.dxutils.CRC16;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;
import org.codehaus.jackson.map.ObjectMapper;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the BootComponent interface with zookeeper.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
public class ZookeeperBootComponent extends AbstractBootComponent<ZookeeperBootComponentConfig>
        implements EventListener<AbstractEvent>, NodeRegistry.Listener {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EventComponent m_event;

    private short m_id = NodeID.INVALID_ID;
    private IPV4Unit m_address;
    private NodeRole m_nodeRole;

    private NodeRegistry.NodeDetails m_details;

    private BloomFilter m_bloomFilter;

    private NodeRegistry m_nodeRegistry;

    private static final int INVALID_COUNTER_VALUE = -1;
    private static final int BOOTSTRAP_COUNTER_VALUE = 1;

    private int m_counterValue = INVALID_COUNTER_VALUE;

    private CuratorFramework m_curatorClient;

    private static final RetryPolicy RETRY_POLICY = new ExponentialBackoffRetry(1000, 3);

    private static final String BASE_DIR = "/dxram";
    private static final String NODES_DIR = String.format("%s/nodes", BASE_DIR);
    private static final String LOCK_DIR = String.format("%s/lock", BASE_DIR);
    private static final String BARRIER_DIR = String.format("%s/barrier", BASE_DIR);
    private static final String COUNTER_PATH = String.format("%s/counter", BASE_DIR);

    private static final String SUPERPEER_DIR = String.format("%s/superpeer", NODES_DIR);
    private static final String PEER_DIR = String.format("%s/peer", NODES_DIR);

    private static final String BOOTSTRAP_NODE_PATH = String.format("%s/boot", BASE_DIR);
    private static final String BOOTSTRAP_LOCK_PATH = String.format("%s/boot", LOCK_DIR);
    private static final String BOOTSTRAP_BARRIER_PATH = String.format("%s/boot", BARRIER_DIR);

    private DistributedAtomicInteger m_counter;

    /**
     * Constructor
     */
    public ZookeeperBootComponent() {
        super(DXRAMComponentOrder.Init.BOOT, DXRAMComponentOrder.Shutdown.BOOT, ZookeeperBootComponentConfig.class);
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
        NodeRegistry.NodeDetails details = m_nodeRegistry.getDetails(m_id).withCapabilities(p_capibilities);
        m_nodeRegistry.updateNodeDetails(details);
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
            return null;
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
    public void singleNodeCleanup(final short p_nodeId, final NodeRole p_role) {

    }

    @Override
    public void eventTriggered(final AbstractEvent p_event) {
        if (p_event instanceof NodeFailureEvent) {

        } else if (p_event instanceof NodeJoinEvent) {

        }
    }

    @Override
    public boolean finishInitComponent() {

        return true;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_event = p_componentAccessor.getComponent(EventComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {

        m_address = p_config.getEngineConfig().getAddress();
        m_nodeRole = p_config.getEngineConfig().getRole();
        m_bloomFilter = new BloomFilter((int) m_config.getBitfieldSize().getBytes(), 65536);

        LOGGER.info("Initializing with address %s, role %s", m_address, m_nodeRole);

        m_event.registerListener(this, NodeFailureEvent.class);
        m_event.registerListener(this, NodeJoinEvent.class);

        String zooKeeperAddress = m_config.getConnection().getAddressStr();

        // Connect to Zookeeper
        m_curatorClient = CuratorFrameworkFactory.newClient(zooKeeperAddress, RETRY_POLICY);
        m_curatorClient.start();

        m_nodeRegistry = new NodeRegistry(m_curatorClient, this);

        m_counter = new DistributedAtomicInteger(m_curatorClient, COUNTER_PATH, RETRY_POLICY);

        // Assign a globally unique counter value to this superpeer
        if (m_nodeRole.equals(NodeRole.SUPERPEER)) {
            assignNodeId();
        }

        // Start bootstrap process if this is the first node
        if (isBootstrapNode()) {
            try {
                initializeBootstrapNode();
            } catch (Exception p_e) {
                LOGGER.error("Initializing bootstrap node failed", p_e);
                return false;
            }

            // Finish bootstrap process
            return true;
        }

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

        // Assign a globally unique counter value if it hasn't been assigned yet
        if (m_counterValue == -1) {
            assignNodeId();
        }

        m_details = buildNodeDetails();

        try {
            m_nodeRegistry.start(m_details);
        } catch (Exception p_e) {
            LOGGER.error("Starting node registry failed");
            return false;
        }

        LOGGER.info("Started node registry");

        LOGGER.info("Assigned node id 0x%04x to this node", m_id);

        LOGGER.info("Using 0x%04x as bootstrap node with ip address %s", bootstrapDetails.getId(), bootstrapDetails.getAddress());

        return true;
    }

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



    @Override
    protected boolean shutdownComponent() {
        m_curatorClient.close();

        return true;
    }

    /**
     * Builds this node's details.
     *
     * @return This node's details.
     */
    private NodeRegistry.NodeDetails buildNodeDetails() {
        return NodeRegistry.NodeDetails.builder(m_id, m_address.getIP(), m_address.getPort())
                .withRole(m_nodeRole)
                .withRack(m_config.getRack())
                .withSwitch(m_config.getSwitch())
                .withOnline(true)
                .build();
    }

    /**
     * Initializes this node as the bootstrap node.
     *
     * @throws Exception If ZooKeeper reports errors.
     */
    private void initializeBootstrapNode() throws Exception {

        LOGGER.info("Starting bootstrap process on node %s", m_address.getAddressStr());

        short nodeId = calculateNodeId();

        m_details = buildNodeDetails();

        m_nodeRegistry.start(m_details);

        // Insert own node information into ZooKeeper so other nodes can find the bootstrap node
        m_curatorClient.create().creatingParentsIfNeeded().forPath(BOOTSTRAP_NODE_PATH, m_details.toByteArray());

        LOGGER.info("Assigned id 0x%04x to bootstrap node", nodeId);
    }

    /**
     * Assigns a globally unique id to this node.
     */
    private void assignNodeId() {
        AtomicValue<Integer> atomicValue = null;

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
     * Indicates if this node is responsible for the bootstrap process.
     *
     * @return True if this node is the bootstrap node; false else.
     */
    private boolean isBootstrapNode() {
        return m_counterValue == BOOTSTRAP_COUNTER_VALUE;
    }

    @Override
    public void onPeerJoined(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Node %s joined the network", p_nodeDetails);
    }

    @Override
    public void onPeerLeft(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Node %s left the network", p_nodeDetails);
    }

    @Override
    public void onSuperpeerJoined(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Node %s joined the network", p_nodeDetails);
    }

    @Override
    public void onSuperpeerLeft(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Node %s left the network", p_nodeDetails);
    }

    @Override
    public void onNodeUpdated(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Updated node %s", p_nodeDetails);
    }
}

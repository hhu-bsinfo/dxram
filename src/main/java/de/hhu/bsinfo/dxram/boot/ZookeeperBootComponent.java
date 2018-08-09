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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import de.hhu.bsinfo.dxutils.serialization.ByteBufferImExporter;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.boot.NodesConfiguration.NodeEntry;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.events.NodeJoinEvent;
import de.hhu.bsinfo.dxram.util.NodeCapabilities;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxram.util.ZooKeeperHandler;
import de.hhu.bsinfo.dxutils.BloomFilter;
import de.hhu.bsinfo.dxutils.CRC16;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the BootComponent interface with zookeeper.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
public class ZookeeperBootComponent extends AbstractBootComponent<ZookeeperBootComponentConfig>
        implements EventListener<AbstractEvent>, NodeRegistry.Listener {
    // component dependencies
    private EventComponent m_event;
    private LookupComponent m_lookup;

    // private state
    private IPV4Unit m_ownAddress;
    private NodeRole m_nodeRole;
    private ZooKeeperHandler m_zookeeper;
    private short m_bootstrap = NodeID.INVALID_ID;
    private BloomFilter m_bloomFilter;
    private NodesConfiguration m_nodes;

    private NodeRegistry m_nodeRegistry;

    private boolean m_shutdown;

    private static final int COUNTER_VALUE_INVALID = -1;
    private static final int BOOTSTRAP_COUNTER_VALUE = 1;
    private static final int BOOTSTRAP_TIMEOUT = 30000;

    private int m_counterValue = COUNTER_VALUE_INVALID;

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
    public List<BackupPeer> getPeersFromNodeFile() {
        NodeEntry[] allNodes = m_nodes.getNodes();

        NodeEntry currentEntry;
        ArrayList<BackupPeer> ret = new ArrayList<>();
        for (int i = 0; i < allNodes.length; i++) {
            currentEntry = allNodes[i];
            if (currentEntry != null) {
                if (currentEntry.readFromFile() && currentEntry.getRole() == NodeRole.PEER) {
                    ret.add(new BackupPeer((short) (i & 0xFFFF), currentEntry.getRack(), currentEntry.getSwitch()));
                }
            }
        }

        return ret;
    }

    @Override
    public List<BackupPeer> getAvailableBackuppeerIds() {
        NodeEntry[] allNodes = m_nodes.getNodes();

        NodeEntry currentEntry;
        ArrayList<BackupPeer> ret = new ArrayList<>();
        for (int i = 0; i < allNodes.length; i++) {
            currentEntry = allNodes[i];
            if (currentEntry != null) {
                if (currentEntry.getRole() == NodeRole.PEER && currentEntry.getStatus() &&
                        currentEntry.isAvailableForBackup()) {
                    ret.add(new BackupPeer((short) (i & 0xFFFF), currentEntry.getRack(), currentEntry.getSwitch()));
                }
            }
        }

        return ret;
    }

    @Override
    public ArrayList<NodeEntry> getOnlineNodes() {
        return m_nodes.getOnlineNodes();
    }

    @Override
    public NodesConfiguration getNodesConfiguration() {
        return m_nodes;
    }

    @Override
    public void putOnlineNodes(ArrayList<NodeEntry> p_onlineNodes) {
        for (NodeEntry entry : p_onlineNodes) {
            m_nodes.addNode(entry);
        }
    }

    @Override
    public List<Short> getOnlineNodeIds() {
        NodeEntry[] allNodes = m_nodes.getNodes();

        NodeEntry currentEntry;
        ArrayList<Short> ret = new ArrayList<>();
        for (int i = 0; i < allNodes.length; i++) {
            currentEntry = allNodes[i];
            if (currentEntry != null) {
                if (currentEntry.getStatus()) {
                    ret.add((short) (i & 0xFFFF));
                }
            }
        }

        return ret;
    }

    @Override
    public List<Short> getOnlinePeerIds() {
        NodeEntry[] allNodes = m_nodes.getNodes();

        NodeEntry currentEntry;
        ArrayList<Short> ret = new ArrayList<>();
        for (int i = 0; i < allNodes.length; i++) {
            currentEntry = allNodes[i];
            if (currentEntry != null) {
                if (currentEntry.getRole() == NodeRole.PEER && currentEntry.getStatus()) {
                    ret.add((short) (i & 0xFFFF));
                }
            }
        }

        return ret;
    }

    @Override
    public List<Short> getOnlineSuperpeerIds() {
        NodeEntry[] allNodes = m_nodes.getNodes();

        NodeEntry currentEntry;
        ArrayList<Short> ret = new ArrayList<>();
        for (int i = 0; i < allNodes.length; i++) {
            currentEntry = allNodes[i];
            if (currentEntry != null) {
                if (currentEntry.getRole() == NodeRole.SUPERPEER && currentEntry.getStatus()) {
                    ret.add((short) (i & 0xFFFF));
                }
            }
        }

        return ret;
    }

    @Override
    public List<Short> getSupportingNodes(final int p_capabilities) {

        return Arrays.stream(m_nodes.getNodes())
                .filter(node -> NodeCapabilities.supportsAll(node.getCapabilities(), p_capabilities))
                .map(NodeEntry::getNodeID)
                .collect(Collectors.toList());
    }

    @Override
    public short getNodeId() {
        return m_nodes.getOwnNodeID();
    }

    @Override
    public NodeRole getNodeRole() {
        return m_nodes.getOwnNodeEntry().getRole();
    }

    @Override
    public int getNodeCapabilities() {
        return m_nodes.getOwnNodeEntry().getCapabilities();
    }

    @Override
    public void updateNodeCapabilities(int p_capibilities) {
        m_nodes.getOwnNodeEntry().setCapabilities(p_capibilities);
    }

    @Override
    public short getRack() {
        return m_nodes.getOwnNodeEntry().getRack();
    }

    @Override
    public short getSwitch() {
        return m_nodes.getOwnNodeEntry().getSwitch();
    }

    @Override
    public int getNumberOfAvailableSuperpeers() {
        return getOnlineSuperpeerIds().size();
    }

    @Override
    public short getBootstrapId() {
        NodeEntry bootstrapEntry = getBootstrapEntry();

        if (bootstrapEntry == null) {
            return NodeID.INVALID_ID;
        }

        return bootstrapEntry.getNodeID();
    }

    @Override
    public boolean isNodeOnline(final short p_nodeId) {
        NodeEntry entry = m_nodes.getNode(p_nodeId);

        if (entry == null) {
            LOGGER.warn("Could not find node %s", NodeID.toHexString(p_nodeId));
            return false;
        }

        return entry.getStatus();
    }

    @Override
    public NodeRole getNodeRole(final short p_nodeId) {
        NodeEntry entry = m_nodes.getNode(p_nodeId);

        if (entry == null) {
            LOGGER.warn("Could not find node %s", NodeID.toHexString(p_nodeId));
            return null;
        }

        return entry.getRole();
    }

    @Override
    public int getNodeCapabilities(short p_nodeId) {
        NodeEntry entry = m_nodes.getNode(p_nodeId);

        if (entry == null) {
            LOGGER.warn("Could not find node %s", NodeID.toHexString(p_nodeId));
            return 0;
        }

        return entry.getCapabilities();
    }

    @Override
    public InetSocketAddress getNodeAddress(final short p_nodeId) {
        NodeEntry entry = m_nodes.getNode(p_nodeId);
        InetSocketAddress address;
        // return "proper" invalid address if entry does not exist
        if (entry == null) {

            //LOGGER.warn("Could not find ip and port for node id %s", NodeID.toHexString(p_nodeID));

            address = new InetSocketAddress("255.255.255.255", 0xFFFF);
        } else {
            address = entry.getAddress().getInetSocketAddress();
        }

        return address;
    }

    @Override
    public boolean nodeAvailable(final short p_nodeId) {
        return isNodeOnline(p_nodeId);
    }

    @Override
    public void singleNodeCleanup(final short p_nodeId, final NodeRole p_role) {

        m_nodes.getNode(p_nodeId).setStatus(false);
    }

    @Override
    public void eventTriggered(final AbstractEvent p_event) {
        if (p_event instanceof NodeFailureEvent) {
            m_nodes.getNode(((NodeFailureEvent) p_event).getNodeID()).setStatus(false);
        } else if (p_event instanceof NodeJoinEvent) {
            NodeJoinEvent event = (NodeJoinEvent) p_event;

            LOGGER.info(String.format("Node %s with capabilities %s joined", NodeID.toHexString(event.getNodeID()),
                    NodeCapabilities.toString(event.getCapabilities())));

            boolean readFromFile = m_nodes.getNode(event.getNodeID()) != null;
            m_nodes.addNode(new NodeEntry(event.getAddress(), event.getNodeID(), event.getRack(), event.getSwitch(),
                    event.getRole(), event.getCapabilities(), readFromFile,
                    event.isAvailableForBackup(), true));
        }
    }

    @Override
    public boolean finishInitComponent() {

        // Set own status to online
        m_nodes.getOwnNodeEntry().setStatus(true);

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
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {

        m_ownAddress = p_config.getEngineConfig().getAddress();
        m_nodeRole = p_config.getEngineConfig().getRole();
        m_bloomFilter = new BloomFilter((int) m_config.getBitfieldSize().getBytes(), 65536);
        m_nodes = new NodesConfiguration(this);

        LOGGER.info("Initializing with address %s, role %s", m_ownAddress, m_nodeRole);

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
            assignCounterValue();
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
        NodeEntry bootstrapEntry = getBootstrapEntry();

        while(bootstrapEntry == null) {

            try {
                Thread.sleep(1000);
            } catch (InterruptedException p_e) {
                // Ignored
            }

            bootstrapEntry = getBootstrapEntry();
        }

        m_nodes.addNode(bootstrapEntry);

        LOGGER.info("Bootstrap node is ready");

        // Assign a globally unique counter value if it hasn't been assigned yet
        if (m_counterValue == -1) {
            assignCounterValue();
        }

        short nodeId = calculateNodeId();

        NodeEntry ownEntry = new NodeEntry(m_ownAddress, nodeId, m_config.getRack(), m_config.getSwitch(), m_nodeRole,
                NodeCapabilities.NONE, false, false, true);

        m_nodes.addNode(ownEntry);
        m_nodes.setOwnNodeID(nodeId);

        saveNodeEntry(ownEntry);

        NodeRegistry.NodeDetails nodeDetails = buildNodeDetails(nodeId);

        try {
            m_nodeRegistry.start(nodeDetails);
        } catch (Exception p_e) {
            LOGGER.error("Starting node registry failed");
            return false;
        }

        LOGGER.info("Started node registry");

        LOGGER.info("Assigned node id 0x%04x to this node", nodeId);

        LOGGER.info("Using 0x%04x as bootstrap node with ip address %s", bootstrapEntry.getNodeID(), bootstrapEntry.getAddress());

        return true;
    }

    @Override
    @Nullable
    public NodeEntry getNodeEntry(short p_nodeId) {
        byte[] bootBytes;

        String path = String.format("%s/%s", PEER_DIR, NodeID.toHexStringShort(p_nodeId));

        try {
            bootBytes = m_curatorClient.getData().forPath(path);
        } catch (Exception p_e) {
            path = String.format("%s/%s", SUPERPEER_DIR, NodeID.toHexStringShort(p_nodeId));

            try {
                bootBytes = m_curatorClient.getData().forPath(path);
            } catch (Exception p_e1) {
                return null;
            }
        }

        ByteBufferImExporter importer = new ByteBufferImExporter(ByteBuffer.wrap(bootBytes));
        NodeEntry nodeEntry = new NodeEntry(true);
        nodeEntry.importObject(importer);

        return nodeEntry;
    }

    private NodeRegistry.NodeDetails buildNodeDetails(final short p_nodeId) {
        return NodeRegistry.NodeDetails.builder(p_nodeId, m_ownAddress.getIP(), m_ownAddress.getPort())
                .withRole(m_nodeRole)
                .withRack(m_config.getRack())
                .withSwitch(m_config.getSwitch())
                .withOnline(true)
                .build();
    }

    private void saveNodeEntry(NodeEntry p_entry) {
        ByteBuffer buffer = ByteBuffer.allocate(p_entry.sizeofObject());
        ByteBufferImExporter exporter = new ByteBufferImExporter(buffer);
        exporter.exportObject(p_entry);

        String path = String.format("%s/%s", p_entry.getRole().equals(NodeRole.PEER) ? PEER_DIR : SUPERPEER_DIR,
                NodeID.toHexStringShort(p_entry.getNodeID()));

        try {
            m_curatorClient.create().creatingParentsIfNeeded().forPath(path, buffer.array());
        } catch (Exception p_e) {
            throw new RuntimeException("Saving node entry failed", p_e);
        }
    }

    @Nullable
    private NodeEntry getBootstrapEntry() {
        byte[] bootBytes;

        try {
            bootBytes = m_curatorClient.getData().forPath(BOOTSTRAP_NODE_PATH);
        } catch (Exception p_e) {
            return null;
        }

        return NodeEntry.fromByteArray(bootBytes);
    }



    @Override
    protected boolean shutdownComponent() {
        m_shutdown = true;

        m_curatorClient.close();

        return true;
    }

    // -----------------------------------------------------------------------------------

    private void initializeBootstrapNode() throws Exception {

        LOGGER.info("Starting bootstrap process on node %s", m_ownAddress.getAddressStr());

        short nodeId = calculateNodeId();

        NodeEntry entry = new NodeEntry(m_ownAddress, nodeId, m_config.getRack(), m_config.getSwitch(), m_nodeRole, NodeCapabilities.NONE, false, false, true);

        ByteBuffer buffer = ByteBuffer.allocate(entry.sizeofObject());
        ByteBufferImExporter exporter = new ByteBufferImExporter(buffer);
        exporter.exportObject(entry);

        m_nodes.addNode(entry);
        m_nodes.setOwnNodeID(nodeId);

        // Save node information within peer/superpeer folder
        saveNodeEntry(entry);

        NodeRegistry.NodeDetails nodeDetails = buildNodeDetails(nodeId);

        m_nodeRegistry.start(nodeDetails);

        LOGGER.info("Started node registry");

        // Insert own node information into ZooKeeper so other nodes can find the bootstrap node
        m_curatorClient.create().creatingParentsIfNeeded().forPath(BOOTSTRAP_NODE_PATH, buffer.array());

        LOGGER.info("Assigned id 0x%04x to bootstrap node", nodeId);
    }

    private void assignCounterValue() {
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

        LOGGER.info("Assigned counter value %d to this node", m_counterValue);
    }

    private short calculateNodeId() {
        int seed = 1;
        short nodeId = 0;

        for(int i = 0; i < m_counterValue; i++) {
            nodeId = CRC16.continuousHash(seed, nodeId);
            seed++;
        }

        return nodeId;
    }

    private boolean isBootstrapNode() {
        return m_counterValue == BOOTSTRAP_COUNTER_VALUE;
    }

    /**
     * Replaces the current bootstrap with p_nodeID if the failed bootstrap has not been replaced by another superpeer
     *
     * @param p_nodeID The new bootstrap peer
     */
    private void setBootstrapPeer(final short p_nodeID) {
        NodeEntry newBootstrapEntry = getNodeEntry(p_nodeID);

        if (newBootstrapEntry == null) {
            LOGGER.warn("New bootstrap peer 0x%04X does not exist", p_nodeID);
            return;
        }

        Stat stat = null;

        try {
            stat = m_curatorClient.checkExists().forPath(BOOTSTRAP_NODE_PATH);
        } catch (Exception p_e) {
            // Ignored
        }

        int version = stat == null ? 0 : stat.getVersion();

        try {
            m_curatorClient.create().orSetData(version).forPath(BOOTSTRAP_NODE_PATH, newBootstrapEntry.toByteArray());
        } catch (Exception p_e) {
            LOGGER.warn("Setting new bootstrap node failed", p_e);
        }
    }

    @Override
    public void onPeerJoined(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Node %s joined the network", p_nodeDetails);
    }

    @Override
    public void onPeerLeft(NodeRegistry.NodeDetails p_nodeDetails) {

    }

    @Override
    public void onSuperpeerJoined(NodeRegistry.NodeDetails p_nodeDetails) {
        LOGGER.info("Node %s joined the network", p_nodeDetails);
    }

    @Override
    public void onSuperpeerLeft(NodeRegistry.NodeDetails p_nodeDetails) {

    }
}

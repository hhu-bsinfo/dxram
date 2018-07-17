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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
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
import de.hhu.bsinfo.dxram.util.ZooKeeperHandler.ZooKeeperException;
import de.hhu.bsinfo.dxutils.BloomFilter;
import de.hhu.bsinfo.dxutils.CRC16;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Implementation of the BootComponent interface with zookeeper.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
public class ZookeeperBootComponent extends AbstractBootComponent<ZookeeperBootComponentConfig>
        implements Watcher, EventListener<AbstractEvent> {
    // component dependencies
    private EventComponent m_event;
    private LookupComponent m_lookup;

    // private state
    private IPV4Unit m_ownAddress;
    private ZooKeeperHandler m_zookeeper;
    private short m_bootstrap = NodeID.INVALID_ID;
    private BloomFilter m_bloomFilter;

    private NodesConfiguration m_nodes;

    private volatile boolean m_isStarting;

    private boolean m_shutdown;

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
    public List<BackupPeer> getIDsOfAvailableBackupPeers() {
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
    public void putOnlineNodes(ArrayList<NodeEntry> p_onlineNodes) {
        for (NodeEntry entry : p_onlineNodes) {
            m_nodes.addNode(entry);
        }
    }

    @Override
    public List<Short> getIDsOfOnlineNodes() {
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
    public List<Short> getIDsOfOnlinePeers() {
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
    public List<Short> getIDsOfOnlineSuperpeers() {
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
    public short getNodeID() {
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
        // if bootstrap is not available (wrong startup order of superpeers and peers)
        return getIDsOfOnlineSuperpeers().size();
    }

    @Override
    public short getNodeIDBootstrap() {
        // if bootstrap is not available (wrong startup order of superpeers and peers)
        byte[] data = zookeeperGetData("nodes/bootstrap");
        if (data != null) {
            return Short.parseShort(new String(data));
        } else {
            return NodeID.INVALID_ID;
        }
    }

    @Override
    public boolean isNodeOnline(final short p_nodeID) {
        NodeEntry entry = m_nodes.getNode(p_nodeID);
        if (entry == null) {

            LOGGER.warn("Could not find node %s", NodeID.toHexString(p_nodeID));

            return false;
        }

        return entry.getStatus();
    }

    @Override
    public NodeRole getNodeRole(final short p_nodeID) {
        NodeEntry entry = m_nodes.getNode(p_nodeID);
        if (entry == null) {

            LOGGER.warn("Could not find node %s", NodeID.toHexString(p_nodeID));

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
    public InetSocketAddress getNodeAddress(final short p_nodeID) {
        NodeEntry entry = m_nodes.getNode(p_nodeID);
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
    public boolean nodeAvailable(final short p_nodeID) {
        return isNodeOnline(p_nodeID);
    }

    @Override
    public void singleNodeCleanup(final short p_nodeID, final NodeRole p_role) {
        Stat status;

        if (p_role == NodeRole.SUPERPEER) {
            // Remove superpeer
            if (!m_nodes.getNode(p_nodeID).readFromFile()) {
                // Enable re-usage of NodeID if failed superpeer was not in nodes file
                zookeeperCreate("node/free/" + p_nodeID);
            }
            LOGGER.debug("Removed superpeer 0x%X from zookeeper", p_nodeID);

            // Determine new bootstrap if failed superpeer is current one
            if (p_nodeID == m_bootstrap) {
                setBootstrapPeer(m_nodes.getOwnNodeID());

                LOGGER.debug("Failed node %s was bootstrap. New bootstrap is %s", NodeID.toHexString(p_nodeID),
                        NodeID.toHexString(m_bootstrap));

            }
        } else if (p_role == NodeRole.PEER) {
            // Remove peer
            if (!m_nodes.getNode(p_nodeID).readFromFile()) {
                // Enable re-usage of NodeID if failed peer was not in nodes file
                zookeeperCreate("node/free/" + p_nodeID);
            }
            LOGGER.debug("Removed peer 0x%X from zookeeper", p_nodeID);
        }

        if (!m_nodes.getNode(p_nodeID).readFromFile()) {
            try {
                // Remove node from "new nodes"
                status = zookeeperGetStatus("nodes/new/" + p_nodeID);
                if (status != null) {
                    zookeeperDelete("nodes/new/" + p_nodeID, status.getVersion());
                }
            } catch (final ZooKeeperException e) {
                // Entry was already deleted by another node
            }
        }

        m_nodes.getNode(p_nodeID).setStatus(false);
    }

    @Override
    public void process(final WatchedEvent p_event) {
        if (!m_shutdown) {
            if (p_event.getType() == Event.EventType.None && p_event.getState() == KeeperState.Expired) {

                LOGGER.error("ZooKeeper state expired");

            }
        }
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

        m_isStarting = false;

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
        NodeRole role = p_config.getEngineConfig().getRole();

        LOGGER.info("Initializing with address %s, role %s", m_ownAddress, role);

        m_event.registerListener(this, NodeFailureEvent.class);
        m_event.registerListener(this, NodeJoinEvent.class);

        m_zookeeper = new ZooKeeperHandler(getConfig().getPath(), getConfig().getConnection().getAddressStr(),
                (int) getConfig().getTimeout().getMs());
        m_isStarting = true;

        m_nodes = new NodesConfiguration();

        if (!parseNodes(getConfig().getNodesConfig(), role, getConfig().getRack(), getConfig().getSwitch())) {

            LOGGER.error("Parsing nodes failed");

            return false;
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_shutdown = true;

        if (m_lookup != null && m_lookup.isResponsibleForBootstrapCleanup()) {
            try {

                LOGGER.info("Cleaning-up ZooKeeper folder");

                m_zookeeper.close(true);
            } catch (final ZooKeeperException e) {

                LOGGER.error("Closing zookeeper failed", e);

            }
        } else {
            // LookupComponent has not been initialized or this node is not responsible for clean-up

            if (m_nodes.getOwnNodeEntry().getRole() == NodeRole.PEER) {
                // Remove own stuff from ZooKeeper for reboot
                singleNodeCleanup(m_nodes.getOwnNodeID(), NodeRole.PEER);
            }

            try {
                m_zookeeper.close(false);
            } catch (final ZooKeeperException e) {

                LOGGER.error("Closing zookeeper failed", e);

            }
        }

        return true;
    }

    // -----------------------------------------------------------------------------------

    /**
     * Replaces the current bootstrap with p_nodeID if the failed bootstrap has not been replaced by another superpeer
     *
     * @param p_nodeID
     *         the new bootstrap candidate
     */
    private void setBootstrapPeer(final short p_nodeID) {
        short currentBootstrap;
        Stat status;
        String entry;

        try {
            status = zookeeperGetStatus("nodes/bootstrap");
        } catch (final ZooKeeperException e) {
            // Entry should be available, even if another node updated the bootstrap first

            LOGGER.error("Getting status from zookeeper failed", e);

            return;
        }

        entry = new String(zookeeperGetData("nodes/bootstrap", status));
        currentBootstrap = Short.parseShort(entry);
        if (currentBootstrap == m_bootstrap) {
            try {
                if (!zookeeperSetData("nodes/bootstrap", String.valueOf(p_nodeID).getBytes(StandardCharsets.US_ASCII),
                        status.getVersion())) {
                    m_bootstrap = Short.parseShort(new String(zookeeperGetData("nodes/bootstrap")));
                } else {
                    m_bootstrap = p_nodeID;
                }
            } catch (final ZooKeeperException e) {
                // Entry was already updated by another node, try again
                setBootstrapPeer(p_nodeID);
            }
        } else {
            m_bootstrap = currentBootstrap;
        }
    }

    /**
     * Parses the configured nodes
     *
     * @param p_nodes
     *         the nodes to parse
     * @param p_cmdLineNodeRole
     *         the role from command line
     * @param p_cmdLineRack
     *         the rack this node is in (irrelevant for nodes in nodes file)
     * @param p_cmdLineSwitch
     *         the switch this node is connected to (irrelevant for nodes in nodes file)
     * @return the parsed nodes
     */
    private boolean parseNodes(final ArrayList<NodeEntry> p_nodes, final NodeRole p_cmdLineNodeRole,
            final short p_cmdLineRack, final short p_cmdLineSwitch) {
        boolean ret = false;
        String barrier;
        boolean parsed = false;

        m_bloomFilter = new BloomFilter((int) getConfig().getBitfieldSize().getBytes(), 65536);

        barrier = "barrier";

        try {
            if (!m_zookeeper.exists("nodes/bootstrap")) {
                try {
                    // Set barrier object
                    m_zookeeper.createBarrier(barrier);
                    if (p_cmdLineNodeRole != NodeRole.SUPERPEER) {

                        LOGGER.error("Bootstrap superpeer has differing command line NodeRole");

                        m_zookeeper.close(true);
                        return false;
                    }

                    if (p_nodes == null) {

                        LOGGER.error(
                                "Missing nodes configuration or reading nodes configuration from config file failed");

                        m_zookeeper.close(true);
                        return false;
                    }

                    // Load nodes routing information
                    ret = parseNodesBootstrap(p_nodes);
                    parsed = true;
                    // Delete barrier object
                    m_zookeeper.deleteBarrier(barrier);
                } catch (final ZooKeeperException | KeeperException | InterruptedException e) {
                    // Barrier does exist
                }
            }

            if (!parsed) {
                // normal node
                m_zookeeper.waitForBarrier(barrier, this);
                ret = parseNodesNormal(p_nodes, p_cmdLineNodeRole, p_cmdLineRack, p_cmdLineSwitch);
            }
        } catch (final ZooKeeperException e) {

            LOGGER.error("Could not access zookeeper while parsing nodes", e);

            return false;
        }

        return ret;
    }

    /**
     * Parses information from a nodes configuration object and creates routing information
     * in zookeeper. Also assigns valid node IDs and
     *
     * @param p_nodes
     *         the nodes to parse
     * @return whether parsing was successful or not
     * @note this method is called by bootstrap only
     */
    private boolean parseNodesBootstrap(final ArrayList<NodeEntry> p_nodes) {
        short nodeID;
        int seed;

        LOGGER.trace("Entering parseNodesBootstrap");

        try {
            if (!m_zookeeper.exists("nodes")) {
                m_zookeeper.create("nodes");
            }

            // Parse node information
            seed = 1;

            for (NodeEntry entry : p_nodes) {
                nodeID = CRC16.continuousHash(seed);
                while (m_bloomFilter.contains(nodeID) || nodeID == NodeID.INVALID_ID) {
                    nodeID = CRC16.continuousHash(++seed);
                }
                seed++;

                m_bloomFilter.add(nodeID);

                // assign own node entry
                if (m_ownAddress.equals(entry.getAddress())) {
                    m_nodes.setOwnNodeID(nodeID);
                    m_bootstrap = nodeID;

                    LOGGER.info("Own node assigned: %s", entry);

                }

                entry.setNodeID((short) (nodeID & 0x0000FFFF));
                m_nodes.addNode(entry);

                LOGGER.info("Node added: %s", entry);

            }

            if (!m_zookeeper.exists("nodes/new")) {
                m_zookeeper.create("nodes/new");
            }
            m_zookeeper.setChildrenWatch("nodes/new", this);

            if (!m_zookeeper.exists("nodes/free")) {
                m_zookeeper.create("nodes/free");
            }
            m_zookeeper.setChildrenWatch("nodes/free", this);

            // check if own node entry was correctly assigned to a valid node ID
            if (m_nodes.getOwnNodeEntry() == null) {

                LOGGER.error("Bootstrap entry for node in nodes configuration missing");

                m_zookeeper.close(true);
                return false;
            }

            // Register superpeer
            // register only if we are the superpeer. don't add node as superpeer
            if (m_nodes.getOwnNodeEntry().getRole() == NodeRole.SUPERPEER) {
                m_zookeeper.create("nodes/bootstrap", String.valueOf(m_bootstrap).getBytes(StandardCharsets.US_ASCII));
            }
        } catch (final ZooKeeperException | KeeperException | InterruptedException e) {

            LOGGER.error("Parsing nodes bootstrap failed", e);

            return false;
        }

        LOGGER.trace("Exiting parseNodesBootstrap");

        return true;
    }

    /**
     * Parses nodes.config and stores routing information in dxnet for nodes
     *
     * @param p_nodes
     *         the nodes to parse
     * @param p_cmdLineNodeRole
     *         the role from command line
     * @param p_cmdLineRack
     *         the rack this node is in (ignored for nodes in nodes file)
     * @param p_cmdLineSwitch
     *         the switch this node is connected to (ignored for nodes in nodes file)
     * @return whether parsing was successful or not
     * @note this method is called by every node except bootstrap
     */
    private boolean parseNodesNormal(final ArrayList<NodeEntry> p_nodes, final NodeRole p_cmdLineNodeRole,
            final short p_cmdLineRack,
            final short p_cmdLineSwitch) {
        short nodeID;
        int seed;
        String node;
        List<String> childs;

        String[] splits;

        LOGGER.trace("Entering parseNodesNormal");

        try {
            // Parse node information
            seed = 1;

            for (NodeEntry entry : p_nodes) {
                nodeID = CRC16.continuousHash(seed);
                while (m_bloomFilter.contains(nodeID) || nodeID == NodeID.INVALID_ID) {
                    nodeID = CRC16.continuousHash(++seed);
                }
                seed++;

                m_bloomFilter.add(nodeID);

                if (m_ownAddress.equals(entry.getAddress())) {
                    if (entry.getRole() != p_cmdLineNodeRole) {

                        LOGGER.error("NodeRole in configuration differs from command line given NodeRole: %s != %s",
                                entry.getRole(), p_cmdLineNodeRole);

                        return false;
                    }

                    m_nodes.setOwnNodeID(nodeID);
                    m_bootstrap = nodeID;

                    LOGGER.info("Own node assigned: %s", entry);

                }

                entry.setNodeID((short) (nodeID & 0x0000FFFF));
                m_nodes.addNode(entry);

                LOGGER.info("Node added: %s", entry);

            }

            m_bootstrap = Short.parseShort(new String(zookeeperGetData("nodes/bootstrap")));

            // Apply changes
            /*childs = m_zookeeper.getChildren("nodes/new");
            for (String child : childs) {
                nodeID = Short.parseShort(child);
                node = new String(zookeeperGetData("nodes/new/" + nodeID));
                m_bloomFilter.add(nodeID);

                // Set routing information for that node
                splits = node.split(":");

                m_nodes.addNode(
                        new NodeEntry(new IPV4Unit(splits[0], Integer.parseInt(splits[1])), nodeID,
                                Short.parseShort(splits[3]), Short.parseShort(splits[4]),
                                NodeRole.toNodeRole(splits[2]), false, true));

                if (nodeID == m_nodes.getOwnNodeID()) {
                    // NodeID was already re-used
                    m_nodes.setOwnNodeID(NodeID.INVALID_ID);
                }
            }*/

            if (m_nodes.getOwnNodeID() == NodeID.INVALID_ID) {
                // Add this node if it was not in start configuration

                LOGGER.warn("Node not in nodes.config (%s)", m_ownAddress);

                node = m_ownAddress + ":" + p_cmdLineNodeRole.getAcronym() + ':' + p_cmdLineRack + ':' +
                        p_cmdLineSwitch;

                childs = m_zookeeper.getChildren("nodes/free");
                if (!childs.isEmpty()) {
                    nodeID = Short.parseShort(childs.get(0));
                    m_nodes.setOwnNodeID(nodeID);
                    m_zookeeper.create("nodes/new/" + nodeID, node.getBytes(StandardCharsets.US_ASCII));
                    m_zookeeper.delete("nodes/free/" + nodeID);
                } else {
                    splits = m_ownAddress.getIP().split("\\.");
                    seed = (Integer.parseInt(splits[1]) << 16) + (Integer.parseInt(splits[2]) << 8) + Integer.parseInt(
                            splits[3]);
                    nodeID = CRC16.continuousHash(seed);
                    while (m_bloomFilter.contains(nodeID) || nodeID == NodeID.INVALID_ID) {
                        nodeID = CRC16.continuousHash(--seed);
                    }
                    m_bloomFilter.add(nodeID);
                    // Set own NodeID
                    m_nodes.setOwnNodeID(nodeID);
                    m_zookeeper.create("nodes/new/" + nodeID, node.getBytes(StandardCharsets.US_ASCII));
                }

                // Set routing information for that node
                //m_nodes.addNode(new NodeEntry(m_ownAddress, nodeID, p_cmdLineRack, p_cmdLineSwitch,
                //  p_cmdLineNodeRole, false, true));
            } else {
                // Remove NodeID if this node failed before
                nodeID = m_nodes.getOwnNodeID();
                if (m_zookeeper.exists("nodes/free/" + nodeID)) {
                    m_zookeeper.delete("nodes/free/" + nodeID);
                }
            }
            // Set watches
            m_zookeeper.setChildrenWatch("nodes/new", this);
            m_zookeeper.setChildrenWatch("nodes/free", this);
        } catch (final ZooKeeperException | KeeperException | InterruptedException e) {

            LOGGER.error("Parsing nodes normal failed", e);

            return false;
        }

        LOGGER.trace("Exiting parseNodesNormal");

        return true;
    }

    /**
     * Create a path in zookeeper.
     *
     * @param p_path
     *         Path to create.
     */
    private void zookeeperCreate(final String p_path) {
        try {
            m_zookeeper.create(p_path);
        } catch (final ZooKeeperException | KeeperException | InterruptedException e) {

            LOGGER.error("Creating path in zookeeper failed", e);

        }
    }

    /**
     * Get the status of a path.
     *
     * @param p_path
     *         Path to get the status of.
     * @return Status of the path.
     * @throws ZooKeeperException
     *         if status could not be gotten
     */
    private Stat zookeeperGetStatus(final String p_path) throws ZooKeeperException {
        return m_zookeeper.getStatus(p_path);
    }

    /**
     * Delete a path in zookeeper.
     *
     * @param p_path
     *         Path to delete.
     * @param p_version
     *         Version of the path to delete.
     * @throws ZooKeeperException
     *         if deletion failed
     */
    private void zookeeperDelete(final String p_path, final int p_version) throws ZooKeeperException {
        m_zookeeper.delete(p_path, p_version);
    }

    /**
     * Get data from a path.
     *
     * @param p_path
     *         Path to get the data of.
     * @return Data stored with the path.
     */
    private byte[] zookeeperGetData(final String p_path) {
        byte[] data = null;

        try {
            data = m_zookeeper.getData(p_path);
        } catch (final ZooKeeperException e) {

            LOGGER.error("Getting data from zookeeper failed", e);

        }

        return data;
    }

    /**
     * Get data from a path.
     *
     * @param p_path
     *         Path to get the data of.
     * @param p_status
     *         Status of the node.
     * @return Data from the path.
     */
    private byte[] zookeeperGetData(final String p_path, final Stat p_status) {
        byte[] data = null;

        try {
            data = m_zookeeper.getData(p_path, p_status);
        } catch (final ZooKeeperException e) {

            LOGGER.error("Getting data from zookeeper failed", e);

        }

        return data;
    }

    /**
     * Set data for a path.
     *
     * @param p_path
     *         Path to set the data for.
     * @param p_data
     *         Data to set.
     * @param p_version
     *         Version of the path.
     * @return True if successful, false otherwise.
     * @throws ZooKeeperException
     *         if data could not be set
     */
    private boolean zookeeperSetData(final String p_path, final byte[] p_data, final int p_version)
            throws ZooKeeperException {
        m_zookeeper.setData(p_path, p_data, p_version);
        return true;
    }

    /**
     * Check if a path exists.
     *
     * @param p_path
     *         Path to check.
     * @return True if exists, false otherwise.
     */
    private boolean zookeeperPathExists(final String p_path) {
        boolean ret = false;

        try {
            ret = m_zookeeper.exists(p_path);
        } catch (final ZooKeeperException e) {

            LOGGER.error("Checking if path exists in zookeeper failed", e);

        }

        return ret;
    }
}

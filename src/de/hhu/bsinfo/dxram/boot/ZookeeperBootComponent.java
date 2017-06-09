/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.boot;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.NodesConfiguration.NodeEntry;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.BloomFilter;
import de.hhu.bsinfo.utils.CRC16;
import de.hhu.bsinfo.utils.NodeID;
import de.hhu.bsinfo.utils.ZooKeeperHandler;
import de.hhu.bsinfo.utils.ZooKeeperHandler.ZooKeeperException;
import de.hhu.bsinfo.utils.unit.IPV4Unit;

/**
 * Implementation of the BootComponent interface with zookeeper.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class ZookeeperBootComponent extends AbstractBootComponent<ZookeeperBootComponentConfig> implements Watcher {
    // component dependencies
    private BackupComponent m_backup;
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
        super(DXRAMComponentOrder.Init.BOOT, DXRAMComponentOrder.Shutdown.BOOT);
    }

    @Override
    public List<Short> getIDsOfOnlineNodes() {
        // TODO: Don't use ZooKeeper for this

        List<Short> ids = new ArrayList<>();

        if (zookeeperPathExists("nodes/superpeers")) {
            try {
                List<String> children = m_zookeeper.getChildren("nodes/superpeers");
                for (String child : children) {
                    ids.add(Short.parseShort(child));
                }
            } catch (final ZooKeeperException ignored) {
            }
        }
        if (zookeeperPathExists("nodes/peers")) {
            try {
                List<String> children = m_zookeeper.getChildren("nodes/peers");
                for (String child : children) {
                    ids.add(Short.parseShort(child));
                }
            } catch (final ZooKeeperException ignored) {
            }
        }
        if (zookeeperPathExists("nodes/terminals")) {
            try {
                List<String> children = m_zookeeper.getChildren("nodes/terminals");
                for (String child : children) {
                    ids.add(Short.parseShort(child));
                }
            } catch (final ZooKeeperException ignored) {
            }
        }

        return ids;
    }

    @Override
    public List<Short> getIDsOfOnlinePeers() {
        // TODO: Don't use ZooKeeper for this

        short childID;
        List<Short> ids = new ArrayList<>();

        if (zookeeperPathExists("nodes/peers")) {
            try {
                List<String> children = m_zookeeper.getChildren("nodes/peers");
                for (String child : children) {
                    childID = Short.parseShort(child);
                    if (childID != getNodeID()) {
                        ids.add(Short.parseShort(child));
                    }
                }
            } catch (final ZooKeeperException ignored) {
            }
        }

        return ids;
    }

    @Override
    public List<Short> getIDsOfAvailableBackupPeers() {
        // TODO: Don't use ZooKeeper for this

        short childID;
        List<Short> ids = new ArrayList<>();

        if (zookeeperPathExists("nodes/peers")) {
            try {
                List<String> children = m_zookeeper.getChildren("nodes/peers");
                for (String child : children) {
                    childID = Short.parseShort(child);
                    if (childID != getNodeID()) {
                        // Add peer if it is available for backup (1), only
                        if (zookeeperGetData("nodes/peers/" + childID)[0] == (byte) 1) {
                            ids.add(Short.parseShort(child));
                        }
                    }
                }
            } catch (final ZooKeeperException ignored) {
            }
        }

        return ids;
    }

    @Override
    public List<Short> getIDsOfOnlineSuperpeers() {
        // TODO: Don't use ZooKeeper for this

        short childID;
        List<Short> ids = new ArrayList<>();

        if (zookeeperPathExists("nodes/superpeers")) {
            try {
                List<String> children = m_zookeeper.getChildren("nodes/superpeers");
                for (String child : children) {
                    childID = Short.parseShort(child);
                    if (childID != getNodeID()) {
                        ids.add(Short.parseShort(child));
                    }
                }
            } catch (final ZooKeeperException ignored) {
            }
        }

        return ids;
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
    public int getNumberOfAvailableSuperpeers() {
        // if bootstrap is not available (wrong startup order of superpeers and peers)
        byte[] data = zookeeperGetData("nodes/superpeers");
        if (data != null) {
            return Integer.parseInt(new String(data));
        } else {
            return 0;
        }
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
        if (zookeeperPathExists("nodes/superpeers")) {
            try {
                List<String> children = m_zookeeper.getChildren("nodes/superpeers");
                for (String child : children) {
                    if (p_nodeID == Short.parseShort(child)) {
                        return true;
                    }
                }
            } catch (final ZooKeeperException ignored) {
            }
        }
        if (zookeeperPathExists("nodes/peers")) {
            try {
                List<String> children = m_zookeeper.getChildren("nodes/peers");
                for (String child : children) {
                    if (p_nodeID == Short.parseShort(child)) {
                        return true;
                    }
                }
            } catch (final ZooKeeperException ignored) {
            }
        }
        if (zookeeperPathExists("nodes/terminals")) {
            try {
                List<String> children = m_zookeeper.getChildren("nodes/terminals");
                for (String child : children) {
                    if (p_nodeID == Short.parseShort(child)) {
                        return true;
                    }
                }
            } catch (final ZooKeeperException ignored) {
            }
        }

        return false;
    }

    @Override
    public NodeRole getNodeRole(final short p_nodeID) {
        NodeEntry entry = m_nodes.getNode(p_nodeID);
        if (entry == null) {
            // #if LOGGER >= WARN
            LOGGER.warn("Could not find node role for %s", NodeID.toHexString(p_nodeID));
            // #endif /* LOGGER >= WARN */
            return null;
        }

        return entry.getRole();
    }

    @Override
    public InetSocketAddress getNodeAddress(final short p_nodeID) {
        NodeEntry entry = m_nodes.getNode(p_nodeID);
        InetSocketAddress address;
        // return "proper" invalid address if entry does not exist
        if (entry == null) {
            // #if LOGGER >= WARN
            LOGGER.warn("Could not find ip and port for node id %s", NodeID.toHexString(p_nodeID));
            // #endif /* LOGGER >= WARN */
            address = new InetSocketAddress("255.255.255.255", 0xFFFF);
        } else {
            address = entry.getAddress().getInetSocketAddress();
        }

        return address;
    }

    @Override
    public boolean nodeAvailable(final short p_nodeID) {
        return zookeeperPathExists("nodes/superpeers/" + p_nodeID) || zookeeperPathExists("nodes/peers/" + p_nodeID) ||
                zookeeperPathExists("nodes/terminals/" + p_nodeID);
    }

    @Override
    public void singleNodeCleanup(final short p_nodeID, final NodeRole p_role) {
        Stat status;

        if (p_role == NodeRole.SUPERPEER) {
            try {
                // Remove superpeer
                status = zookeeperGetStatus("nodes/superpeers/" + p_nodeID);
                if (status != null) {
                    zookeeperDelete("nodes/superpeers/" + p_nodeID, status.getVersion());
                    if (!m_nodes.getNode(p_nodeID).readFromFile()) {
                        // Enable re-usage of NodeID if failed superpeer was not in nodes file
                        zookeeperCreate("node/free/" + p_nodeID);
                    }
                    LOGGER.debug("Removed superpeer 0x%X from zookeeper", p_nodeID);
                }
            } catch (final ZooKeeperException e) {
                // Entry was already deleted by another node
            }

            // Determine new bootstrap if failed superpeer is current one
            if (p_nodeID == m_bootstrap) {
                setBootstrapPeer(m_nodes.getOwnNodeID());

                // #if LOGGER >= DEBUG
                LOGGER.debug("Failed node %s was bootstrap. New bootstrap is %s", NodeID.toHexString(p_nodeID), NodeID.toHexString(m_bootstrap));
                // #endif /* LOGGER >= DEBUG */

            }
        } else if (p_role == NodeRole.PEER) {
            try {
                // Remove peer
                status = zookeeperGetStatus("nodes/peers/" + p_nodeID);
                if (status != null) {
                    // Remove all children if there are any (currently not unused)
                    List<String> children = m_zookeeper.getChildren("nodes/peers/" + p_nodeID, this);
                    for (String child : children) {
                        Stat stat = zookeeperGetStatus("nodes/peers/" + p_nodeID + '/' + child);
                        if (stat != null) {
                            zookeeperDelete("nodes/peers/" + p_nodeID + '/' + child, stat.getVersion());
                        }
                    }

                    zookeeperDelete("nodes/peers/" + p_nodeID, status.getVersion());
                    if (!m_nodes.getNode(p_nodeID).readFromFile()) {
                        // Enable re-usage of NodeID if failed peer was not in nodes file
                        zookeeperCreate("node/free/" + p_nodeID);
                    }
                    LOGGER.debug("Removed peer 0x%X from zookeeper", p_nodeID);
                }
            } catch (final ZooKeeperException e) {
                // Entry was already deleted by another node
            }
        } else {
            try {
                // Remove terminal
                status = zookeeperGetStatus("nodes/terminals/" + p_nodeID);
                if (status != null) {
                    zookeeperDelete("nodes/terminals/" + p_nodeID, status.getVersion());
                    LOGGER.debug("Removed terminal 0x%X from zookeeper", p_nodeID);
                }
            } catch (final ZooKeeperException e) {
                // Entry was already deleted by another node
            }
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

        // TODO: Remove failed node from nodes configuration?
        // m_nodesConfiguration.removeNode(p_nodeID);
    }

    @Override
    public void process(final WatchedEvent p_event) {
        String path;
        String prefix;

        List<String> children;
        short nodeID;
        NodeRole role;
        String node;
        String[] splits;

        if (!m_shutdown) {
            if (p_event.getType() == Event.EventType.None && p_event.getState() == KeeperState.Expired) {
                // #if LOGGER >= ERROR
                LOGGER.error("ZooKeeper state expired");
                // #endif /* LOGGER >= ERROR */
            } else {
                try {
                    path = p_event.getPath();
                    prefix = m_zookeeper.getPath() + '/';
                    while (m_isStarting) {
                        try {
                            Thread.sleep(100);
                        } catch (final InterruptedException ignored) {
                        }
                    }
                    if (path != null) {
                        if (path.equals(prefix + "nodes/new")) {

                            children = m_zookeeper.getChildren("nodes/new", this);
                            for (String child : children) {
                                nodeID = Short.parseShort(child);
                                node = new String(zookeeperGetData("nodes/new/" + nodeID));
                                splits = node.split(":");

                                role = NodeRole.toNodeRole(splits[2]);

                                m_nodes.addNode(nodeID, new NodeEntry(new IPV4Unit(splits[0], Integer.parseInt(splits[1])), (short) 0, (short) 0, role, false));
                            }
                        }
                    }
                } catch (final ZooKeeperException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Could not access ZooKeeper", e);
                    // #endif /* LOGGER >= ERROR */
                }
            }
        }
    }

    //@Override
    //public void eventTriggered(final NodeFailureEvent p_event) {
    // TODO: Remove failed node from nodes configuration?
    // m_nodesConfiguration.removeNode(p_event.getNodeID());
    //}

    @Override
    public boolean finishInitComponent() {
        // Register peer/superpeer (a terminal node is not registered to exclude it from backup)
        try {
            if (m_nodes.getOwnNodeEntry().getRole() == NodeRole.SUPERPEER) {
                m_zookeeper.create("nodes/superpeers/" + m_nodes.getOwnNodeID());
            } else if (m_nodes.getOwnNodeEntry().getRole() == NodeRole.PEER) {
                if (m_backup.isActiveAndAvailableForBackup()) {
                    m_zookeeper.create("nodes/peers/" + m_nodes.getOwnNodeID(), new byte[] {1});
                } else {
                    m_zookeeper.create("nodes/peers/" + m_nodes.getOwnNodeID(), new byte[] {0});
                }
            }
        } catch (ZooKeeperException | KeeperException | InterruptedException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Node could not be registered in ZooKeeper. Restart recommendable as other nodes might not find it.");
            // #endif /* LOGGER >= ERROR */
        }

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
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_event = p_componentAccessor.getComponent(EventComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {
        m_ownAddress = p_config.getEngineConfig().getAddress();
        NodeRole role = p_config.getEngineConfig().getRole();

        // #if LOGGER >= INFO
        LOGGER.info("Initializing with address %s, role %s", m_ownAddress, role);
        // #endif /* LOGGER >= INFO */

        m_zookeeper = new ZooKeeperHandler(getConfig().getPath(), getConfig().getConnection().getAddressStr(), (int) getConfig().getTimeout().getMs());
        m_isStarting = true;

        m_nodes = new NodesConfiguration();

        if (!parseNodes(getConfig().getNodesConfig(), role)) {
            // #if LOGGER >= ERROR
            LOGGER.error("Parsing nodes failed");
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_shutdown = true;

        if (m_lookup != null && m_lookup.isResponsibleForBootstrapCleanup()) {
            try {
                // #if LOGGER >= INFO
                LOGGER.info("Cleaning-up ZooKeeper folder");
                // #endif /* LOGGER >= INFO */

                m_zookeeper.close(true);
            } catch (final ZooKeeperException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Closing zookeeper failed", e);
                // #endif /* LOGGER >= ERROR */
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
                // #if LOGGER >= ERROR
                LOGGER.error("Closing zookeeper failed", e);
                // #endif /* LOGGER >= ERROR */
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

            // #if LOGGER >= ERROR
            LOGGER.error("Getting status from zookeeper failed", e);
            // #endif /* LOGGER >= ERROR */

            return;
        }

        entry = new String(zookeeperGetData("nodes/bootstrap", status));
        currentBootstrap = Short.parseShort(entry);
        if (currentBootstrap == m_bootstrap) {
            try {
                if (!zookeeperSetData("nodes/bootstrap", String.valueOf(p_nodeID).getBytes(), status.getVersion())) {
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
     * @return the parsed nodes
     */
    private boolean parseNodes(final ArrayList<NodeEntry> p_nodes, final NodeRole p_cmdLineNodeRole) {
        boolean ret = false;
        String barrier;
        boolean parsed = false;

        m_bloomFilter = new BloomFilter((int) getConfig().getZookeeperBitfieldSize().getBytes(), 65536);

        barrier = "barrier";

        try {
            if (!m_zookeeper.exists("nodes/bootstrap")) {
                try {
                    // Set barrier object
                    m_zookeeper.createBarrier(barrier);
                    if (p_cmdLineNodeRole != NodeRole.SUPERPEER) {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Bootstrap superpeer has differing command line NodeRole");
                        // #endif /* LOGGER >= ERROR */
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
                ret = parseNodesNormal(p_nodes, p_cmdLineNodeRole);
            }
        } catch (final ZooKeeperException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not access zookeeper while parsing nodes", e);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        m_isStarting = false;
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
        int numberOfSuperpeers;
        int seed;

        // #if LOGGER == TRACE
        LOGGER.trace("Entering parseNodesBootstrap");
        // #endif /* LOGGER == TRACE */

        try {
            if (!m_zookeeper.exists("nodes")) {
                m_zookeeper.create("nodes");
            }

            // Parse node information
            numberOfSuperpeers = 0;
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
                    // #if LOGGER >= INFO
                    LOGGER.info("Own node assigned: %s", entry);
                    // #endif /* LOGGER >= INFO */
                }
                if (entry.getRole() == NodeRole.SUPERPEER) {
                    numberOfSuperpeers++;
                }

                m_nodes.addNode((short) (nodeID & 0x0000FFFF), entry);
                // #if LOGGER >= INFO
                LOGGER.info("Node added: %s", entry);
                // #endif /* LOGGER >= INFO */
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
                // #if LOGGER >= ERROR
                LOGGER.error("Bootstrap entry for node in nodes configuration missing");
                // #endif /* LOGGER >= ERROR */
                m_zookeeper.close(true);
                return false;
            }

            // set default/invalid data
            if (!m_zookeeper.exists("nodes/peers")) {
                m_zookeeper.create("nodes/peers");
            }
            if (!m_zookeeper.exists("nodes/superpeers")) {
                m_zookeeper.create("nodes/superpeers", String.valueOf(numberOfSuperpeers).getBytes());
            } else {
                m_zookeeper.setData("nodes/superpeers", String.valueOf(numberOfSuperpeers).getBytes());
            }
            if (!m_zookeeper.exists("nodes/terminals")) {
                m_zookeeper.create("nodes/terminals");
            }

            // Register superpeer
            // register only if we are the superpeer. don't add node as superpeer
            if (m_nodes.getOwnNodeEntry().getRole() == NodeRole.SUPERPEER) {
                m_zookeeper.create("nodes/bootstrap", String.valueOf(m_bootstrap).getBytes());
            }
        } catch (final ZooKeeperException | KeeperException | InterruptedException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Parsing nodes bootstrap failed", e);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting parseNodesBootstrap");
        // #endif /* LOGGER == TRACE */

        return true;
    }

    /**
     * Parses nodes.config and stores routing information in net for nodes
     *
     * @param p_nodes
     *         the nodes to parse
     * @param p_cmdLineNodeRole
     *         the role from command line
     * @return whether parsing was successful or not
     * @note this method is called by every node except bootstrap
     */
    private boolean parseNodesNormal(final ArrayList<NodeEntry> p_nodes, final NodeRole p_cmdLineNodeRole) {
        short nodeID;
        int seed;
        String node;
        List<String> childs;

        String[] splits;

        // #if LOGGER == TRACE
        LOGGER.trace("Entering parseNodesNormal");
        // #endif /* LOGGER == TRACE */

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
                        // #if LOGGER >= ERROR
                        LOGGER.error("NodeRole in configuration differs from command line given NodeRole: %s != %s", entry.getRole(), p_cmdLineNodeRole);
                        // #endif /* LOGGER >= ERROR */
                        return false;
                    }

                    m_nodes.setOwnNodeID(nodeID);
                    m_bootstrap = nodeID;
                    // #if LOGGER >= INFO
                    LOGGER.info("Own node assigned: %s", entry);
                    // #endif /* LOGGER >= INFO */
                }

                m_nodes.addNode((short) (nodeID & 0x0000FFFF), entry);
                // #if LOGGER >= INFO
                LOGGER.info("Node added: %s", entry);
                // #endif /* LOGGER >= INFO */
            }

            m_bootstrap = Short.parseShort(new String(zookeeperGetData("nodes/bootstrap")));

            // Apply changes
            childs = m_zookeeper.getChildren("nodes/new");
            for (String child : childs) {
                nodeID = Short.parseShort(child);
                node = new String(zookeeperGetData("nodes/new/" + nodeID));
                m_bloomFilter.add(nodeID);

                // Set routing information for that node
                splits = node.split(":");

                m_nodes.addNode(nodeID,
                        new NodeEntry(new IPV4Unit(splits[0], Integer.parseInt(splits[1])), (short) 0, (short) 0, NodeRole.toNodeRole(splits[2]), false));

                if (nodeID == m_nodes.getOwnNodeID()) {
                    // NodeID was already re-used
                    m_nodes.setOwnNodeID(NodeID.INVALID_ID);
                }
            }

            if (m_nodes.getOwnNodeID() == NodeID.INVALID_ID) {
                // Add this node if it was not in start configuration
                // #if LOGGER >= WARN
                LOGGER.warn("Node not in nodes.config (%s)", m_ownAddress);
                // #endif /* LOGGER >= WARN */

                node = m_ownAddress + ":" + p_cmdLineNodeRole.getAcronym() + ':' + 0 + ':' + 0;

                childs = m_zookeeper.getChildren("nodes/free");
                if (!childs.isEmpty()) {
                    nodeID = Short.parseShort(childs.get(0));
                    m_nodes.setOwnNodeID(nodeID);
                    m_zookeeper.create("nodes/new/" + nodeID, node.getBytes());
                    m_zookeeper.delete("nodes/free/" + nodeID);
                } else {
                    splits = m_ownAddress.getIP().split("\\.");
                    seed = ((Integer.parseInt(splits[1]) << 16) + (Integer.parseInt(splits[2]) << 8) + Integer.parseInt(splits[3])) * -1;
                    nodeID = CRC16.continuousHash(seed);
                    while (m_bloomFilter.contains(nodeID) || nodeID == NodeID.INVALID_ID) {
                        nodeID = CRC16.continuousHash(--seed);
                    }
                    m_bloomFilter.add(nodeID);
                    // Set own NodeID
                    m_nodes.setOwnNodeID(nodeID);
                    m_zookeeper.create("nodes/new/" + nodeID, node.getBytes());
                }

                // Set routing information for that node
                m_nodes.addNode(nodeID, new NodeEntry(m_ownAddress, (short) 0, (short) 0, p_cmdLineNodeRole, false));
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
            // #if LOGGER >= ERROR
            LOGGER.error("Parsing nodes normal failed", e);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting parseNodesNormal");
        // #endif /* LOGGER == TRACE */

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
            // #if LOGGER >= ERROR
            LOGGER.error("Creating path in zookeeper failed", e);
            // #endif /* LOGGER >= ERROR */
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
            // #if LOGGER >= ERROR
            LOGGER.error("Getting data from zookeeper failed", e);
            // #endif /* LOGGER >= ERROR */
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
            // #if LOGGER >= ERROR
            LOGGER.error("Getting data from zookeeper failed", e);
            // #endif /* LOGGER >= ERROR */
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
    private boolean zookeeperSetData(final String p_path, final byte[] p_data, final int p_version) throws ZooKeeperException {
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
            // #if LOGGER >= ERROR
            LOGGER.error("Checking if path exists in zookeeper failed", e);
            // #endif /* LOGGER >= ERROR */
        }

        return ret;
    }
}

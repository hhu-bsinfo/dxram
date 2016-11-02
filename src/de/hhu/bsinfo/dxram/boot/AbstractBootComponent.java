package de.hhu.bsinfo.dxram.boot;

import java.net.InetSocketAddress;
import java.util.List;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Component executing the bootstrapping of a node in DXRAM.
 * It takes care of assigning the node ID to this node, its role and
 * managing everything related to the basic node status (available,
 * failure report...)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public abstract class AbstractBootComponent extends AbstractDXRAMComponent {

    /**
     * Constructor
     *
     * @param p_priorityInit
     *     Default init priority for this component
     * @param p_priorityShutdown
     *     Default shutdown priority for this component
     */
    public AbstractBootComponent(final short p_priorityInit, final short p_priorityShutdown) {
        super(p_priorityInit, p_priorityShutdown);
    }

    /**
     * Check if a specific node is online.
     *
     * @param p_nodeID
     *     Node to check.
     * @return True if online, false offline.
     */
    public abstract boolean isNodeOnline(short p_nodeID);

    /**
     * Get IDs of all available (online) nodes including the own.
     *
     * @return List of IDs of nodes available.
     */
    public abstract List<Short> getIDsOfOnlineNodes();

    /**
     * Get IDs of all available (online) peer nodes except the own.
     *
     * @return List of IDs of nodes available without own ID.
     */
    public abstract List<Short> getIDsOfOnlinePeers();

    /**
     * Get IDs of all available (online) superpeer nodes except the own.
     *
     * @return List of IDs of nodes available without own ID.
     */
    public abstract List<Short> getIDsOfOnlineSuperpeers();

    /**
     * Get the node ID, which is currently assigned to this running instance.
     *
     * @return Own NodeID.
     */
    public abstract short getNodeID();

    /**
     * Get the role, which is currently assigned to this running instance.
     *
     * @return Own Role.
     */
    public abstract NodeRole getNodeRole();

    /**
     * Get the role of another nodeID.
     *
     * @param p_nodeID
     *     Node id of the node.
     * @return Role of other nodeID or null if node does not exist.
     */
    public abstract NodeRole getNodeRole(short p_nodeID);

    /**
     * Get the IP and port of another node.
     *
     * @param p_nodeID
     *     Node ID of the node.
     * @return IP and port of the specified node or an invalid address if not available.
     */
    public abstract InetSocketAddress getNodeAddress(short p_nodeID);

    /**
     * Get the number of currently available superpeers.
     *
     * @return Number of currently available superpeers.
     */
    public abstract int getNumberOfAvailableSuperpeers();

    /**
     * Get the node ID of the currently set bootstrap node.
     *
     * @return Node ID assigned for bootstrapping or -1 if no bootstrap assigned/available.
     */
    public abstract short getNodeIDBootstrap();

    /**
     * Check if a node is available/exists.
     *
     * @param p_nodeID
     *     Node ID to check.
     * @return True if available, false otherwise.
     */
    public abstract boolean nodeAvailable(short p_nodeID);

    /**
     * Report that we detected a node failure.
     *
     * @param p_nodeID
     *     the failed node
     * @param p_role
     *     failed node's role
     */
    public abstract void failureHandling(short p_nodeID, NodeRole p_role);
}

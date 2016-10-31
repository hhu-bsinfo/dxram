package de.hhu.bsinfo.ethnet;

/**
 * Exception if the target destination is unreachable
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.10.2016
 */
public class NetworkDestinationUnreachableException extends NetworkException {

    /**
     * Network Destination Unreachable Exception
     *
     * @param p_nodeId
     *         the NodeID of the unreachable node
     */
    public NetworkDestinationUnreachableException(final short p_nodeId) {
        super("Destination node " + NodeID.toHexString(p_nodeId) + " unreachable");
    }
}

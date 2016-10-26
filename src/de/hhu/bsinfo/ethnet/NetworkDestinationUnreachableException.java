package de.hhu.bsinfo.ethnet;

/**
 * Exception if the target destination is unreachable
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.10.16
 */
public class NetworkDestinationUnreachableException extends NetworkException {

	public NetworkDestinationUnreachableException(final short p_nodeId) {
		super("Destination node " + NodeID.toHexString(p_nodeId) + " unreachable");
	}
}

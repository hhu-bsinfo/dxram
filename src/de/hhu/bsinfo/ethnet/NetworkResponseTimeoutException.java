package de.hhu.bsinfo.ethnet;

/**
 * Exception if a request was sent but the response was not delivered in time.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.10.16
 */
public class NetworkResponseTimeoutException extends NetworkException {

	public NetworkResponseTimeoutException(final short p_nodeId) {
		super("Waiting for response from node " + NodeID.toHexString(p_nodeId) + " failed, timeout");
	}
}

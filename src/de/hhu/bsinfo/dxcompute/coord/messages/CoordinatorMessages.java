
package de.hhu.bsinfo.dxcompute.coord.messages;

/**
 * Different message types used for coordination.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 */
public class CoordinatorMessages {
	public static final byte TYPE = 40;
	public static final byte SUBTYPE_MASTER_BROADCAST_MESSAGE = 1;
	public static final byte SUBTYPE_SLAVE_SIGN_ON_MESSAGE = 2;
	public static final byte SUBTYPE_MASTER_SYNC_BARRIER_BROADCAST = 1;
	public static final byte SUBTYPE_SLAVE_SYNC_BARRIER_SIGN_ON = 2;
	public static final byte SUBTYPE_MASTER_SYNC_BARRIER_RELEASE = 3;
	public static final byte SUBTYPE_SLAVE_JOIN_REQUEST = 4;
	public static final byte SUBTYPE_SLAVE_JOIN_RESPONSE = 5;
	public static final byte SUBTYPE_START_EXECUTION_MESSAGE = 6;
}

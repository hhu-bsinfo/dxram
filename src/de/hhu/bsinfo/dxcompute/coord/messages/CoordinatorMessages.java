
package de.hhu.bsinfo.dxcompute.coord.messages;

/**
 * Different message types used for coordination.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 */
public final class CoordinatorMessages {
	public static final byte TYPE = 40;
	public static final byte SUBTYPE_BARRIER_SLAVE_SIGN_ON_REQUEST = 1;
	public static final byte SUBTYPE_BARRIER_SLAVE_SIGN_ON_RESPONSE = 2;
	public static final byte SUBTYPE_BARRIER_MASTER_RELEASE = 3;

	/**
	 * Static class
	 */
	private CoordinatorMessages() {};
}

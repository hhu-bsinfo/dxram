package de.hhu.bsinfo.dxram.lookup.overlay;

/**
 * Created by nothaas on 5/3/16.
 */
public class BarrierID {
	public static final int INVALID_ID = -1;

	public static final int MAX_ID = 65535;

	public static int createBarrierId(final short p_nodeId, final int p_id) {
		return (((int) (p_nodeId & 0xFFFF)) << 16) | (p_id & 0xFFFF);
	}

	public static short getOwnerID(final int p_barrierId) {
		assert p_barrierId != INVALID_ID;
		return (short) (p_barrierId >> 16);
	}

	public static int getBarrierID(final int p_barrierId) {
		assert p_barrierId != INVALID_ID;
		return (short) (p_barrierId & 0xFFFF);
	}

	public static String toHexString(final int p_barrierId) {
		return "0x" + String.format("%08x", p_barrierId).toUpperCase();
	}
}

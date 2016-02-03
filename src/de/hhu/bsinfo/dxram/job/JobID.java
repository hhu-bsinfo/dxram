package de.hhu.bsinfo.dxram.job;

/**
 * Helper class to work with job IDs.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 *
 */
public class JobID {
	public static final long INVALID_ID = -1;
	
	public static final long CREATORID_BITMASK = 0xFFFF000000000000L;
	public static final long LOCALID_BITMASK = 0x0000FFFFFFFFFFFFL;

	public static final long MAX_LOCALID = Long.MAX_VALUE & LOCALID_BITMASK;

	/**
	 * Static class.
	 */
	private JobID() {}

	/**
	 * Get the CreatorID/NodeID part of the JobID.
	 * @param p_jobID JobID.
	 * @return The NodeID/CreatorID part.
	 */
	public static short getCreatorID(final long p_jobID) {
		assert p_jobID != INVALID_ID;

		return (short) ((p_jobID & CREATORID_BITMASK) >> 48);
	}

	/**
	 * Get the LocalID part of the JobID
	 * @param p_jobID
	 *            the JobID
	 * @return the LocalID part
	 */
	public static long getLocalID(final long p_jobID) {
		assert p_jobID != INVALID_ID;

		return p_jobID & LOCALID_BITMASK;
	}
}

package de.hhu.bsinfo.dxram.job.messages;

/**
 * Different message types used by the job package.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public class JobMessages {
	public static final byte TYPE = 70;
	public static final byte SUBTYPE_PUSH_JOB_QUEUE_REQUEST = 1;
	public static final byte SUBTYPE_PUSH_JOB_QUEUE_RESPONSE = 2;
	public static final byte SUBTYPE_STATUS_REQUEST = 3;
	public static final byte SUBTYPE_STATUS_RESPONSE = 4;
}

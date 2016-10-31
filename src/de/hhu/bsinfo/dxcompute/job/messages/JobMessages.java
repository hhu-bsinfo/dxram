
package de.hhu.bsinfo.dxcompute.job.messages;

/**
 * Different message types used by the job package.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public final class JobMessages {
    public static final byte SUBTYPE_PUSH_JOB_QUEUE_MESSAGE = 1;
    public static final byte SUBTYPE_STATUS_REQUEST = 2;
    public static final byte SUBTYPE_STATUS_RESPONSE = 3;
    public static final byte SUBTYPE_JOB_EVENT_TRIGGERED_MESSAGE = 4;

    /**
     * Static class
     */
    private JobMessages() {
    }
}

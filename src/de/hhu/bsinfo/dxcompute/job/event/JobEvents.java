
package de.hhu.bsinfo.dxcompute.job.event;

/**
 * List of available job events.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public final class JobEvents {
    public static final byte MS_JOB_SCHEDULED_FOR_EXECUTION_EVENT_ID = 1 << 0;
    public static final byte MS_JOB_STARTED_EXECUTION_EVENT_ID = 1 << 1;
    public static final byte MS_JOB_FINISHED_EXECUTION_EVENT_ID = 1 << 2;

    /**
     * Static class
     */
    private JobEvents() {
    }

}

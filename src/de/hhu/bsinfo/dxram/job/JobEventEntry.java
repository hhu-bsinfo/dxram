package de.hhu.bsinfo.dxram.job;

/**
 * Entry for job event handling
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 10.04.2017
 */
public class JobEventEntry {
    private byte m_eventId;
    private AbstractJob m_job;

    /**
     * Constructor
     *
     * @param p_eventId
     *     Event id
     * @param p_job
     *     Job to connect to the event id
     */
    public JobEventEntry(final byte p_eventId, final AbstractJob p_job) {
        m_eventId = p_eventId;
        m_job = p_job;
    }

    /**
     * Get the event id
     *
     * @return Event id
     */
    public byte getEventId() {
        return m_eventId;
    }

    /**
     * Get the job connected to the event
     *
     * @return Job
     */
    public AbstractJob getJob() {
        return m_job;
    }
}

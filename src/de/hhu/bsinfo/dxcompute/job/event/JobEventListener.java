
package de.hhu.bsinfo.dxcompute.job.event;

/**
 * Listener interface to receive events triggered by the job system
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public interface JobEventListener {
    /**
     * Let the listener provide a bit mask with events it wants to listen to.
     * @return Bit mask indicating events to listen to.
     */
    byte getJobEventBitMask();

    /**
     * Callback if an event is triggered that the listener wants to receive.
     * @param p_eventId
     *            Id of the event triggered.
     * @param p_jobId
     *            Id of the job that triggered the event.
     * @param p_sourceNodeId
     *            The source node id that triggered the event (i.e. can be remote as well)
     */
    void jobEventTriggered(final byte p_eventId, final long p_jobId, final short p_sourceNodeId);
}

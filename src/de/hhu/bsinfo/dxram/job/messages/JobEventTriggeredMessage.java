package de.hhu.bsinfo.dxram.job.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.job.JobID;
import de.hhu.bsinfo.menet.AbstractMessage;

public class JobEventTriggeredMessage extends AbstractMessage {
	private long m_jobId = JobID.INVALID_ID;
	
	/**
	 * Creates an instance of PushJobQueueRequest.
	 * This constructor is used when receiving this message.
	 */
	public JobEventTriggeredMessage() {
		super();
	}

	/**
	 * Creates an instance of PushJobQueueRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public JobEventTriggeredMessage(final short p_destination, final long p_jobId, final byte p_eventId) {
		super(p_destination, JobMessages.TYPE, JobMessages.SUBTYPE_JOB_EVENT_TRIGGERED_MESSAGE);

		m_jobId = p_jobId;
		setStatusCode(p_eventId);
	}
	
	/**
	 * Get the job id.
	 * @return Job id.
	 */
	public long getJobID()
	{
		return m_jobId;
	}
	
	/**
	 * Get the id of the event triggered.
	 * @return Event id.
	 */
	public byte getEventId()
	{
		return getStatusCode();
	}
	
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putLong(m_jobId);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {		
		m_jobId = p_buffer.getLong();
	}

	@Override
	protected final int getPayloadLength() {
		return Long.BYTES;
	}
}

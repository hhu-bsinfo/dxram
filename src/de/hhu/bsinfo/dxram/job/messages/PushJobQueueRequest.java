package de.hhu.bsinfo.dxram.job.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.menet.AbstractRequest;

public class PushJobQueueRequest extends AbstractRequest {
	
	private Job m_job = null;
	
	/**
	 * Creates an instance of PushJobQueueRequest.
	 * This constructor is used when receiving this message.
	 */
	public PushJobQueueRequest() {
		super();
	}

	/**
	 * Creates an instance of PushJobQueueRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public PushJobQueueRequest(final short p_destination, final Job p_job) {
		super(p_destination, JobMessages.TYPE, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_REQUEST);

		m_job = p_job;
	}
	
	public Job getJob() {
		return m_job;
	}
	
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
		
		p_buffer.putShort(m_job.getTypeID());
		exporter.exportObject(m_job);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
		
		m_job = Job.createInstance(p_buffer.getShort());
		
		importer.importObject(m_job);
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Short.BYTES + m_job.sizeofObject();
	}
}

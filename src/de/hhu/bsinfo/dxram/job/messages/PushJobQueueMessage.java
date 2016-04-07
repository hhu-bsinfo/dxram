package de.hhu.bsinfo.dxram.job.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.menet.AbstractMessage;

public class PushJobQueueMessage extends AbstractMessage
{
	private Job m_job = null;
	private byte m_callbackJobEventBitMask = 0;
	
	/**
	 * Creates an instance of PushJobQueueRequest.
	 * This constructor is used when receiving this message.
	 */
	public PushJobQueueMessage() {
		super();
	}

	/**
	 * Creates an instance of PushJobQueueRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public PushJobQueueMessage(final short p_destination, final Job p_job, final byte p_callbackJobEventBitMask) {
		super(p_destination, JobMessages.TYPE, JobMessages.SUBTYPE_PUSH_JOB_QUEUE_MESSAGE);

		m_job = p_job;
		m_callbackJobEventBitMask = p_callbackJobEventBitMask;
	}
	
	/**
	 * Get the job of this request.
	 * @return Job.
	 */
	public Job getJob() {
		return m_job;
	}
	
	/**
	 * Get the bitmask to be used when initiating callbacks to the remote
	 * side sending this message.
	 * @return BitMask for callbacks to remote.
	 */
	public byte getCallbackJobEventBitMask()
	{
		return m_callbackJobEventBitMask;
	}
	
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
		
		p_buffer.put(m_callbackJobEventBitMask);
		p_buffer.putShort(m_job.getTypeID());
		exporter.exportObject(m_job);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
		
		m_callbackJobEventBitMask = p_buffer.get();
		m_job = Job.createInstance(p_buffer.getShort());
		
		importer.importObject(m_job);
	}

	@Override
	protected final int getPayloadLength() {
		return Byte.BYTES + Short.BYTES + m_job.sizeofObject();
	}
}

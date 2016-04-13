
package de.hhu.bsinfo.dxcompute.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.menet.AbstractRequest;

public class SubmitTaskRequest extends AbstractRequest {
	private AbstractTaskPayload m_task;

	/**
	 * Creates an instance of RemoteExecuteTaskRequest.
	 * This constructor is used when receiving this message.
	 */
	public SubmitTaskRequest() {
		super();
	}

	/**
	 * Creates an instance of RemoteExecuteTaskRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 * @param p_subtype
	 *            Message subtype
	 * @param p_syncToken
	 *            Token to correctly identify responses to a sync message
	 * @param p_data
	 *            Some custom data.
	 */
	public SubmitTaskRequest(final short p_destination, final AbstractTaskPayload p_task) {
		super(p_destination, MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_REQUEST);
		m_task = p_task;
	}

	public AbstractTaskPayload getTask() {
		return m_task;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);

		p_buffer.putShort(m_task.getTypeId());
		p_buffer.putShort(m_task.getSubtypeId());
		exporter.exportObject(m_task);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);

		short type = p_buffer.getShort();
		short subtype = p_buffer.getShort();
		m_task = AbstractTaskPayload.createInstance(type, subtype);
		if (m_task != null) {
			importer.importObject(m_task);
		}
	}

	@Override
	protected final int getPayloadLength() {
		if (m_task != null) {
			return 2 * Short.BYTES + m_task.sizeofObject();
		} else {
			return 2 * Short.BYTES;
		}
	}
}

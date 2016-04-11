
package de.hhu.bsinfo.dxcompute.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.menet.AbstractRequest;

public class ExecuteTaskRequest extends AbstractRequest {

	private AbstractTaskPayload m_task;

	/**
	 * Creates an instance of MasterSyncBarrierBroadcastMessage.
	 * This constructor is used when receiving this message.
	 */
	public ExecuteTaskRequest() {
		super();
	}

	public AbstractTaskPayload getTask() {
		return m_task;
	}

	/**
	 * Creates an instance of MasterSyncBarrierBroadcastMessage.
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
	public ExecuteTaskRequest(final short p_destination, final AbstractTaskPayload p_task) {
		super(p_destination, MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_REQUEST);
		m_task = p_task;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);

		p_buffer.putLong(m_task.getTaskTypeId());
		exporter.exportObject(m_task);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);

		int typeIdentifier = p_buffer.getInt();
		m_task = AbstractTaskPayload.createInstance(typeIdentifier);
		if (m_task != null) {
			importer.importObject(m_task);
		}
	}

	@Override
	protected final int getPayloadLength() {
		return Long.BYTES + m_task.sizeofObject();
	}
}

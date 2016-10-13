
package de.hhu.bsinfo.dxcompute.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Notify all remote listeners about a task that started execution.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class TaskExecutionStartedMessage extends AbstractMessage {
	private int m_taskPayloadId;

	/**
	 * Creates an instance of TaskRemoteCallbackMessage.
	 * This constructor is used when receiving this message.
	 */
	public TaskExecutionStartedMessage() {
		super();
	}

	/**
	 * Creates an instance of TaskRemoteCallbackMessage.
	 * This constructor is used when sending this message.
	 *
	 * @param p_destination   the destination node id.
	 * @param p_taskPayloadId Id of the task that started execution.
	 */
	public TaskExecutionStartedMessage(final short p_destination, final int p_taskPayloadId) {
		super(p_destination, MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_STARTED_MESSAGE);

		m_taskPayloadId = p_taskPayloadId;
	}

	/**
	 * Id of the task that started execution.
	 *
	 * @return Id of the task.
	 */
	public int getTaskPayloadId() {
		return m_taskPayloadId;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_taskPayloadId);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_taskPayloadId = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES;
	}
}

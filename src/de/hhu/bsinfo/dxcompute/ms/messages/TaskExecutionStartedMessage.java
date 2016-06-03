
package de.hhu.bsinfo.dxcompute.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Notify all remote listeners about a task that started execution.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class TaskExecutionStartedMessage extends AbstractMessage {
	private int m_taskPayloadId;
	private short[] m_slavesAssignedForExecution;

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
	 * @param p_destination
	 *            the destination node id.
	 * @param p_taskPayloadId
	 *            Id of the task that started execution.
	 * @param p_slavesAssignedForExecution
	 *            List of slaves that are assigend for execution.
	 */
	public TaskExecutionStartedMessage(final short p_destination, final int p_taskPayloadId,
			final short[] p_slavesAssignedForExecution) {
		super(p_destination, MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_STARTED_MESSAGE);

		m_taskPayloadId = p_taskPayloadId;
		m_slavesAssignedForExecution = p_slavesAssignedForExecution;
	}

	/**
	 * Id of the task that started execution.
	 * @return Id of the task.
	 */
	public int getTaskPayloadId() {
		return m_taskPayloadId;
	}

	/**
	 * List of slaves that execute the task.
	 * @return List of slaves
	 */
	public short[] getSlavesAssignedForExecution() {
		return m_slavesAssignedForExecution;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_taskPayloadId);
		p_buffer.putInt(m_slavesAssignedForExecution.length);
		for (short slavesAssignedForExecution : m_slavesAssignedForExecution) {
			p_buffer.putShort(slavesAssignedForExecution);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_taskPayloadId = p_buffer.getInt();
		int slaveCount = p_buffer.getInt();
		m_slavesAssignedForExecution = new short[slaveCount];
		for (int i = 0; i < slaveCount; i++) {
			m_slavesAssignedForExecution[i] = p_buffer.getShort();
		}
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + Integer.BYTES + Short.BYTES * m_slavesAssignedForExecution.length;
	}
}

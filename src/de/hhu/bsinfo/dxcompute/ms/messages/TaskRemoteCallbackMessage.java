
package de.hhu.bsinfo.dxcompute.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractMessage;

public class TaskRemoteCallbackMessage extends AbstractMessage {
	private long m_taskPayloadId;
	private int m_callbackId;

	/**
	 * Creates an instance of TaskRemoteCallbackMessage.
	 * This constructor is used when receiving this message.
	 */
	public TaskRemoteCallbackMessage() {
		super();
	}

	/**
	 * Creates an instance of TaskRemoteCallbackMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public TaskRemoteCallbackMessage(final short p_destination, final long p_taskPayloadId, final int p_callbackId) {
		super(p_destination, MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_TASK_REMOTE_CALLBACK_MESSAGE);

		m_taskPayloadId = p_taskPayloadId;
		m_callbackId = p_callbackId;
	}

	public long getTaskPayloadId() {
		return m_taskPayloadId;
	}

	public int getCallbackId() {
		return m_callbackId;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putLong(m_taskPayloadId);
		p_buffer.putInt(m_callbackId);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_taskPayloadId = p_buffer.getLong();
		m_callbackId = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Long.BYTES + Integer.BYTES;
	}
}

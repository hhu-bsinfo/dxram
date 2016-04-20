
package de.hhu.bsinfo.dxcompute.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractResponse;

public class SubmitTaskResponse extends AbstractResponse {
	private int m_assignedComputeGroupId;
	private long m_assignedPayloadId;

	/**
	 * Creates an instance of SubmitTaskResponse.
	 * This constructor is used when receiving this message.
	 */
	public SubmitTaskResponse() {
		super();
	}

	/**
	 * Creates an instance of SubmitTaskResponse.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 */
	public SubmitTaskResponse(final SubmitTaskRequest p_request, final int p_assignedComputeGroupId,
			final long p_assignedPayloadId) {
		super(p_request, MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_RESPONSE);

		m_assignedComputeGroupId = p_assignedComputeGroupId;
		m_assignedPayloadId = p_assignedPayloadId;
	}

	public int getAssignedComputeGroupId() {
		return m_assignedComputeGroupId;
	}

	public long getAssignedPayloadId() {
		return m_assignedPayloadId;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_assignedComputeGroupId);
		p_buffer.putLong(m_assignedPayloadId);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_assignedComputeGroupId = p_buffer.getInt();
		m_assignedPayloadId = p_buffer.getLong();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + Long.BYTES;
	}
}

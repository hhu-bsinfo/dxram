
package de.hhu.bsinfo.dxcompute.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response of the master to a join request by a slave.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class SlaveJoinResponse extends AbstractResponse {
	private int m_executionBarrierId = BarrierID.INVALID_ID;

	/**
	 * Creates an instance of SlaveJoinResponse.
	 * This constructor is used when receiving this message.
	 */
	public SlaveJoinResponse() {
		super();
	}

	/**
	 * Creates an instance of SlaveJoinResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            request to respond to.
	 * @param p_executionBarrierId
	 *            The id of the barrier to sync for execution of a task
	 */
	public SlaveJoinResponse(final SlaveJoinRequest p_request, final int p_executionBarrierId) {
		super(p_request, MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_RESPONSE);

		m_executionBarrierId = p_executionBarrierId;
	}

	/**
	 * Get the barrier id used for the execution barrier to sync slaves to the master.
	 * @return Execution barrier id.
	 */
	public int getExecutionBarrierId() {
		return m_executionBarrierId;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_executionBarrierId);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_executionBarrierId = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES;
	}
}

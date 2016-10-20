
package de.hhu.bsinfo.dxram.recovery.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a RecoverBackupRangeRequest
 * @author Kevin Beineke
 *         08.10.2015
 */
public class RecoverBackupRangeResponse extends AbstractResponse {

	// Attributes
	private int m_numberOfRecoveredChunks;

	// Constructors
	/**
	 * Creates an instance of RecoverBackupRangeResponse
	 */
	public RecoverBackupRangeResponse() {
		super();

		m_numberOfRecoveredChunks = 0;
	}

	/**
	 * Creates an instance of RecoverBackupRangeResponse
	 * @param p_request
	 *            the corresponding RecoverBackupRangeRequest
	 * @param p_numberOfRecoveredChunks
	 *            number of recovered chunks
	 */
	public RecoverBackupRangeResponse(final RecoverBackupRangeRequest p_request,
			final int p_numberOfRecoveredChunks) {
		super(p_request, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE);

		m_numberOfRecoveredChunks = p_numberOfRecoveredChunks;
	}

	// Getters
	/**
	 * Returns the number of recovered chunks
	 * @return the number of recovered chunks
	 */
	public final int getNumberOfRecoveredChunks() {
		return m_numberOfRecoveredChunks;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_numberOfRecoveredChunks);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_numberOfRecoveredChunks = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES;
	}

}

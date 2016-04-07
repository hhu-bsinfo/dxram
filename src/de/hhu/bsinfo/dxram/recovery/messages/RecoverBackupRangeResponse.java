
package de.hhu.bsinfo.dxram.recovery.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a RecoverBackupRangeRequest
 * @author Kevin Beineke
 *         08.10.2015
 */
public class RecoverBackupRangeResponse extends AbstractResponse {

	// Attributes
	private Chunk[] m_chunks;

	// Constructors
	/**
	 * Creates an instance of RecoverBackupRangeResponse
	 */
	public RecoverBackupRangeResponse() {
		super();

		m_chunks = null;
	}

	/**
	 * Creates an instance of RecoverBackupRangeResponse
	 * @param p_request
	 *            the corresponding RecoverBackupRangeRequest
	 * @param p_chunks
	 *            the recovered Chunks
	 */
	public RecoverBackupRangeResponse(final RecoverBackupRangeRequest p_request, final Chunk[] p_chunks) {
		super(p_request, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE);

		m_chunks = p_chunks;
	}

	// Getters
	/**
	 * Get Chunks
	 * @return the Chunks
	 */
	public final Chunk[] getChunks() {
		return m_chunks;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_chunks.length);
		for (Chunk chunk : m_chunks) {
			p_buffer.putLong(chunk.getID());
			p_buffer.putInt(chunk.getDataSize());
			p_buffer.put(chunk.getData());
		}

	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		long chunkID;
		byte[] bytes;

		final int length = p_buffer.getInt();

		m_chunks = new Chunk[length];
		for (int i = 0; i < length; i++) {
			chunkID = p_buffer.getLong();
			bytes = new byte[p_buffer.getInt()];
			p_buffer.get(bytes);

			m_chunks[i] = new Chunk(chunkID, ByteBuffer.wrap(bytes));
		}
	}

	@Override
	protected final int getPayloadLength() {
		int ret = 4;

		for (Chunk chunk : m_chunks) {
			ret += Long.BYTES + Integer.BYTES + chunk.getDataSize();
		}

		return ret;
	}

}

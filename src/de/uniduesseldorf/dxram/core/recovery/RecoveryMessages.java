
package de.uniduesseldorf.dxram.core.recovery;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.io.InputHelper;
import de.uniduesseldorf.dxram.core.io.OutputHelper;
import de.uniduesseldorf.dxram.core.net.AbstractRequest;
import de.uniduesseldorf.dxram.core.net.AbstractResponse;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Encapsulates messages for the LookupHandler
 * @author Kevin Beinekechun
 *         03.06.2013
 */
public final class RecoveryMessages {

	// Constants
	public static final byte TYPE = 40;
	public static final byte SUBTYPE_RECOVER_REQUEST = 1;
	public static final byte SUBTYPE_RECOVER_RESPONSE = 2;

	// Constructors
	/**
	 * Creates an instance of LookupMessages
	 */
	private RecoveryMessages() {}

	// Classes
	/**
	 * Recover Backup Range Request
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class RecoverBackupRangeRequest extends AbstractRequest {

		// Attributes
		private short m_owner;
		private long m_firstChunkIDOrRangeID;

		// Constructors
		/**
		 * Creates an instance of RecoverBackupRangeRequest
		 */
		public RecoverBackupRangeRequest() {
			super();

			m_owner = (short) -1;
			m_firstChunkIDOrRangeID = -1;
		}

		/**
		 * Creates an instance of RecoverBackupRangeRequest
		 * @param p_destination
		 *            the destination
		 * @param p_owner
		 *            the NodeID of the owner
		 * @param p_firstChunkIDOrRangeID
		 *            the first ChunkID of the backup range or the RangeID for migrations
		 */
		public RecoverBackupRangeRequest(final short p_destination, final short p_owner, final long p_firstChunkIDOrRangeID) {
			super(p_destination, TYPE, SUBTYPE_RECOVER_REQUEST);

			Contract.checkNotNull(p_owner, "no NodeID given");
			Contract.checkNotNull(p_firstChunkIDOrRangeID, "no RangeID given");

			m_owner = p_owner;
			m_firstChunkIDOrRangeID = p_firstChunkIDOrRangeID;
		}

		// Getters
		/**
		 * Get the owner
		 * @return the NodeID
		 */
		public final short getOwner() {
			return m_owner;
		}

		/**
		 * Get the ChunkID or RangeID
		 * @return the ChunkID or RangeID
		 */
		public final long getFirstChunkIDOrRangeID() {
			return m_firstChunkIDOrRangeID;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeNodeID(p_buffer, m_owner);
			OutputHelper.writeChunkID(p_buffer, m_firstChunkIDOrRangeID);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_owner = InputHelper.readNodeID(p_buffer);
			m_firstChunkIDOrRangeID = InputHelper.readChunkID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getNodeIDWriteLength() + OutputHelper.getChunkIDWriteLength();
		}

	}

	/**
	 * Response to a RecoverBackupRangeRequest
	 * @author Kevin Beineke
	 *         06.09.2012
	 */
	public static class RecoverBackupRangeResponse extends AbstractResponse {

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
			super(p_request, SUBTYPE_RECOVER_RESPONSE);

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
			OutputHelper.writeChunks(p_buffer, m_chunks);

		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunks = InputHelper.readChunks(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunksWriteLength(m_chunks);
		}

	}

}

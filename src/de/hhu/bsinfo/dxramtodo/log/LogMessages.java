
package de.hhu.bsinfo.dxram.log;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.AbstractRequest;
import de.hhu.bsinfo.menet.AbstractResponse;
import de.hhu.bsinfo.utils.Contract;

/**
 * Encapsulates messages for the LogHandler
 * @author Kevin Beineke 15.05.2015
 */
public final class LogMessages {

	// Constants
	public static final byte TYPE = 30;
	public static final byte SUBTYPE_LOG_MESSAGE = 1;
	public static final byte SUBTYPE_REMOVE_MESSAGE = 2;
	public static final byte SUBTYPE_INIT_REQUEST = 3;
	public static final byte SUBTYPE_INIT_RESPONSE = 4;
	public static final byte SUBTYPE_LOG_COMMAND_REQUEST = 5;
	public static final byte SUBTYPE_LOG_COMMAND_RESPONSE = 6;

	// Constructors
	/**
	 * Creates an instance of DataMessages
	 */
	private LogMessages() {}

	// Classes
	/**
	 * Message for logging a Chunk on a remote node
	 * @author Kevin Beineke 20.04.2014
	 */
	public static class LogMessage extends AbstractMessage {

		// Attributes
		private byte m_rangeID;
		private Chunk[] m_chunks;
		private ByteBuffer m_buffer;

		// Constructors
		/**
		 * Creates an instance of LogMessage
		 */
		public LogMessage() {
			super();

			m_rangeID = -1;
			m_chunks = null;
			m_buffer = null;
		}

		/**
		 * Creates an instance of LogMessage
		 * @param p_destination
		 *            the destination
		 * @param p_chunks
		 *            the Chunks to store
		 */
		public LogMessage(final short p_destination, final Chunk[] p_chunks) {
			super(p_destination, TYPE, SUBTYPE_LOG_MESSAGE);

			Contract.checkNotNull(p_chunks, "no chunks given");

			m_rangeID = -1;
			m_chunks = p_chunks;
		}

		/**
		 * Creates an instance of LogMessage
		 * @param p_destination
		 *            the destination
		 * @param p_chunks
		 *            the Chunks to store
		 * @param p_rangeID
		 *            the RangeID
		 */
		public LogMessage(final short p_destination, final byte p_rangeID, final Chunk[] p_chunks) {
			super(p_destination, TYPE, SUBTYPE_LOG_MESSAGE);

			Contract.checkNotNull(p_chunks, "no chunks given");

			m_rangeID = p_rangeID;
			m_chunks = p_chunks;
		}

		// Getters
		/**
		 * Get the message buffer
		 * @return the message buffer
		 */
		public final ByteBuffer getMessageBuffer() {
			return m_buffer;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			p_buffer.put(m_rangeID);

			p_buffer.putInt(m_chunks.length);
			for (int i = 0; i < m_chunks.length; i++) {
				p_buffer.putLong(m_chunks[i].getChunkID());
				p_buffer.putInt(m_chunks[i].getSize());
				p_buffer.put(m_chunks[i].getData());
			}
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_buffer = p_buffer;
		}

		@Override
		protected final int getPayloadLengthForWrite() {
			int ret = 5;

			for (Chunk chunk : m_chunks) {
				ret += 12 + chunk.getSize();
			}

			return ret;
		}
	}

	/**
	 * Message for removing a Chunk on a remote node
	 * @author Kevin Beineke 20.04.2014
	 */
	public static class RemoveMessage extends AbstractMessage {

		// Attributes
		private long[] m_chunkIDs;
		private byte m_rangeID;
		private ByteBuffer m_buffer;

		// Constructors
		/**
		 * Creates an instance of RemoveMessage
		 */
		public RemoveMessage() {
			super();

			m_chunkIDs = null;
			m_rangeID = -1;
			m_buffer = null;
		}

		/**
		 * Creates an instance of RemoveMessage
		 * @param p_destination
		 *            the destination
		 * @param p_chunkIDs
		 *            the ChunkIDs of the Chunks to remove
		 */
		public RemoveMessage(final short p_destination, final long[] p_chunkIDs) {
			super(p_destination, TYPE, SUBTYPE_REMOVE_MESSAGE);

			m_chunkIDs = p_chunkIDs;
			m_rangeID = -1;
		}

		/**
		 * Creates an instance of RemoveMessage
		 * @param p_destination
		 *            the destination
		 * @param p_chunkIDs
		 *            the ChunkIDs of the Chunks to remove
		 * @param p_rangeID
		 *            the RangeID
		 */
		public RemoveMessage(final short p_destination, final long[] p_chunkIDs, final byte p_rangeID) {
			super(p_destination, TYPE, SUBTYPE_REMOVE_MESSAGE);

			m_chunkIDs = p_chunkIDs;
			m_rangeID = p_rangeID;
		}

		// Getters
		/**
		 * Get the RangeID
		 * @return the RangeID
		 */
		public final ByteBuffer getMessageBuffer() {
			return m_buffer;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeByte(p_buffer, m_rangeID);
			OutputHelper.writeInt(p_buffer, m_chunkIDs.length);
			for (int i = 0; i < m_chunkIDs.length; i++) {
				OutputHelper.writeChunkID(p_buffer, m_chunkIDs[i]);
			}
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_buffer = p_buffer;
		}

		@Override
		protected final int getPayloadLengthForWrite() {
			return OutputHelper.getByteWriteLength() + OutputHelper.getIntWriteLength()
					+ (OutputHelper.getChunkIDWriteLength()) * m_chunkIDs.length;
		}
	}

	/**
	 * Request for initialization of a backup range on a remote node
	 * @author Kevin Beineke 20.04.2014
	 */
	public static class InitRequest extends AbstractRequest {

		// Attributes
		private long m_firstChunkIDOrRangeID;
		private short m_owner;

		// Constructors
		/**
		 * Creates an instance of InitRequest
		 */
		public InitRequest() {
			super();

			m_firstChunkIDOrRangeID = 0;
			m_owner = -1;
		}

		/**
		 * Creates an instance of InitRequest
		 * @param p_destination
		 *            the destination
		 * @param p_firstChunkIDOrRangeID
		 *            the beginning of the range
		 * @param p_owner
		 *            the current owner
		 */
		public InitRequest(final short p_destination, final long p_firstChunkIDOrRangeID, final short p_owner) {
			super(p_destination, TYPE, SUBTYPE_INIT_REQUEST);

			m_firstChunkIDOrRangeID = p_firstChunkIDOrRangeID;
			m_owner = p_owner;
		}

		// Getters
		/**
		 * Get the beginning of the range
		 * @return the ChunkID
		 */
		public final long getFirstCIDOrRangeID() {
			return m_firstChunkIDOrRangeID;
		}

		/**
		 * Get the current owner
		 * @return the current owner
		 */
		public final short getOwner() {
			return m_owner;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeLong(p_buffer, m_firstChunkIDOrRangeID);
			OutputHelper.writeShort(p_buffer, m_owner);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_firstChunkIDOrRangeID = InputHelper.readLong(p_buffer);
			m_owner = InputHelper.readShort(p_buffer);
		}

		@Override
		protected final int getPayloadLengthForWrite() {
			return OutputHelper.getLongWriteLength() + OutputHelper.getShortWriteLength();
		}
	}

	/**
	 * Response to a InitRequest
	 * @author Kevin Beineke 20.04.2014
	 */
	public static class InitResponse extends AbstractResponse {

		// Attributes
		private boolean m_success;

		// Constructors
		/**
		 * Creates an instance of InitResponse
		 */
		public InitResponse() {
			super();

			m_success = false;
		}

		/**
		 * Creates an instance of InitResponse
		 * @param p_request
		 *            the request
		 * @param p_success
		 *            true if remove was successful
		 */
		public InitResponse(final InitRequest p_request, final boolean p_success) {
			super(p_request, SUBTYPE_INIT_RESPONSE);

			m_success = p_success;
		}

		// Getters
		/**
		 * Get the status
		 * @return true if remove was successful
		 */
		public final boolean getStatus() {
			return m_success;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeBoolean(p_buffer, m_success);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_success = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLengthForWrite() {
			return OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Request for command
	 * @author Kevin Beineke 10.09.2015
	 */
	public static class LogCommandRequest extends AbstractRequest {

		// Attributes
		private String m_cmd;

		// Constructors
		/**
		 * Creates an instance of LogCommandRequest
		 */
		public LogCommandRequest() {
			super();
			m_cmd = null;
		}

		/**
		 * Creates an instance of LogCommandRequest
		 * @param p_destination
		 *            the destination
		 * @param p_cmd
		 *            the command
		 */
		public LogCommandRequest(final short p_destination, final String p_cmd) {
			super(p_destination, TYPE, SUBTYPE_LOG_COMMAND_REQUEST);
			Contract.checkNotNull(p_cmd, "error: no argument given");
			m_cmd = p_cmd;
		}

		/**
		 * Get the command
		 * @return the command
		 */
		public final String getArgument() {
			return m_cmd;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeString(p_buffer, m_cmd);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_cmd = InputHelper.readString(p_buffer);
		}

		@Override
		protected final int getPayloadLengthForWrite() {
			return OutputHelper.getStringsWriteLength(m_cmd);
		}

	}

	/**
	 * Response to a LogCommandRequest
	 * @author Florian Klein 05.07.2014
	 */
	public static class LogCommandResponse extends AbstractResponse {

		// Attributes
		private String m_answer;

		// Constructors
		/**
		 * Creates an instance of LogCommandResponse
		 */
		public LogCommandResponse() {
			super();

			m_answer = null;
		}

		/**
		 * Creates an instance of LogCommandResponse
		 * @param p_request
		 *            the corresponding LogCommandRequest
		 * @param p_answer
		 *            the answer
		 */
		public LogCommandResponse(final LogCommandRequest p_request, final String p_answer) {
			super(p_request, SUBTYPE_LOG_COMMAND_RESPONSE);

			m_answer = p_answer;
		}

		// Getters
		/**
		 * Get the answer
		 * @return the answer
		 */
		public final String getAnswer() {
			return m_answer;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeString(p_buffer, m_answer);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_answer = InputHelper.readString(p_buffer);
		}

		@Override
		protected final int getPayloadLengthForWrite() {
			return OutputHelper.getStringsWriteLength(m_answer);
		}

	}
}

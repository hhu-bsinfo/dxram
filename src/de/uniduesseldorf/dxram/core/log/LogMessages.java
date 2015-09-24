
package de.uniduesseldorf.dxram.core.log;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.io.InputHelper;
import de.uniduesseldorf.dxram.core.io.OutputHelper;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.AbstractRequest;
import de.uniduesseldorf.dxram.core.net.AbstractResponse;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Encapsulates messages for the LogHandler
 * @author Kevin Beineke 15.05.2015
 */
public final class LogMessages {

	// Constants
	public static final byte TYPE = 30;
	public static final byte SUBTYPE_LOG_REQUEST = 1;
	public static final byte SUBTYPE_LOG_RESPONSE = 2;
	public static final byte SUBTYPE_LOG_MESSAGE = 3;
	public static final byte SUBTYPE_REMOVE_MESSAGE = 4;
	public static final byte SUBTYPE_INIT_REQUEST = 5;
	public static final byte SUBTYPE_INIT_RESPONSE = 6;
	public static final byte SUBTYPE_LOG_COMMAND_REQUEST = 7;
	public static final byte SUBTYPE_LOG_COMMAND_RESPONSE = 8;

	// Constructors
	/**
	 * Creates an instance of DataMessages
	 */
	private LogMessages() {}

	// Classes
	/**
	 * Request for logging a Chunk on a remote node
	 * @author Kevin Beineke 20.04.2014
	 */
	public static class LogRequest extends AbstractRequest {

		// Attributes
		private Chunk[] m_chunks;
		private byte m_rangeID;

		// Constructors
		/**
		 * Creates an instance of LogRequest
		 */
		public LogRequest() {
			super();

			m_chunks = null;
			m_rangeID = -1;
		}

		/**
		 * Creates an instance of LogRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunks
		 *            the Chunks to store
		 */
		public LogRequest(final short p_destination, final Chunk[] p_chunks) {
			super(p_destination, TYPE, SUBTYPE_LOG_REQUEST);

			Contract.checkNotNull(p_chunks, "no chunks given");

			m_chunks = p_chunks;
			m_rangeID = -1;
		}

		/**
		 * Creates an instance of LogRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunks
		 *            the Chunks to store
		 * @param p_rangeID
		 *            the RangeID
		 */
		public LogRequest(final short p_destination, final Chunk[] p_chunks, final byte p_rangeID) {
			super(p_destination, TYPE, SUBTYPE_LOG_REQUEST);

			Contract.checkNotNull(p_chunks, "no chunks given");

			m_chunks = p_chunks;
			m_rangeID = p_rangeID;
		}

		// Getters
		/**
		 * Get the Chunk to store
		 * @return the Chunk to store
		 */
		public final Chunk[] getChunks() {
			return m_chunks;
		}

		/**
		 * Get the RangeID
		 * @return the RangeID
		 */
		public final byte getRangeID() {
			return m_rangeID;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunks(p_buffer, m_chunks);
			OutputHelper.writeByte(p_buffer, m_rangeID);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunks = InputHelper.readChunks(p_buffer);
			m_rangeID = InputHelper.readByte(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunksWriteLength(m_chunks) + OutputHelper.getByteWriteLength();
		}

	}

	/**
	 * Response to a LogRequest
	 * @author Kevin Beineke 20.04.2014
	 */
	public static class LogResponse extends AbstractResponse {

		// Attributes
		private boolean m_success;

		// Constructors
		/**
		 * Creates an instance of LogResponse
		 */
		public LogResponse() {
			super();

			m_success = false;
		}

		/**
		 * Creates an instance of LogResponse
		 * @param p_request
		 *            the request
		 * @param p_success
		 *            true if remove was successful
		 */
		public LogResponse(final LogRequest p_request, final boolean p_success) {
			super(p_request, SUBTYPE_LOG_RESPONSE);

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
		protected final int getPayloadLength() {
			return OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Message for logging a Chunk on a remote node
	 * @author Kevin Beineke 20.04.2014
	 */
	public static class LogMessage extends AbstractMessage {

		// Attributes
		private Chunk[] m_chunks;
		private byte m_rangeID;

		// Constructors
		/**
		 * Creates an instance of LogMessage
		 */
		public LogMessage() {
			super();

			m_chunks = null;
			m_rangeID = -1;
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

			m_chunks = p_chunks;
			m_rangeID = -1;
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
		public LogMessage(final short p_destination, final Chunk[] p_chunks, final byte p_rangeID) {
			super(p_destination, TYPE, SUBTYPE_LOG_MESSAGE);

			Contract.checkNotNull(p_chunks, "no chunks given");

			m_chunks = p_chunks;
			m_rangeID = p_rangeID;
		}

		// Getters
		/**
		 * Get the Chunk to store
		 * @return the Chunk to store
		 */
		public final Chunk[] getChunks() {
			return m_chunks;
		}

		/**
		 * Get the RangeID
		 * @return the RangeID
		 */
		public final byte getRangeID() {
			return m_rangeID;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunks(p_buffer, m_chunks);
			OutputHelper.writeByte(p_buffer, m_rangeID);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunks = InputHelper.readChunks(p_buffer);
			m_rangeID = InputHelper.readByte(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunksWriteLength(m_chunks) + OutputHelper.getByteWriteLength();
		}
	}

	/**
	 * Message for removing a Chunk on a remote node
	 * @author Kevin Beineke 20.04.2014
	 */
	public static class RemoveMessage extends AbstractMessage {

		// Attributes
		private long[] m_chunkIDs;
		private int[] m_versions;
		private byte m_rangeID;

		// Constructors
		/**
		 * Creates an instance of RemoveMessage
		 */
		public RemoveMessage() {
			super();

			m_chunkIDs = null;
			m_versions = null;
			m_rangeID = -1;
		}

		/**
		 * Creates an instance of RemoveMessage
		 * @param p_destination
		 *            the destination
		 * @param p_chunkIDs
		 *            the ChunkIDs of the Chunks to remove
		 * @param p_versions
		 *            the versions of the Chunks to remove
		 */
		public RemoveMessage(final short p_destination, final long[] p_chunkIDs, final int[] p_versions) {
			super(p_destination, TYPE, SUBTYPE_REMOVE_MESSAGE);

			m_chunkIDs = p_chunkIDs;
			m_versions = p_versions;
			m_rangeID = -1;
		}

		/**
		 * Creates an instance of RemoveMessage
		 * @param p_destination
		 *            the destination
		 * @param p_chunkIDs
		 *            the ChunkIDs of the Chunks to remove
		 * @param p_versions
		 *            the versions of the Chunks to remove
		 * @param p_rangeID
		 *            the RangeID
		 */
		public RemoveMessage(final short p_destination, final long[] p_chunkIDs, final int[] p_versions, final byte p_rangeID) {
			super(p_destination, TYPE, SUBTYPE_REMOVE_MESSAGE);

			m_chunkIDs = p_chunkIDs;
			m_versions = p_versions;
			m_rangeID = p_rangeID;
		}

		// Getters
		/**
		 * Get the ChunkIDs
		 * @return the ChunkIDs
		 */
		public final long[] getChunkIDs() {
			return m_chunkIDs;
		}

		/**
		 * Get the versions
		 * @return the versions
		 */
		public final int[] getVersions() {
			return m_versions;
		}

		/**
		 * Get the RangeID
		 * @return the RangeID
		 */
		public final byte getRangeID() {
			return m_rangeID;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkIDs(p_buffer, m_chunkIDs);
			OutputHelper.writeIntArray(p_buffer, m_versions);
			OutputHelper.writeByte(p_buffer, m_rangeID);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunkIDs = InputHelper.readChunkIDs(p_buffer);
			m_versions = InputHelper.readIntArray(p_buffer);
			m_rangeID = InputHelper.readByte(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDsWriteLength(m_chunkIDs.length)
					+ OutputHelper.getByteWriteLength() + OutputHelper.getIntArrayWriteLength(m_versions.length);
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
		protected final int getPayloadLength() {
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
		protected final int getPayloadLength() {
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
		protected final int getPayloadLength() {
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
		protected final int getPayloadLength() {
			return OutputHelper.getStringsWriteLength(m_answer);
		}

	}
}

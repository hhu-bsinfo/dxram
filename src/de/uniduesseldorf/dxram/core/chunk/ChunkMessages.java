
package de.uniduesseldorf.dxram.core.chunk;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.io.InputHelper;
import de.uniduesseldorf.dxram.core.io.OutputHelper;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.AbstractRequest;
import de.uniduesseldorf.dxram.core.net.AbstractResponse;
import de.uniduesseldorf.dxram.core.util.ChunkID;

import de.uniduesseldorf.utils.Contract;

/**
 * Encapsulates messages for the DataHandler
 * @author Florian Klein 24.03.2012
 */
public final class ChunkMessages {

	// Constants
	public static final byte TYPE = 10;
	public static final byte SUBTYPE_GET_REQUEST = 1;
	public static final byte SUBTYPE_GET_RESPONSE = 2;
	public static final byte SUBTYPE_PUT_REQUEST = 3;
	public static final byte SUBTYPE_PUT_RESPONSE = 4;
	public static final byte SUBTYPE_REMOVE_REQUEST = 5;
	public static final byte SUBTYPE_REMOVE_RESPONSE = 6;
	public static final byte SUBTYPE_LOCK_REQUEST = 7;
	public static final byte SUBTYPE_LOCK_RESPONSE = 8;
	public static final byte SUBTYPE_UNLOCK_MESSAGE = 9;
	public static final byte SUBTYPE_DATA_REQUEST = 10;
	public static final byte SUBTYPE_DATA_RESPONSE = 11;
	public static final byte SUBTYPE_DATA_MESSAGE = 12;
	public static final byte SUBTYPE_MULTIGET_REQUEST = 13;
	public static final byte SUBTYPE_MULTIGET_RESPONSE = 14;
	public static final byte SUBTYPE_CHUNK_COMMAND_MESSAGE = 15;
	public static final byte SUBTYPE_CHUNK_COMMAND_REQUEST = 16;
	public static final byte SUBTYPE_CHUNK_COMMAND_RESPONSE = 17;

	// Constructors
	/**
	 * Creates an instance of DataMessages
	 */
	private ChunkMessages() {}

	// Classes
	/**
	 * Request for getting a Chunk from a remote node
	 * @author Florian Klein 09.03.2012
	 */
	public static class GetRequest extends AbstractRequest {

		// Attributes
		private long m_chunkID;

		// Constructors
		/**
		 * Creates an instance of GetRequest
		 */
		public GetRequest() {
			super();

			m_chunkID = ChunkID.INVALID_ID;
		}

		/**
		 * Creates an instance of GetRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunkID
		 *            The ID of the Chunk to get
		 */
		public GetRequest(final short p_destination, final long p_chunkID) {
			super(p_destination, TYPE, SUBTYPE_GET_REQUEST);

			ChunkID.check(p_chunkID);

			m_chunkID = p_chunkID;
		}

		// Getters
		/**
		 * Get the ID of the Chunk to get
		 * @return the ID of the Chunk to get
		 */
		public final long getChunkID() {
			return m_chunkID;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkID(p_buffer, m_chunkID);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunkID = InputHelper.readChunkID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDWriteLength();
		}

	}

	/**
	 * Response to a GetRequest
	 * @author Florian Klein 09.03.2012
	 */
	public static class GetResponse extends AbstractResponse {

		// Attributes
		private Chunk m_chunk;

		// Constructors
		/**
		 * Creates an instance of GetResponse
		 */
		public GetResponse() {
			super();

			m_chunk = null;
		}

		/**
		 * Creates an instance of GetResponse
		 * @param p_request
		 *            the corresponding GetRequest
		 * @param p_chunk
		 *            the requested Chunk
		 */
		public GetResponse(final GetRequest p_request, final Chunk p_chunk) {
			super(p_request, SUBTYPE_GET_RESPONSE);

			m_chunk = p_chunk;
		}

		// Getters
		/**
		 * Get the requested Chunk
		 * @return the requested Chunk
		 */
		public final Chunk getChunk() {
			return m_chunk;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunk(p_buffer, m_chunk);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunk = InputHelper.readChunk(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkWriteLength(m_chunk);
		}

	}

	/**
	 * Request for updating a Chunk on a remote node
	 * @author Florian Klein 09.03.2012
	 */
	public static class PutRequest extends AbstractRequest {

		// Attributes
		private Chunk m_chunk;
		private boolean m_releaseLock;

		// Constructors
		/**
		 * Creates an instance of PutRequest
		 */
		public PutRequest() {
			super();

			m_chunk = null;
			m_releaseLock = false;
		}

		/**
		 * Creates an instance of PutRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunk
		 *            the Chunk to put
		 */
		public PutRequest(final short p_destination, final Chunk p_chunk) {
			this(p_destination, p_chunk, false);
		}

		/**
		 * Creates an instance of PutRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunk
		 *            the Chunk to put
		 * @param p_releaseLock
		 *            if true a potential lock will be released
		 */
		public PutRequest(final short p_destination, final Chunk p_chunk, final boolean p_releaseLock) {
			super(p_destination, TYPE, SUBTYPE_PUT_REQUEST);

			Contract.checkNotNull(p_chunk, "no chunk given");

			m_chunk = p_chunk;
			m_releaseLock = p_releaseLock;
		}

		// Getters
		/**
		 * Get the Chunk to put
		 * @return the Chunk to put
		 */
		public final Chunk getChunk() {
			return m_chunk;
		}

		/**
		 * Checks if a potential lock should be realeased
		 * @return true if a potential lock should be released, false otherwise
		 */
		public final boolean isReleaseLock() {
			return m_releaseLock;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunk(p_buffer, m_chunk);
			OutputHelper.writeBoolean(p_buffer, m_releaseLock);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunk = InputHelper.readChunk(p_buffer);
			m_releaseLock = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkWriteLength(m_chunk) + OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Response to a PutRequest
	 * @author Florian Klein 09.03.2012
	 */
	public static class PutResponse extends AbstractResponse {

		// Attributes
		private boolean m_success;

		// Constructors
		/**
		 * Creates an instance of PutResponse
		 */
		public PutResponse() {
			super();

			m_success = false;
		}

		/**
		 * Creates an instance of DataResponse
		 * @param p_request
		 *            the request
		 * @param p_success
		 *            true if put was successful
		 */
		public PutResponse(final PutRequest p_request, final boolean p_success) {
			super(p_request, SUBTYPE_PUT_RESPONSE);

			m_success = p_success;
		}

		// Getters
		/**
		 * Get the status
		 * @return true if put was successful
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
	 * Request for removing a Chunk on a remote node
	 * @author Florian Klein 09.03.2012
	 */
	public static class RemoveRequest extends AbstractRequest {

		// Attributes
		private long m_chunkID;

		// Constructors
		/**
		 * Creates an instance of RemoveRequest
		 */
		public RemoveRequest() {
			super();

			m_chunkID = ChunkID.INVALID_ID;
		}

		/**
		 * Creates an instance of RemoveRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunkID
		 *            the ID for the Chunk to remove
		 */
		public RemoveRequest(final short p_destination, final long p_chunkID) {
			super(p_destination, TYPE, SUBTYPE_REMOVE_REQUEST);

			ChunkID.check(p_chunkID);

			m_chunkID = p_chunkID;
		}

		// Getters
		/**
		 * Get the ID for the Chunk to remove
		 * @return the ID for the Chunk to remove
		 */
		public final long getChunkID() {
			return m_chunkID;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkID(p_buffer, m_chunkID);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunkID = InputHelper.readChunkID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDWriteLength();
		}

	}

	/**
	 * Response to a RemoveRequest
	 * @author Florian Klein 09.03.2012
	 */
	public static class RemoveResponse extends AbstractResponse {

		// Attributes
		private boolean m_success;

		// Constructors
		/**
		 * Creates an instance of RemoveResponse
		 */
		public RemoveResponse() {
			super();

			m_success = false;
		}

		/**
		 * Creates an instance of RemoveResponse
		 * @param p_request
		 *            the request
		 * @param p_success
		 *            true if remove was successful
		 */
		public RemoveResponse(final RemoveRequest p_request, final boolean p_success) {
			super(p_request, SUBTYPE_REMOVE_RESPONSE);

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
	 * Request for locking a Chunk on a remote node
	 * @author Florian Klein 09.03.2012
	 */
	public static class LockRequest extends AbstractRequest {

		// Attributes
		private long m_chunkID;
		private boolean m_readLock;

		// Constructors
		/**
		 * Creates an instance of LockRequest
		 */
		public LockRequest() {
			super();

			m_chunkID = ChunkID.INVALID_ID;
			m_readLock = false;
		}

		/**
		 * Creates an instance of LockRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunkID
		 *            The ID of the Chunk to lock
		 */
		public LockRequest(final short p_destination, final long p_chunkID) {
			this(p_destination, p_chunkID, false);
		}

		/**
		 * Creates an instance of LockRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunkID
		 *            The ID of the Chunk to lock
		 * @param p_readLock
		 *            if true the lock is a read lock, otherwise the lock is a write lock
		 */
		public LockRequest(final short p_destination, final long p_chunkID, final boolean p_readLock) {
			super(p_destination, TYPE, SUBTYPE_LOCK_REQUEST);

			ChunkID.check(p_chunkID);

			m_chunkID = p_chunkID;
			m_readLock = p_readLock;
		}

		// Getters
		/**
		 * Get the ID of the Chunk to lock
		 * @return the ID of the Chunk to lock
		 */
		public final long getChunkID() {
			return m_chunkID;
		}

		/**
		 * Checks if the lock is a read lock
		 * @return true if the lock is a read lock, false otherwise
		 */
		public final boolean isReadLock() {
			return m_readLock;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkID(p_buffer, m_chunkID);
			OutputHelper.writeBoolean(p_buffer, m_readLock);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunkID = InputHelper.readChunkID(p_buffer);
			m_readLock = InputHelper.readBoolean(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDWriteLength() + OutputHelper.getBooleanWriteLength();
		}

	}

	/**
	 * Response to a LockRequest
	 * @author Florian Klein 09.03.2012
	 */
	public static class LockResponse extends AbstractResponse {

		// Attributes
		private Chunk m_chunk;

		// Constructors
		/**
		 * Creates an instance of LockResponse
		 */
		public LockResponse() {
			super();

			m_chunk = null;
		}

		/**
		 * Creates an instance of LockResponse
		 * @param p_request
		 *            the corresponding LockRequest
		 * @param p_chunk
		 *            the requested Chunk
		 */
		public LockResponse(final LockRequest p_request, final Chunk p_chunk) {
			super(p_request, SUBTYPE_LOCK_RESPONSE);

			m_chunk = p_chunk;
		}

		// Getters
		/**
		 * Get the requested Chunk
		 * @return the requested Chunk
		 */
		public final Chunk getChunk() {
			return m_chunk;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunk(p_buffer, m_chunk);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunk = InputHelper.readChunk(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkWriteLength(m_chunk);
		}

	}

	/**
	 * Request for unlocking a Chunk on a remote node
	 * @author Florian Klein 09.03.2012
	 */
	public static class UnlockMessage extends AbstractMessage {

		// Attributes
		private long m_chunkID;

		// Constructors
		/**
		 * Creates an instance of UnlockRequest
		 */
		public UnlockMessage() {
			super();

			m_chunkID = ChunkID.INVALID_ID;
		}

		/**
		 * Creates an instance of UnlockRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunkID
		 *            The ID of the Chunk to unlock
		 */
		public UnlockMessage(final short p_destination, final long p_chunkID) {
			super(p_destination, TYPE, SUBTYPE_UNLOCK_MESSAGE);

			ChunkID.check(p_chunkID);

			m_chunkID = p_chunkID;
		}

		// Getters
		/**
		 * Get the ID of the Chunk to lock
		 * @return the ID of the Chunk to lock
		 */
		public final long getChunkID() {
			return m_chunkID;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkID(p_buffer, m_chunkID);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunkID = InputHelper.readChunkID(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDWriteLength();
		}

	}

	/**
	 * Request for storing a Chunk on a remote node after migration
	 * @author Florian Klein 09.03.2012
	 */
	public static class DataRequest extends AbstractRequest {

		// Attributes
		private Chunk[] m_chunks;

		// Constructors
		/**
		 * Creates an instance of DataRequest
		 */
		public DataRequest() {
			super();

			m_chunks = null;
		}

		/**
		 * Creates an instance of DataRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunk
		 *            a single Chunk to store
		 */
		public DataRequest(final short p_destination, final Chunk p_chunk) {
			super(p_destination, TYPE, SUBTYPE_DATA_REQUEST);

			Contract.checkNotNull(p_chunk, "no chunk given");

			m_chunks = new Chunk[] {p_chunk};
		}

		/**
		 * Creates an instance of DataRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunks
		 *            the Chunks to store
		 */
		public DataRequest(final short p_destination, final Chunk[] p_chunks) {
			super(p_destination, TYPE, SUBTYPE_DATA_REQUEST);

			Contract.checkNotNull(p_chunks, "no chunks given");

			m_chunks = p_chunks;
		}

		// Getters
		/**
		 * Get the Chunks to store
		 * @return the Chunks to store
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

	/**
	 * Response to a DataRequest
	 * @author Florian Klein 09.03.2012
	 */
	public static class DataResponse extends AbstractResponse {

		// Constructors
		/**
		 * Creates an instance of DataResponse
		 */
		public DataResponse() {
			super();
		}

		/**
		 * Creates an instance of DataResponse
		 * @param p_request
		 *            the request
		 */
		public DataResponse(final DataRequest p_request) {
			super(p_request, SUBTYPE_DATA_RESPONSE);
		}

	}

	/**
	 * Message for storing a Chunk on a remote node after migration
	 * @author Florian Klein 09.03.2012
	 */
	public static class DataMessage extends AbstractMessage {

		// Attributes
		private Chunk[] m_chunks;

		// Constructors
		/**
		 * Creates an instance of DataMessage
		 */
		public DataMessage() {
			super();

			m_chunks = null;
		}

		/**
		 * Creates an instance of DataMessage
		 * @param p_destination
		 *            the destination
		 * @param p_chunks
		 *            the Chunks to store
		 */
		public DataMessage(final short p_destination, final Chunk[] p_chunks) {
			super(p_destination, TYPE, SUBTYPE_DATA_MESSAGE);

			Contract.checkNotNull(p_chunks, "no chunks given");

			m_chunks = p_chunks;
		}

		// Getters
		/**
		 * Get the Chunks to store
		 * @return the Chunks to store
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

	/**
	 * Request for getting multiple Chunks from a remote node
	 * @author Florian Klein 05.07.2014
	 */
	public static class MultiGetRequest extends AbstractRequest {

		// Attributes
		private long[] m_chunkIDs;

		// Constructors
		/**
		 * Creates an instance of MultiGetRequest
		 */
		public MultiGetRequest() {
			super();

			m_chunkIDs = null;
		}

		/**
		 * Creates an instance of MultiGetRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunkIDs
		 *            The IDs of the Chunk to get
		 */
		public MultiGetRequest(final short p_destination, final long... p_chunkIDs) {
			super(p_destination, TYPE, SUBTYPE_MULTIGET_REQUEST);

			Contract.checkNotNull(p_chunkIDs, "no IDs given");

			ChunkID.check(p_chunkIDs);

			m_chunkIDs = p_chunkIDs;
		}

		// Getters
		/**
		 * Get the IDs of the Chunks to get
		 * @return the IDs of the Chunks to get
		 */
		public final long[] getChunkIDs() {
			return m_chunkIDs;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeChunkIDs(p_buffer, m_chunkIDs);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_chunkIDs = InputHelper.readChunkIDs(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getChunkIDsWriteLength(m_chunkIDs.length);
		}

	}

	/**
	 * Response to a MultiGetRequest
	 * @author Florian Klein 05.07.2014
	 */
	public static class MultiGetResponse extends AbstractResponse {

		// Attributes
		private Chunk[] m_chunks;

		// Constructors
		/**
		 * Creates an instance of MultiGetResponse
		 */
		public MultiGetResponse() {
			super();

			m_chunks = null;
		}

		/**
		 * Creates an instance of MultiGetResponse
		 * @param p_request
		 *            the corresponding MultiGetRequest
		 * @param p_chunks
		 *            the requested Chunks
		 */
		public MultiGetResponse(final MultiGetRequest p_request, final Chunk... p_chunks) {
			super(p_request, SUBTYPE_MULTIGET_RESPONSE);

			Contract.checkNotNull(p_chunks, "no chunks given");

			m_chunks = p_chunks;
		}

		// Getters
		/**
		 * Get the requested Chunks
		 * @return the requested Chunks
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

	/**
	 * Message for command
	 * @author Michael Schoettner 12.08.2015
	 */
	public static class ChunkCommandMessage extends AbstractMessage {

		// Attributes
		private String m_cmd;

		// Constructors
		/**
		 * Creates an instance of CommandMessage
		 */
		public ChunkCommandMessage() {
			super();

			m_cmd = null;
		}

		/**
		 * Creates an instance of CommandMessage
		 * @param p_destination
		 *            the destination
		 * @param p_cmd
		 *            the command
		 */
		public ChunkCommandMessage(final short p_destination, final String p_cmd) {
			super(p_destination, TYPE, SUBTYPE_CHUNK_COMMAND_MESSAGE);
			Contract.checkNotNull(p_cmd, "no command given");
			m_cmd = p_cmd;
		}

		// Getters
		/**
		 * Get the command
		 * @return the command
		 */
		public final String getCommand() {
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
	 * Request for command
	 * @author Michael Schoettner 20.8.2015
	 */
	public static class ChunkCommandRequest extends AbstractRequest {

		// Attributes
		private String m_cmd;

		// Constructors
		/**
		 * Creates an instance of CommandRequest
		 */
		public ChunkCommandRequest() {
			super();
			m_cmd = null;
		}

		/**
		 * Creates an instance of CommandRequest
		 * @param p_destination
		 *            the destination
		 * @param p_cmd
		 *            the command
		 */
		public ChunkCommandRequest(final short p_destination, final String p_cmd) {
			super(p_destination, TYPE, SUBTYPE_CHUNK_COMMAND_REQUEST);
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
	 * Response to a CommandRequest
	 * @author Florian Klein 05.07.2014
	 */
	public static class ChunkCommandResponse extends AbstractResponse {

		// Attributes
		private String m_answer;

		// Constructors
		/**
		 * Creates an instance of CommandResponse
		 */
		public ChunkCommandResponse() {
			super();

			m_answer = null;
		}

		/**
		 * Creates an instance of CommandResponse
		 * @param p_request
		 *            the corresponding CommandRequest
		 * @param p_answer
		 *            the answer
		 */
		public ChunkCommandResponse(final ChunkCommandRequest p_request, final String p_answer) {
			super(p_request, SUBTYPE_CHUNK_COMMAND_RESPONSE);

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

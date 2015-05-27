
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
	public static final byte SUBTYPE_INIT_REQUEST = 4;
	public static final byte SUBTYPE_INIT_RESPONSE = 5;

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
		private Chunk m_chunk;

		// Constructors
		/**
		 * Creates an instance of LogRequest
		 */
		public LogRequest() {
			super();

			m_chunk = null;
		}

		/**
		 * Creates an instance of LogRequest
		 * @param p_destination
		 *            the destination
		 * @param p_chunk
		 *            the chunk to log
		 */
		public LogRequest(final short p_destination, final Chunk p_chunk) {
			super(p_destination, TYPE, SUBTYPE_LOG_REQUEST);

			m_chunk = p_chunk;
		}

		// Getters
		/**
		 * Get the Chunk to log
		 * @return the Chunk to log
		 */
		public final Chunk getChunk() {
			Chunk ret = null;

			if (m_chunk.getSize() != 0) {
				ret = m_chunk;
			}

			return ret;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			if (m_chunk != null) {
				OutputHelper.writeChunk(p_buffer, m_chunk);
			} else {
				OutputHelper.writeChunk(p_buffer, new Chunk(0, new byte[0]));
			}
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
		private Chunk m_chunk;

		// Constructors
		/**
		 * Creates an instance of LogMessage
		 */
		public LogMessage() {
			super();

			m_chunk = null;
		}

		/**
		 * Creates an instance of LogMessage
		 * @param p_destination
		 *            the destination
		 * @param p_chunk
		 *            the Chunk to store
		 */
		public LogMessage(final short p_destination, final Chunk p_chunk) {
			super(p_destination, TYPE, SUBTYPE_LOG_MESSAGE);

			Contract.checkNotNull(p_chunk, "no chunk given");

			m_chunk = p_chunk;
		}

		// Getters
		/**
		 * Get the Chunk to store
		 * @return the Chunk to store
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
	 * Request for initialization of a backup range on a remote node
	 * @author Kevin Beineke 20.04.2014
	 */
	public static class InitRequest extends AbstractRequest {

		// Attributes
		private long m_start;

		// Constructors
		/**
		 * Creates an instance of InitRequest
		 */
		public InitRequest() {
			super();

			m_start = 0;
		}

		/**
		 * Creates an instance of InitRequest
		 * @param p_destination
		 *            the destination
		 * @param p_start
		 *            the beginning of the range
		 */
		public InitRequest(final short p_destination, final long p_start) {
			super(p_destination, TYPE, SUBTYPE_INIT_REQUEST);

			m_start = p_start;
		}

		// Getters
		/**
		 * Get the beginning of the range
		 * @return the ChunkID
		 */
		public final long getStartCID() {
			return m_start;
		}

		// Methods
		@Override
		protected final void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeLong(p_buffer, m_start);
		}

		@Override
		protected final void readPayload(final ByteBuffer p_buffer) {
			m_start = InputHelper.readLong(p_buffer);
		}

		@Override
		protected final int getPayloadLength() {
			return OutputHelper.getLongWriteLength();
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

}

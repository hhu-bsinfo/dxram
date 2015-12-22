package de.uniduesseldorf.dxram.core.cmd.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.ChunkCommandRequest;
import de.uniduesseldorf.dxram.core.io.InputHelper;
import de.uniduesseldorf.dxram.core.io.OutputHelper;

import de.uniduesseldorf.menet.AbstractMessage;
import de.uniduesseldorf.menet.AbstractRequest;
import de.uniduesseldorf.menet.AbstractResponse;
import de.uniduesseldorf.utils.Contract;

public class CommandMessages {

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
	public ChunkCommandMessage(final short p_source, final short p_destination, final String p_cmd) {
		super(p_source, p_destination, TYPE, SUBTYPE_CHUNK_COMMAND_MESSAGE);
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
	protected final int getPayloadLengthForWrite() {
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
	protected final int getPayloadLengthForWrite() {
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
	protected final int getPayloadLengthForWrite() {
		return OutputHelper.getStringsWriteLength(m_answer);
	}

}
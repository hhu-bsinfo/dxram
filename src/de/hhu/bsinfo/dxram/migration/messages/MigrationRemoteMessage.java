
package de.hhu.bsinfo.dxram.migration.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Sends a Migration Message which requests a remote migration request
 * @author Mike
 */
public class MigrationRemoteMessage extends AbstractMessage {

	// Attributes
	private long m_chunkID;
	private short m_target;

	// Constructors

	/**
	 * Created because compiler
	 */
	public MigrationRemoteMessage() {
		super();
	}

	/**
	 * Creates an instance of LogMessage
	 */
	public MigrationRemoteMessage(final short p_destination, final long p_cid, final short p_target) {

		super(p_destination, DXRAMMessageTypes.MIGRATION_MESSAGES_TYPE,
				MigrationMessages.SUBTYPE_MIGRATION_REMOTE_MESSAGE);

		m_chunkID = p_cid;
		m_target = p_target;

	}

	public long getChunkID() {
		return m_chunkID;
	}

	/**
	 * get node id to migrate to
	 * @return node id to migrate to
	 */
	public short getTargetNode() {
		return m_target;
	}

	// Network Data Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {

		p_buffer.putLong(m_chunkID);
		p_buffer.putShort(m_target);

	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {

		m_chunkID = p_buffer.getLong();
		m_target = p_buffer.getShort();

	}

	@Override
	protected final int getPayloadLength() {

		return Long.BYTES + Short.BYTES;
	}
}
package de.uniduesseldorf.dxram.core.migrate.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a DataRequest
 * @author Florian Klein 09.03.2012
 */
public class MigrateResponse extends AbstractResponse {

	private long[] m_failedMigrationIDs = null;
	
	// Constructors
	/**
	 * Creates an instance of DataResponse
	 */
	public MigrateResponse() {
		super();
	}

	/**
	 * Creates an instance of DataResponse
	 * @param p_request
	 *            the request
	 */
	public MigrateResponse(final MigrateRequest p_request, final long[] p_failedMigrationIDs) {
		super(p_request, MigrationMessages.SUBTYPE_MIGRATE_RESPONSE);
		
		m_failedMigrationIDs = p_failedMigrationIDs;
	}
	
	public long[] getFailedMigrationIDs()
	{
		return m_failedMigrationIDs;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_failedMigrationIDs.length);
		
		for (long failedID : m_failedMigrationIDs) {
			p_buffer.putLong(failedID);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_failedMigrationIDs = new long[p_buffer.getInt()];
		
		for (int i = 0; i < m_failedMigrationIDs.length; i++) {
			m_failedMigrationIDs[i] = p_buffer.getLong();
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + m_failedMigrationIDs.length * Long.BYTES;
	}
}

package de.uniduesseldorf.dxram.core.migrate.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.ByteBufferDataStructureReaderWriter;
import de.uniduesseldorf.dxram.core.mem.Chunk;
import de.uniduesseldorf.dxram.core.mem.DataStructure;

import de.uniduesseldorf.menet.AbstractRequest;

/**
 * Request for storing a Chunk on a remote node after migration
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class MigrateRequest extends AbstractRequest {

	// data structure is used when request is sent
	private DataStructure[] m_dataStructures = null;
	// chunks are used when request is received
	private Chunk[] m_chunks = null;

	/**
	 * Creates an instance of DataRequest.
	 * This constructor is used when receiving this message.
	 */
	public MigrateRequest() {
		super();
	}

	/**
	 * Creates an instance of DataRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination
	 * @param p_dataStructure
	 *            The data structure to migrate.
	 */
	public MigrateRequest(final short p_destination, final DataStructure p_dataStructure) {
		super(p_destination, MigrationMessages.TYPE, MigrationMessages.SUBTYPE_MIGRATE_REQUEST);

		m_dataStructures = new DataStructure[] {p_dataStructure};
	}

	/**
	 * Creates an instance of DataRequest
	 * @param p_destination
	 *            the destination
	 * @param p_dataStructures
	 *            Multiple data structures to migrate
	 */
	public MigrateRequest(final short p_destination, final DataStructure[] p_dataStructures) {
		super(p_destination, MigrationMessages.TYPE, MigrationMessages.SUBTYPE_MIGRATE_REQUEST);

		m_dataStructures = p_dataStructures;
	}

	/**
	 * Get the Chunks to store
	 * @return the Chunks to store
	 */
	public final Chunk[] getChunks() {
		return m_chunks;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ByteBufferDataStructureReaderWriter dataStructureWriter = new ByteBufferDataStructureReaderWriter(p_buffer);
		
		p_buffer.putInt(m_dataStructures.length);
		for (DataStructure dataStructure : m_dataStructures)
		{
			p_buffer.putLong(dataStructure.getID());
			dataStructure.writePayload(0, dataStructureWriter);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		ByteBufferDataStructureReaderWriter dataStructureReader = new ByteBufferDataStructureReaderWriter(p_buffer);
		
		m_chunks = new Chunk[p_buffer.getInt()];
		
		for (int i = 0; i < m_chunks.length; i++)
		{
			m_chunks[i] = new Chunk(p_buffer.getLong());
			m_chunks[i].readPayload(0, dataStructureReader);
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		int length = 0;
		length += Long.BYTES * m_dataStructures.length;
		for (DataStructure dataStructure : m_dataStructures)
			length += dataStructure.sizeofPayload();
		
		return length;
	}

}

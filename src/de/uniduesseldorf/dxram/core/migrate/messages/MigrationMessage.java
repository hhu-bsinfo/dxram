package de.uniduesseldorf.dxram.core.migrate.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.ByteBufferDataStructureReaderWriter;
import de.uniduesseldorf.dxram.core.mem.Chunk;
import de.uniduesseldorf.dxram.core.mem.DataStructure;

import de.uniduesseldorf.menet.AbstractMessage;

/**
 * Message for storing a Chunk on a remote node after migration
 * @author Florian Klein 09.03.2012
 */
public class MigrationMessage extends AbstractMessage {

	// Attributes
	private DataStructure[] m_dataStructures = null;
	
	// Constructors
	/**
	 * Creates an instance of DataMessage
	 */
	public MigrationMessage() {
		super();
	}

	/**
	 * Creates an instance of DataMessage
	 * @param p_destination
	 *            the destination
	 * @param p_chunks
	 *            the Chunks to store
	 * @param p_versions
	 *            the versions of the Chunks
	 */
	public MigrationMessage(final short p_destination, final DataStructure[] p_dataStructures) {
		super(p_destination, MigrationMessages.TYPE, MigrationMessages.SUBTYPE_MIGRATION_MESSAGE);

		m_dataStructures = p_dataStructures;
	}

	// Getters
	/**
	 * Get the DataStructures to store
	 * @return the DataStructures to store
	 */
	public final DataStructure[] getDataStructures() {
		return m_dataStructures;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		
		p_buffer.putInt(m_dataStructures.length);
		ByteBufferDataStructureReaderWriter dataStructureWriter = new ByteBufferDataStructureReaderWriter(p_buffer);
		for (DataStructure dataStructure : m_dataStructures) {
			p_buffer.putLong(dataStructure.getID());
			p_buffer.putInt(dataStructure.sizeofPayload());
			dataStructure.writePayload(0, dataStructureWriter);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		ByteBufferDataStructureReaderWriter dataStructureReader = new ByteBufferDataStructureReaderWriter(p_buffer);
		
		m_dataStructures = new Chunk[p_buffer.getInt()];
		
		for (int i = 0; i < m_dataStructures.length; i++) {
			long id = p_buffer.getLong();
			int size = p_buffer.getInt();
			
			m_dataStructures[i] = new Chunk(id, size);
			m_dataStructures[i].readPayload(0, size, dataStructureReader);
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		int size = Integer.BYTES;
		
		size += m_dataStructures.length * Long.BYTES;
		size += m_dataStructures.length * Integer.BYTES;
		
		for (DataStructure dataStructure : m_dataStructures) {
			size += dataStructure.sizeofPayload();
		}
		
		return size;
	}

}
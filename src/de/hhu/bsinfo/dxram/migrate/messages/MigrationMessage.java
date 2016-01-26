package de.hhu.bsinfo.dxram.migrate.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message for storing a Chunk on a remote node after migration
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
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
	 * @param p_dataStructures
	 *            the DataStructures to store
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
		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
		for (DataStructure dataStructure : m_dataStructures) {
			int size = dataStructure.sizeofObject();
			
			p_buffer.putLong(dataStructure.getID());
			exporter.setPayloadSize(size);
			p_buffer.putInt(size);
			exporter.exportObject(dataStructure);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
		
		m_dataStructures = new Chunk[p_buffer.getInt()];
		
		for (int i = 0; i < m_dataStructures.length; i++) {
			long id = p_buffer.getLong();
			int size = p_buffer.getInt();
			
			importer.setPayloadSize(size);
			m_dataStructures[i] = new Chunk(id, size);
			importer.importObject(m_dataStructures[i]);
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		int size = Integer.BYTES;
		
		size += m_dataStructures.length * Long.BYTES;
		size += m_dataStructures.length * Integer.BYTES;
		
		for (DataStructure dataStructure : m_dataStructures) {
			size += dataStructure.sizeofObject();
		}
		
		return size;
	}

}
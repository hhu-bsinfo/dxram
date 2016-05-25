package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Request to put data into the superpeer storage.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.05.15
 */
public class SuperpeerStoragePutRequest extends AbstractRequest {
	// DataStructures used when sending the put request.
	// These are also used by the response to directly write the
	// receiving data to the structures
	// Chunks are created and used when receiving a put request
	private DataStructure m_dataStructure;
	private Chunk m_chunk;

	private boolean m_isReplicate;

	/**
	 * Creates an instance of SuperpeerStoragePutRequest.
	 * This constructor is used when receiving this message.
	 */
	public SuperpeerStoragePutRequest() {
		super();
	}

	/**
	 * Creates an instance of SuperpeerStoragePutRequest
	 *
	 * @param p_destination   the destination
	 * @param p_dataStructure Data structure with the data to put.
	 * @param p_replicate     True if this message is a replication to other superpeer message, false if normal message
	 */
	public SuperpeerStoragePutRequest(final short p_destination, final DataStructure p_dataStructure,
			boolean p_replicate) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_REQUEST);

		m_dataStructure = p_dataStructure;
		m_isReplicate = p_replicate;
	}

	/**
	 * Get the Chunks to put when this message is received.
	 *
	 * @return the Chunks to put
	 */
	public final Chunk getChunk() {
		return m_chunk;
	}

	/**
	 * Check if this request is a replicate message.
	 *
	 * @return True if replicate message.
	 */
	public boolean isReplicate() {
		return m_isReplicate;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
		int size = m_dataStructure.sizeofObject();

		p_buffer.putLong(m_dataStructure.getID());
		exporter.setPayloadSize(size);
		p_buffer.putInt(size);
		p_buffer.order(ByteOrder.nativeOrder());
		exporter.exportObject(m_dataStructure);
		p_buffer.order(ByteOrder.BIG_ENDIAN);
		p_buffer.put((byte) (m_isReplicate ? 1 : 0));
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);

		m_chunk = new Chunk();

		long id = p_buffer.getLong();
		int size = p_buffer.getInt();

		importer.setPayloadSize(size);
		m_chunk = new Chunk(id, size);
		p_buffer.order(ByteOrder.nativeOrder());
		importer.importObject(m_chunk);
		p_buffer.order(ByteOrder.BIG_ENDIAN);
		m_isReplicate = p_buffer.get() != 0;
	}

	@Override
	protected final int getPayloadLength() {
		int size = ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode());

		if (m_dataStructure != null) {
			size += Long.BYTES + Integer.BYTES + m_dataStructure.sizeofObject() + Byte.BYTES;
		} else {
			size += Long.BYTES + Integer.BYTES + m_chunk.sizeofObject() + Byte.BYTES;
		}

		return size;
	}
}

package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.menet.AbstractRequest;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
	private DataStructure[] m_dataStructures;

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
	 * @param p_destination    the destination
	 * @param p_dataStructures Data structure with the data to put.
	 */
	public SuperpeerStoragePutRequest(final short p_destination, final DataStructure... p_dataStructures) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_REQUEST);

		m_dataStructures = p_dataStructures;

		byte tmpCode = getStatusCode();
		setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(tmpCode, p_dataStructures.length));
	}

	/**
	 * Get the DataStructures to put when this message is received.
	 *
	 * @return the Chunk to put
	 */
	public final DataStructure[] getDataStructures() {
		return m_dataStructures;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_dataStructures.length);

		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
		for (DataStructure dataStructure : m_dataStructures) {
			int size = dataStructure.sizeofObject();

			p_buffer.putLong(dataStructure.getID());
			exporter.setPayloadSize(size);
			p_buffer.putInt(size);
			p_buffer.order(ByteOrder.nativeOrder());
			exporter.exportObject(dataStructure);
			p_buffer.order(ByteOrder.BIG_ENDIAN);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
		int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

		m_dataStructures = new Chunk[numChunks];

		for (int i = 0; i < m_dataStructures.length; i++) {
			long id = p_buffer.getLong();
			int size = p_buffer.getInt();

			importer.setPayloadSize(size);
			m_dataStructures[i] = new Chunk(id, size);
			p_buffer.order(ByteOrder.nativeOrder());
			importer.importObject(m_dataStructures[i]);
			p_buffer.order(ByteOrder.BIG_ENDIAN);
		}
	}

	@Override
	protected final int getPayloadLength() {
		int size = ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode());

		size += m_dataStructures.length * Long.BYTES;
		size += m_dataStructures.length * Integer.BYTES;

		for (DataStructure dataStructure : m_dataStructures) {
			size += dataStructure.sizeofObject();
		}

		return size;
	}
}

package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.menet.AbstractRequest;

import java.nio.ByteBuffer;

/**
 * Request to get data from the superpeer storage.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.05.15
 */
public class SuperpeerStorageGetRequest extends AbstractRequest {
	// the data structure is stored for the sender of the request
	// to write the incoming data of the response to it
	// the requesting IDs are taken from the structures
	private DataStructure[] m_dataStructures;
	// this is only used when receiving the request
	private long[] m_storageIDs;

	/**
	 * Creates an instance of SuperpeerStorageGetRequest.
	 * This constructor is used when receiving this message.
	 */
	public SuperpeerStorageGetRequest() {
		super();
	}

	/**
	 * Creates an instance of SuperpeerStorageGetRequest.
	 * This constructor is used when sending this message.
	 *
	 * @param p_destination    the destination node id.
	 * @param p_dataStructures Data structure with the ID of the chunk to get.
	 */
	public SuperpeerStorageGetRequest(final short p_destination, final DataStructure... p_dataStructures) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_REQUEST);

		m_dataStructures = p_dataStructures;

		byte tmpCode = getStatusCode();
		setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(tmpCode, p_dataStructures.length));
	}

	/**
	 * Get the chunk IDs of this request (when receiving it).
	 *
	 * @return Chunk ID.
	 */
	public long[] getStorageIDs() {
		return m_storageIDs;
	}

	/**
	 * Get the data structures stored with this request.
	 * This is used to write the received data to the provided object to avoid
	 * using multiple buffers.
	 *
	 * @return Data structures to store data to when the response arrived.
	 */
	public DataStructure[] getDataStructures() {
		return m_dataStructures;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_dataStructures.length);

		for (DataStructure dataStructure : m_dataStructures) {
			p_buffer.putLong(dataStructure.getID());
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

		m_storageIDs = new long[numChunks];
		for (int i = 0; i < m_storageIDs.length; i++) {
			m_storageIDs[i] = p_buffer.getLong();
		}
	}

	@Override
	protected final int getPayloadLength() {
		if (m_dataStructures != null) {
			return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode())
					+ Long.BYTES * m_dataStructures.length;
		} else {
			return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode())
					+ Long.BYTES * m_storageIDs.length;
		}
	}
}

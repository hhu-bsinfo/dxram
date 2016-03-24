package de.hhu.bsinfo.dxram.nameservice;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;

/**
 * Nameservice service providing mappings of string identifiers to chunkIDs.
 * Note: The character set and length of the string are limited. Refer to 
 * the convert class for details.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NameserviceService extends DXRAMService {

	private LoggerComponent m_logger;
	private LookupComponent m_lookup;
	
	private NameServiceStringConverter m_converter = null;
	
	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {
		p_settings.setDefaultValue(NameserviceConfigurationValues.Component.TYPE);
	}

	@Override
	protected boolean startService(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_logger = getComponent(LoggerComponent.class);
		m_lookup = getComponent(LookupComponent.class);
		
		m_converter = new NameServiceStringConverter(p_settings.getValue(NameserviceConfigurationValues.Component.TYPE));
		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_lookup = null;
		m_converter = null;
		return true;
	}

	/**
	 * Register a DataStructure for a specific name.
	 * @param p_dataStructure DataStructure to register.
	 * @param p_name Name to associate with the ID of the DataStructure.
	 */
	public void register(final DataStructure p_dataStructure, final String p_name) {
		int id = m_converter.convert(p_name);
		m_logger.trace(getClass(), "Registering chunkID 0x" + Long.toHexString(p_dataStructure.getID()) + ", name " + p_name + ", id " + id);
		
		m_lookup.insertID(id, p_dataStructure.getID());
	}
	
	/**
	 * Get the chunk ID of the specific name from the service.
	 * @param p_name Registered name to get the chunk ID for.
	 * @return If the name was registered with a chunk ID before, returns the chunk ID, null otherwise.
	 */
	public long getChunkID(final String p_name) {
		int id = m_converter.convert(p_name);
		m_logger.trace(getClass(), "Lookup name " + p_name + ", id " + id);
		
		long ret = m_lookup.getChunkID(id);
		
		m_logger.trace(getClass(), "Lookup name " + p_name + ", resulting chunkID 0x" + Long.toHexString(ret));
		
		return ret;
	}
	
	/**
	 * Remove the name of a registered DataStructure from lookup.
	 * @param p_dataStructure DataStructure/Chunk ID to remove the name entry of.
	 */
	public void remove(final DataStructure p_dataStructure) {
		m_lookup.remove(p_dataStructure.getID());
	}
	
//	/**
//	 * Inserts given ChunkID with key in index chunk
//	 * @param p_key
//	 *            the key to be inserted
//	 * @param p_chunkID
//	 *            the ChunkID to be inserted
//	 * @param p_chunk
//	 *            the index chunk
//	 * @throws DXRAMException
//	 *             if there is no fitting chunk
//	 */
//	private void insertMapping(final int p_key, final long p_chunkID, final Chunk p_chunk) {
//		int size;
//		ByteBuffer indexData;
//		ByteBuffer appendixData;
//		Chunk indexChunk = p_chunk;
//		Chunk appendix;
//
//		// Iterate over index chunks to get to the last one
//		while (true) {
//			// Get data and number of written bytes of current index chunk
//			indexData = indexChunk.getData();
//			size = indexData.getInt(0);
//
//			if (-1 != indexData.getInt(indexData.capacity() - 12)) {
//				// This is the last index chunk
//				if (24 <= indexData.capacity() - size) {
//					// If there is at least 24 Bytes (= two entries; the last one of every index file is needed
//					// to address the next index chunk) left in this index chunk, the new entry will be appended
//
//					// Set position on first unwritten byte and add <ID, ChunkID>
//					indexData.position(size);
//					indexData.putInt(p_key);
//					indexData.putLong(p_chunkID);
//					indexData.putInt(0, size + 12);
//					put(indexChunk);
//				} else {
//					// The last index chunk is full -> create new chunk and add its address to the old one
//					appendix = create(INDEX_SIZE);
//					appendixData = appendix.getData();
//					appendixData.putInt(4 + 12);
//					appendixData.putInt(p_key);
//					appendixData.putLong(p_chunkID);
//					put(appendix);
//
//					indexData.position(indexData.capacity() - 12);
//					indexData.putInt(-1);
//					indexData.putLong(appendix.getChunkID());
//					put(indexChunk);
//				}
//				break;
//			}
//			// Get next index file and repeat
//			indexChunk = get(indexData.getLong(indexData.capacity() - 8));
//		}
//	}
//
//	/**
//	 * Removes given key in index chunk
//	 * @param p_key
//	 *            the key to be removed
//	 * @param p_chunk
//	 *            the index chunk
//	 * @throws DXRAMException
//	 *             if there is no fitting chunk
//	 */
//	@SuppressWarnings("unused")
//	private void removeMapping(final int p_key, final Chunk p_chunk) {
//		int j = 0;
//		int id;
//		int size;
//		ByteBuffer indexData;
//		ByteBuffer lastIndexData;
//		Chunk indexChunk;
//		Chunk lastIndexChunk;
//		Chunk predecessorChunk = null;
//
//		indexChunk = p_chunk;
//		indexData = indexChunk.getData();
//
//		while (true) {
//			// Get j-th ID from index chunk
//			id = indexData.getInt(j * 12 + 4);
//			if (id == p_key) {
//				// ID is found -> remove entry
//				if (-1 != indexData.getInt(indexData.capacity() - 12)) {
//					// Deletion in last index chunk
//					deleteEntryInLastIndexFile(indexChunk, predecessorChunk, j);
//				} else {
//					// Deletion in index file with successor -> replace entry with last entry in whole list
//					// Find last index chunk
//					lastIndexChunk = indexChunk;
//					lastIndexData = indexData;
//					while (-1 == lastIndexData.getInt(lastIndexData.capacity() - 12)) {
//						predecessorChunk = lastIndexChunk;
//						lastIndexChunk = get(lastIndexData.getLong(lastIndexData.capacity() - 8));
//						lastIndexData = lastIndexChunk.getData();
//					}
//					// Replace entry that should be removed with last entry from last index file
//					size = lastIndexData.getInt(0);
//					indexData.putInt(j * 12 + 4, lastIndexData.getInt(size - 12));
//					indexData.putLong(j * 12 + 4 + 4, lastIndexData.getLong(size - 8));
//					put(indexChunk);
//					// Remove last entry from last index file
//					deleteEntryInLastIndexFile(lastIndexChunk, predecessorChunk, (size - 4) / 12 - 1);
//				}
//				break;
//			} else if (id == -1) {
//				// Get next user index and remember current chunk (may be needed for deletion)
//				predecessorChunk = indexChunk;
//				indexChunk = get(indexData.getLong(indexData.capacity() - 8));
//				indexData = indexChunk.getData();
//				j = 0;
//				continue;
//			}
//			j++;
//		}
//	}
//	
//	/**
//	 * Deletes the entry with given index in index chunk
//	 * @param p_indexChunk
//	 *            the index chunk
//	 * @param p_predecessorChunk
//	 *            the parent index chunk
//	 * @param p_index
//	 *            the index
//	 * @throws DXRAMException
//	 *             if there is no fitting chunk
//	 */
//	private void deleteEntryInLastIndexFile(final Chunk p_indexChunk, final Chunk p_predecessorChunk, final int p_index) {
//		int size;
//		byte[] data1;
//		byte[] data2;
//		byte[] allData;
//		ByteBuffer indexData;
//
//		// Get data and size of last index file
//		indexData = p_indexChunk.getData();
//		size = indexData.getInt(0);
//
//		if (size > 16) {
//			// If there is more than one entry -> shift all entries 12 Bytes to the left beginning
//			// at p_index to overwrite entry
//			data1 = new byte[p_index * 12 + 4];
//			data2 = new byte[size - (p_index * 12 + 4 + 12)];
//			allData = new byte[size];
//
//			indexData.get(data1, 0, p_index * 12 + 4);
//			indexData.position(p_index * 12 + 4 + 12);
//			indexData.get(data2, 0, size - (p_index * 12 + 4 + 12));
//
//			System.arraycopy(data1, 0, allData, 0, data1.length);
//			System.arraycopy(data2, 0, allData, data1.length, data2.length);
//
//			indexData.position(0);
//			indexData.put(allData);
//			indexData.putInt(0, size - 12);
//			put(p_indexChunk);
//		} else {
//			// There is only one entry in index file
//			if (null != p_predecessorChunk) {
//				// If there is a predecessor, remove current index file and update predecessor
//				remove(p_indexChunk.getChunkID());
//
//				indexData = p_predecessorChunk.getData();
//				indexData.position(indexData.getInt(0));
//				// Overwrite the addressing to predecessor's successor with zeros
//				for (int i = 0; i < 12; i++) {
//					indexData.put((byte) 0);
//				}
//				put(p_predecessorChunk);
//			} else {
//				// If there is no predecessor, the entry to remove is the last entry in list
//				// -> overwrite <ID, ChunkID> with zeros and update size
//				indexData.position(0);
//				indexData.putInt(4);
//				for (int i = 0; i < 12; i++) {
//					indexData.put((byte) 0);
//				}
//				put(p_indexChunk);
//			}
//		}
//	}
}

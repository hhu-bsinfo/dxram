
package de.hhu.bsinfo.dxram.nameservice;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.NameServiceIndexData;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent.MemoryErrorCodes;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Nameservice service providing mappings of string identifiers to chunkIDs.
 * Note: The character set and length of the string are limited. Refer to
 * the convert class for details.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NameserviceService extends AbstractDXRAMService {

	private LoggerComponent m_logger;
	private LookupComponent m_lookup;
	private MemoryManagerComponent m_memoryManager;

	private NameServiceStringConverter m_converter;

	private NameServiceIndexData m_indexData;

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {
		p_settings.setDefaultValue(NameserviceConfigurationValues.Component.TYPE);
	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_logger = getComponent(LoggerComponent.class);
		m_lookup = getComponent(LookupComponent.class);
		m_memoryManager = getComponent(MemoryManagerComponent.class);

		m_converter =
				new NameServiceStringConverter(p_settings.getValue(NameserviceConfigurationValues.Component.TYPE));

		m_indexData = new NameServiceIndexData();

		if (getComponent(AbstractBootComponent.class).getNodeRole() == NodeRole.PEER) {
			m_indexData.setID(m_memoryManager.createIndex(m_indexData.sizeofObject()));
			if (m_indexData.getID() == ChunkID.INVALID_ID) {
				m_logger.error(getClass(), "Creating root index chunk failed.");
				return false;
			}
		}

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_lookup = null;
		m_converter = null;
		return true;
	}

	/**
	 * Register a chunk id for a specific name.
	 * @param p_chunkId
	 *            Chunk id to register.
	 * @param p_name
	 *            Name to associate with the ID of the DataStructure.
	 */
	public void register(final long p_chunkId, final String p_name) {
		final int id = m_converter.convert(p_name);
		m_logger.trace(getClass(), "Registering chunkID " + ChunkID.toHexString(p_chunkId) + ", name "
				+ p_name + ", id " + id);

		m_lookup.insertNameserviceEntry(id, p_chunkId);
		insertMapping(id, p_chunkId);
	}

	/**
	 * Register a DataStructure for a specific name.
	 * @param p_dataStructure
	 *            DataStructure to register.
	 * @param p_name
	 *            Name to associate with the ID of the DataStructure.
	 */
	public void register(final DataStructure p_dataStructure, final String p_name) {
		try {
			final int id = m_converter.convert(p_name);
			m_logger.trace(getClass(), "Registering chunkID " + ChunkID.toHexString(p_dataStructure.getID()) + ", name "
					+ p_name + ", id " + id);

			m_lookup.insertNameserviceEntry(id, p_dataStructure.getID());
			insertMapping(id, p_dataStructure.getID());
		} catch (final IllegalArgumentException e) {
			m_logger.error(getClass(), "Lookup in name service failed", e);
		}
	}

	/**
	 * Get the chunk ID of the specific name from the service.
	 * @param p_name
	 *            Registered name to get the chunk ID for.
	 * @return If the name was registered with a chunk ID before, returns the chunk ID, -1 otherwise.
	 */
	public long getChunkID(final String p_name) {
		long ret = -1;
		try {
			final int id = m_converter.convert(p_name);
			m_logger.trace(getClass(), "Lookup name " + p_name + ", id " + id);

			ret = m_lookup.getChunkIDForNameserviceEntry(id);

			m_logger.trace(getClass(), "Lookup name " + p_name + ", resulting chunkID " + ChunkID.toHexString(ret));
		} catch (final IllegalArgumentException e) {
			m_logger.error(getClass(), "Lookup in name service failed", e);
		}

		return ret;
	}

	/**
	 * Remove the name of a registered DataStructure from lookup.
	 * @return the number of entries in name service
	 */
	public int getEntryCount() {
		return m_lookup.getNameserviceEntryCount();
	}

	/**
	 * Inserts the nameservice entry to chunk with LocalID 0 for backup
	 * @param p_key
	 *            the key
	 * @param p_chunkID
	 *            the ChunkID
	 * @return whether this operation was successful
	 */
	private boolean insertMapping(final int p_key, final long p_chunkID) {
		if (!m_indexData.insertMapping(p_key, p_chunkID)) {
			// index chunk full, create new one
			final NameServiceIndexData nextIndexChunk = new NameServiceIndexData();
			nextIndexChunk.setID(m_memoryManager.create(nextIndexChunk.sizeofObject()));
			if (nextIndexChunk.getID() == ChunkID.INVALID_ID) {
				m_logger.error(getClass(), "Creating next index chunk failed.");
				return false;
			}

			// link previous to new and update
			m_indexData.setNextIndexDataChunk(nextIndexChunk.getID());
			if (m_memoryManager.put(m_indexData) != MemoryErrorCodes.SUCCESS) {
				m_logger.error(getClass(), "Updating current index chunk with successor failed.");
				return false;
			}

			m_indexData = nextIndexChunk;
		}

		// insert mapping into current chunk and update
		m_indexData.insertMapping(p_key, p_chunkID);
		if (m_memoryManager.put(m_indexData) != MemoryErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Updating current index chunk failed.");
			return false;
		}

		return true;
	}
}

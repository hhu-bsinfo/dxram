
package de.hhu.bsinfo.dxram.nameservice;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.NameServiceIndexData;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.Pair;

/**
 * Nameservice component providing mappings of string identifiers to chunkIDs.
 * Note: The character set and length of the string are limited. Refer to
 * the convert class for details.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NameserviceComponent extends AbstractDXRAMComponent {

	private LoggerComponent m_logger;
	private LookupComponent m_lookup;
	private ChunkComponent m_chunk;

	private NameServiceStringConverter m_converter;

	private NameServiceIndexData m_indexData;
	private Lock m_indexDataLock;

	/**
	 * Constructor
	 * @param p_priorityInit
	 *            Priority for initialization of this component.
	 *            When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown
	 *            Priority for shutting down this component.
	 *            When choosing the order, consider component dependencies here.
	 */
	public NameserviceComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	/**
	 * Register a DataStructure for a specific name.
	 * @param p_dataStructure
	 *            DataStructure to register.
	 * @param p_name
	 *            Name to associate with the ID of the DataStructure.
	 */
	public void register(final DataStructure p_dataStructure, final String p_name) {
		register(p_dataStructure.getID(), p_name);
	}

	/**
	 * Register a chunk id for a specific name.
	 * @param p_chunkId
	 *            Chunk id to register.
	 * @param p_name
	 *            Name to associate with the ID of the DataStructure.
	 */
	public void register(final long p_chunkId, final String p_name) {
		try {
			final int id = m_converter.convert(p_name);
			m_logger.trace(getClass(), "Registering chunkID " + ChunkID.toHexString(p_chunkId) + ", name "
					+ p_name + ", id " + id);

			m_lookup.insertNameserviceEntry(id, p_chunkId);
			insertMapping(id, p_chunkId);
		} catch (final IllegalArgumentException e) {
			m_logger.error(getClass(), "Lookup in name service failed", e);
		}
	}

	/**
	 * Get the chunk ID of the specific name from the service.
	 * @param p_name
	 *            Registered name to get the chunk ID for.
	 * @param p_timeoutMs
	 *            Timeout for trying to get the entry (if it does not exist, yet).
	 *            set this to -1 for infinite loop if you know for sure, that the entry has to exist
	 * @return If the name was registered with a chunk ID before, returns the chunk ID, -1 otherwise.
	 */
	public long getChunkID(final String p_name, final int p_timeoutMs) {
		long ret = -1;
		try {
			final int id = m_converter.convert(p_name);
			m_logger.trace(getClass(), "Lookup name " + p_name + ", id " + id);

			ret = m_lookup.getChunkIDForNameserviceEntry(id, p_timeoutMs);

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
	 * Get all available name mappings
	 * @return List of available name mappings
	 */
	public ArrayList<Pair<String, Long>> getAllEntries() {
		ArrayList<Pair<String, Long>> list = new ArrayList<Pair<String, Long>>();

		ArrayList<Pair<Integer, Long>> entries = m_lookup.getNameserviceEntries();
		if (list != null) {
			// convert index representation
			for (Pair<Integer, Long> entry : entries) {
				list.add(new Pair<String, Long>(m_converter.convert(entry.first()), entry.second()));
			}
		}

		return list;
	}

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(NameserviceConfigurationValues.Component.TYPE);
	}

	@Override
	protected boolean initComponent(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_logger = getDependentComponent(LoggerComponent.class);
		m_lookup = getDependentComponent(LookupComponent.class);
		m_chunk = getDependentComponent(ChunkComponent.class);

		m_converter =
				new NameServiceStringConverter(p_settings.getValue(NameserviceConfigurationValues.Component.TYPE));

		m_indexData = new NameServiceIndexData();

		if (getDependentComponent(AbstractBootComponent.class).getNodeRole() == NodeRole.PEER) {
			m_indexData.setID(m_chunk.createIndexChunk(m_indexData.sizeofObject()));
			if (m_indexData.getID() == ChunkID.INVALID_ID) {
				m_logger.error(getClass(), "Creating root index chunk failed.");
				return false;
			}
		}

		m_indexDataLock = new ReentrantLock(false);

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_lookup = null;
		m_converter = null;
		m_chunk = null;

		m_converter = null;

		m_indexData = null;
		m_indexDataLock = null;

		return true;
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
		m_indexDataLock.lock();
		if (!m_indexData.insertMapping(p_key, p_chunkID)) {
			// index chunk full, create new one
			final NameServiceIndexData nextIndexChunk = new NameServiceIndexData();
			nextIndexChunk.setID(m_chunk.createChunk(nextIndexChunk.sizeofObject()));
			if (nextIndexChunk.getID() == ChunkID.INVALID_ID) {
				m_logger.error(getClass(), "Creating next index chunk failed.");
				return false;
			}

			// link previous to new and update
			m_indexData.setNextIndexDataChunk(nextIndexChunk.getID());
			if (!m_chunk.putChunk(m_indexData)) {
				m_logger.error(getClass(), "Updating current index chunk with successor failed.");
				m_indexDataLock.unlock();
				return false;
			}

			m_indexData = nextIndexChunk;
		}

		// insert mapping into current chunk and update
		m_indexData.insertMapping(p_key, p_chunkID);
		if (!m_chunk.putChunk(m_indexData)) {
			m_logger.error(getClass(), "Updating current index chunk failed.");
			m_indexDataLock.unlock();
			return false;
		}

		m_indexDataLock.unlock();
		return true;
	}
}

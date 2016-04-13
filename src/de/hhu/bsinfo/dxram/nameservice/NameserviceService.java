
package de.hhu.bsinfo.dxram.nameservice;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;

/**
 * Nameservice service providing mappings of string identifiers to chunkIDs.
 * Note: The character set and length of the string are limited. Refer to
 * the convert class for details.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NameserviceService extends AbstractDXRAMService {

	private NameserviceComponent m_nameservice;

	/**
	 * Register a chunk id for a specific name.
	 * @param p_chunkId
	 *            Chunk id to register.
	 * @param p_name
	 *            Name to associate with the ID of the DataStructure.
	 */
	public void register(final long p_chunkId, final String p_name) {
		m_nameservice.register(p_chunkId, p_name);
	}

	/**
	 * Register a DataStructure for a specific name.
	 * @param p_dataStructure
	 *            DataStructure to register.
	 * @param p_name
	 *            Name to associate with the ID of the DataStructure.
	 */
	public void register(final DataStructure p_dataStructure, final String p_name) {
		m_nameservice.register(p_dataStructure, p_name);
	}

	/**
	 * Get the chunk ID of the specific name from the service.
	 * @param p_name
	 *            Registered name to get the chunk ID for.
	 * @return If the name was registered with a chunk ID before, returns the chunk ID, -1 otherwise.
	 */
	public long getChunkID(final String p_name) {
		return m_nameservice.getChunkID(p_name);
	}

	/**
	 * Remove the name of a registered DataStructure from lookup.
	 * @param p_dataStructure
	 *            DataStructure/Chunk ID to remove the name entry of.
	 */
	public void remove(final DataStructure p_dataStructure) {
		m_nameservice.remove(p_dataStructure);
	}

	/**
	 * Remove the name of a registered DataStructure from lookup.
	 * @return the number of entries in name service
	 */
	public int getEntryCount() {
		return m_nameservice.getEntryCount();
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_nameservice = getComponent(NameserviceComponent.class);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_nameservice = null;

		return true;
	}
}

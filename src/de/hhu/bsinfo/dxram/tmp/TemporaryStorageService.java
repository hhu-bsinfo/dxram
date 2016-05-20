package de.hhu.bsinfo.dxram.tmp;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.overlay.SuperpeerStorage;
import de.hhu.bsinfo.dxram.nameservice.NameServiceStringConverter;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.dxram.tmp.tcmds.TcmdTmpCreate;
import de.hhu.bsinfo.dxram.tmp.tcmds.TcmdTmpGet;
import de.hhu.bsinfo.dxram.tmp.tcmds.TcmdTmpPut;
import de.hhu.bsinfo.dxram.tmp.tcmds.TcmdTmpRemove;
import de.hhu.bsinfo.dxram.tmp.tcmds.TcmdTmpStatus;

/**
 * This service provides access to a temporary "chunk" storage residing on the
 * superpeers. This storage is intended for storing small amounts of data which
 * are needed for a short time. Thus, this data is not backed up like the chunk
 * data in the ChunkService. However, it is replicated to further superpeers to give
 * a certain degree of fault tolerance.
 * Use this to store results of computations or helper data for computations.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.05.16
 */
public class TemporaryStorageService extends AbstractDXRAMService {

	private LookupComponent m_lookup;
	private TerminalComponent m_terminal;

	private NameServiceStringConverter m_idConverter = new NameServiceStringConverter("NAME");

	/**
	 * Create a unique storage id using the nameservice string converter. This limits the string
	 * length to 5 digits.
	 *
	 * @param p_name Name to convert to a storage id.
	 * @return Storage id.
	 */
	public int generateStorageId(final String p_name) {
		return m_idConverter.convert(p_name);
	}

	/**
	 * Create a block of memory in the superpeer storage.
	 *
	 * @param p_id   Storage id to use to identify the block.
	 * @param p_size Size of the block to allocate
	 * @return True if successful, false on failure (no space, element count exceeded or id used).
	 */
	public boolean create(final int p_id, final int p_size) {
		return m_lookup.superpeerStorageCreate(p_id, p_size);
	}

	/**
	 * Create a block of memory in the superpeer storage.
	 *
	 * @param p_dataStructure Data structure with the storage id assigned to allocate memory for.
	 * @return True if successful, false on failure (no space, element count exceeded or id used).
	 */
	public boolean create(final DataStructure p_dataStructure) {
		return m_lookup.superpeerStorageCreate(p_dataStructure);
	}

	/**
	 * Put data into an allocated block of memory in the superpeer storage.
	 *
	 * @param p_dataStructure Data structure to put with the storage id assigned.
	 * @return True if successful, false otherwise.
	 */
	public boolean put(final DataStructure p_dataStructure) {
		return m_lookup.superpeerStoragePut(p_dataStructure);
	}

	/**
	 * Get data from the superpeer storage.
	 *
	 * @param p_id Id of an allocated block to get the data from.
	 * @return Chunk with the data other null on error.
	 */
	public Chunk get(final int p_id) {
		return m_lookup.superpeerStorageGet(p_id);
	}

	/**
	 * Get data from the superpeer storage.
	 *
	 * @param p_dataStructure Data structure with the storage id assigned to read the data into.
	 * @return True on success, false on failure.
	 */
	public boolean get(final DataStructure p_dataStructure) {
		return m_lookup.superpeerStorageGet(p_dataStructure);
	}

	/**
	 * Remove an allocated block from the superpeer storage.
	 *
	 * @param p_id Storage id identifying the block to remove.
	 * @return True if successful, false otherwise.
	 */
	public boolean remove(final int p_id) {
		return m_lookup.superpeerStorageRemove(p_id);
	}

	/**
	 * Remove an allocated block from the superpeer storage.
	 *
	 * @param p_dataStructure Data structure with the storage id assigned to remove.
	 * @return True if successful, false otherwise.
	 */
	public boolean remove(final DataStructure p_dataStructure) {
		return m_lookup.superpeerStorageRemove(p_dataStructure);
	}

	/**
	 * Get the status of the superpeer storage.
	 *
	 * @return Status of the superpeer storage.
	 */
	public SuperpeerStorage.Status getStatus() {
		return m_lookup.superpeerStorageGetStatus();
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_lookup = getComponent(LookupComponent.class);
		m_terminal = getComponent(TerminalComponent.class);

		m_terminal.registerCommand(new TcmdTmpCreate());
		m_terminal.registerCommand(new TcmdTmpRemove());
		m_terminal.registerCommand(new TcmdTmpGet());
		m_terminal.registerCommand(new TcmdTmpStatus());
		m_terminal.registerCommand(new TcmdTmpPut());

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_lookup = null;
		m_terminal = null;
		return true;
	}
}

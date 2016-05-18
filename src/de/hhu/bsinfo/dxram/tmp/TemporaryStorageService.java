package de.hhu.bsinfo.dxram.tmp;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.overlay.SuperpeerStorage;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.dxram.tmp.tcmds.*;

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

	public boolean create(final int p_id, final int p_size) {
		return m_lookup.superpeerStorageCreate(p_id, p_size);
	}

	public boolean create(final DataStructure p_dataStructure) {
		return m_lookup.superpeerStorageCreate(p_dataStructure);
	}

	public boolean put(final DataStructure p_dataStructure) {
		return m_lookup.superpeerStoragePut(p_dataStructure);
	}

	public Chunk get(final int p_id) {
		return m_lookup.superpeerStorageGet(p_id);
	}

	public boolean get(final DataStructure p_dataStructure) {
		return m_lookup.superpeerStorageGet(p_dataStructure);
	}

	public boolean remove(final int p_id) {
		return m_lookup.superpeerStorageRemove(p_id);
	}

	public boolean remove(final DataStructure p_dataStructure) {
		return m_lookup.superpeerStorageRemove(p_dataStructure);
	}

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

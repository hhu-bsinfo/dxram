/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.nameservice;

import java.util.ArrayList;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Nameservice service providing mappings of string identifiers to chunkIDs.
 * Note: The character set and length of the string are limited. Refer to
 * the convert class for details.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class NameserviceService extends AbstractDXRAMService<NameserviceServiceConfig> {
    // component dependencies
    private NameserviceComponent m_nameservice;

    /**
     * Constructor
     */
    public NameserviceService() {
        super("name", NameserviceServiceConfig.class);
    }

    /**
     * Get the nameservice mode (NAME or ID)
     *
     * @return "NAME" or "ID"
     */
    public String getNameserviceMode() {
        return m_nameservice.getConfig().getType();
    }

    /**
     * Remove the name of a registered AbstractChunk from lookup.
     *
     * @return the number of entries in name service
     */
    public int getEntryCount() {
        return m_nameservice.getEntryCount();
    }

    /**
     * Get all available name mappings
     *
     * @return List of available name mappings
     */
    public ArrayList<NameserviceEntryStr> getAllEntries() {
        return m_nameservice.getAllEntries();
    }

    /**
     * Register a chunk id for a specific name.
     *
     * @param p_chunkId
     *         Chunk id to register.
     * @param p_name
     *         Name to associate with the ID of the AbstractChunk.
     */
    public void register(final long p_chunkId, final String p_name) {
        m_nameservice.register(p_chunkId, p_name);
    }

    /**
     * Register a AbstractChunk for a specific name.
     *
     * @param p_chunk
     *         AbstractChunk to register.
     * @param p_name
     *         Name to associate with the ID of the AbstractChunk.
     */
    public void register(final AbstractChunk p_chunk, final String p_name) {
        register(p_chunk.getID(), p_name);
    }

    /**
     * Get the chunk ID of the specific name from the service.
     *
     * @param p_name
     *         Registered name to get the chunk ID for.
     * @param p_timeoutMs
     *         Timeout for trying to get the entry (if it does not exist, yet).
     *         set this to -1 for infinite loop if you know for sure, that the entry has to exist
     * @return If the name was registered with a chunk ID before, returns the chunk ID, -1 otherwise.
     */
    public long getChunkID(final String p_name, final int p_timeoutMs) {
        return m_nameservice.getChunkID(p_name, p_timeoutMs);
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_nameservice = p_componentAccessor.getComponent(NameserviceComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}

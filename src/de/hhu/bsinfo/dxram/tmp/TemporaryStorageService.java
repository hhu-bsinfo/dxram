/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.tmp;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.SuperpeerStorage;
import de.hhu.bsinfo.dxram.nameservice.NameServiceStringConverter;

/**
 * This service provides access to a temporary "chunk" storage residing on the
 * superpeers. This storage is intended for storing small amounts of data which
 * are needed for a short time. Thus, this data is not backed up like the chunk
 * data in the ChunkService. However, it is replicated to further superpeers to give
 * a certain degree of fault tolerance.
 * Use this to store results of computations or helper data for computations.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 18.05.2016
 */
public class TemporaryStorageService extends AbstractDXRAMService {

    // dependent components
    private LookupComponent m_lookup;

    private NameServiceStringConverter m_idConverter = new NameServiceStringConverter("NAME");

    /**
     * Constructor
     */
    public TemporaryStorageService() {
        super("tmpstore");
    }

    /**
     * Get the status of the superpeer storage.
     *
     * @return Status of the superpeer storage.
     */
    public SuperpeerStorage.Status getStatus() {
        return m_lookup.superpeerStorageGetStatus();
    }

    /**
     * Create a unique storage id using the nameservice string converter. This limits the string
     * length to 5 digits.
     *
     * @param p_name
     *     Name to convert to a storage id.
     * @return Storage id.
     */
    public int generateStorageId(final String p_name) {
        return m_idConverter.convert(p_name);
    }

    /**
     * Create a block of memory in the superpeer storage.
     *
     * @param p_id
     *     Storage id to use to identify the block.
     * @param p_size
     *     Size of the block to allocate
     * @return True if successful, false on failure (no space, element count exceeded or id used).
     */
    public boolean create(final int p_id, final int p_size) {
        return m_lookup.superpeerStorageCreate(p_id, p_size);
    }

    /**
     * Create a block of memory in the superpeer storage.
     *
     * @param p_dataStructure
     *     Data structure with the storage id assigned to allocate memory for.
     * @return True if successful, false on failure (no space, element count exceeded or id used).
     */
    public boolean create(final DataStructure p_dataStructure) {
        return m_lookup.superpeerStorageCreate(p_dataStructure);
    }

    /**
     * Put data into an allocated block of memory in the superpeer storage.
     *
     * @param p_dataStructure
     *     Data structure to put with the storage id assigned.
     * @return True if successful, false otherwise.
     */
    public boolean put(final DataStructure p_dataStructure) {
        return m_lookup.superpeerStoragePut(p_dataStructure);
    }

    /**
     * Get data from the superpeer storage.
     *
     * @param p_dataStructure
     *     Data structure with the storage id assigned to read the data into.
     * @return True on success, false on failure.
     */
    public boolean get(final DataStructure p_dataStructure) {
        return m_lookup.superpeerStorageGet(p_dataStructure);
    }

    /**
     * Remove an allocated block from the superpeer storage.
     *
     * @param p_id
     *     Storage id identifying the block to remove.
     * @return True if successful, false otherwise.
     */
    public boolean remove(final int p_id) {
        return m_lookup.superpeerStorageRemove(p_id);
    }

    /**
     * Remove an allocated block from the superpeer storage.
     *
     * @param p_dataStructure
     *     Data structure with the storage id assigned to remove.
     * @return True if successful, false otherwise.
     */
    public boolean remove(final DataStructure p_dataStructure) {
        return m_lookup.superpeerStorageRemove(p_dataStructure);
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}

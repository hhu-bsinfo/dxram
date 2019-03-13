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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponentConfig;
import de.hhu.bsinfo.dxram.chunk.ChunkIndexComponent;
import de.hhu.bsinfo.dxram.engine.Component;
import de.hhu.bsinfo.dxram.engine.Inject;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.ComponentProvider;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.NameserviceEntry;

/**
 * Nameservice component providing mappings of string identifiers to chunkIDs.
 * Note: The character set and length of the string are limited. Refer to
 * the convert class for details.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
@Module.Attributes(supportsSuperpeer = false, supportsPeer = true)
@Component.Attributes(priorityInit = DXRAMComponentOrder.Init.NAMESERVICE,
        priorityShutdown = DXRAMComponentOrder.Shutdown.NAMESERVICE)
public class NameserviceComponent extends Component<NameserviceComponentConfig> {

    @Inject
    private LookupComponent m_lookup;

    @Inject
    private ChunkIndexComponent m_chunkIndex;

    private NameServiceStringConverter m_converter;

    private boolean m_chunkIndexDataEnabled;
    private NameServiceIndexData m_indexData;
    private boolean m_indexDataRegistered;
    private Lock m_indexDataLock;

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
     * Register a chunk id for a specific name.
     *
     * @param p_chunkId
     *         Chunk id to register.
     * @param p_name
     *         Name to associate with the ID of the AbstractChunk.
     */
    public void register(final long p_chunkId, final String p_name) {
        try {
            final int id = m_converter.convert(p_name);

            LOGGER.trace("Registering chunkID 0x%X, name %s, id %d", p_chunkId, p_name, id);

            m_lookup.insertNameserviceEntry(id, p_chunkId);
            insertMapping(id, p_chunkId);
        } catch (final IllegalArgumentException e) {
            LOGGER.error("Lookup in name service failed", e);
        }
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
        long ret = ChunkID.INVALID_ID;

        try {
            final int id = m_converter.convert(p_name);

            LOGGER.trace("Lookup name %s, id %d", p_name, id);

            ret = m_lookup.getChunkIDForNameserviceEntry(id, p_timeoutMs);

            LOGGER.trace("Lookup name %s, resulting chunkID 0x%X", p_name, ret);
        } catch (final IllegalArgumentException e) {
            LOGGER.error("Lookup in name service failed", e);
        }

        return ret;
    }

    public void reinit() {

        LOGGER.warn("Re-initializing");

        shutdownName();
        initName();
    }

    @Override
    protected void resolveComponentDependencies(final ComponentProvider p_componentAccessor) {
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_chunkIndex = p_componentAccessor.getComponent(ChunkIndexComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
        ChunkComponentConfig chunkComponentConfig = p_config.getComponentConfig(ChunkComponent.class);

        // can't store index data as chunk if backend storage disabled
        m_chunkIndexDataEnabled = chunkComponentConfig.isChunkStorageEnabled();

        return initName();
    }

    @Override
    protected boolean shutdownComponent() {
        shutdownName();

        return true;
    }

    /**
     * Remove the name of a registered AbstractChunk from lookup.
     *
     * @return the number of entries in name service
     */
    int getEntryCount() {
        return m_lookup.getNameserviceEntryCount();
    }

    /**
     * Get all available name mappings
     *
     * @return List of available name mappings
     */
    ArrayList<NameserviceEntryStr> getAllEntries() {
        ArrayList<NameserviceEntryStr> list = new ArrayList<>();
        ArrayList<NameserviceEntry> entries = m_lookup.getNameserviceEntries();

        // convert index representation
        for (NameserviceEntry entry : entries) {
            list.add(new NameserviceEntryStr(m_converter.convert(entry.getId()), entry.getValue()));
        }

        return list;
    }

    /**
     * Initialize the nameservice
     *
     * @return True on success, false on error
     */
    private boolean initName() {
        m_converter = new NameServiceStringConverter(getConfig().getType());

        if (m_chunkIndexDataEnabled) {
            m_indexData = new NameServiceIndexData();

            // this does not create the backup (if backup is turned on) because the backup
            // service is not available at this point
            // the flag m_indexDataRegistered ensures that a backup of the data is distributed
            // later once the first name entry is registered
            m_indexData.setID(m_chunkIndex.createIndexChunk(m_indexData.sizeofObject()));

            if (m_indexData.getID() == ChunkID.INVALID_ID) {
                LOGGER.error("Creating root index chunk failed");
                return false;
            }

            m_indexDataRegistered = false;

            m_indexDataLock = new ReentrantLock(false);
        }

        return true;
    }

    /**
     * Shut down the nameservice
     */
    private void shutdownName() {
        m_converter = null;

        m_indexData = null;
        m_indexDataLock = null;
    }

    /**
     * Inserts the nameservice entry to chunk with LocalID 0 for backup
     *
     * @param p_key
     *         the key
     * @param p_chunkID
     *         the ChunkID
     * @return whether this operation was successful
     */
    private boolean insertMapping(final int p_key, final long p_chunkID) {
        if (m_chunkIndexDataEnabled) {
            m_indexDataLock.lock();

            if (!m_indexDataRegistered) {
                m_chunkIndex.registerIndexChunk(m_indexData.getID(), m_indexData.sizeofObject());
            }

            if (!m_indexData.insertMapping(p_key, p_chunkID)) {
                // index chunk full, create new one
                final NameServiceIndexData nextIndexChunk = new NameServiceIndexData();
                nextIndexChunk.setID(m_chunkIndex.createIndexChunk(nextIndexChunk.sizeofObject()));

                if (nextIndexChunk.getID() == ChunkID.INVALID_ID) {
                    LOGGER.error("Creating next index chunk failed");

                    m_indexDataLock.unlock();
                    return false;
                }

                // link previous to new and update
                m_indexData.setNextIndexDataChunk(nextIndexChunk.getID());

                if (!m_chunkIndex.putIndexChunk(m_indexData)) {
                    LOGGER.error("Updating current index chunk with successor failed");

                    m_indexDataLock.unlock();
                    return false;
                }

                m_indexData = nextIndexChunk;
            }

            // insert mapping into current chunk and update
            m_indexData.insertMapping(p_key, p_chunkID);

            if (!m_chunkIndex.putIndexChunk(m_indexData)) {
                LOGGER.error("Updating current index chunk failed");

                m_indexDataLock.unlock();
                return false;
            }

            m_indexDataLock.unlock();
        }

        return true;
    }
}

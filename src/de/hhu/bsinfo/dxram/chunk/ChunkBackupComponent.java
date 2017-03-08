/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.chunk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.messages.InitRequest;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.ethnet.NetworkException;

/**
 * Component for chunk handling.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class ChunkBackupComponent extends AbstractDXRAMComponent {

    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkBackupComponent.class.getSimpleName());

    // dependent components
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;

    /**
     * Constructor
     */
    public ChunkBackupComponent() {
        super(DXRAMComponentOrder.Init.CHUNK, DXRAMComponentOrder.Shutdown.CHUNK);
    }

    /**
     * Replicates all local Chunks to a specific backup peer
     *
     * @param p_backupPeer
     *     the new backup peer
     * @param p_chunkIDRanges
     *     the ChunkIDs of the Chunks to replicate arranged in ranges
     * @param p_rangeID
     *     the RangeID
     */
    public void replicateBackupRange(final short p_backupPeer, final long[] p_chunkIDRanges, final short p_rangeID) {
        int counter = 0;
        Chunk currentChunk;
        Chunk[] chunks;

        // Initialize backup range on backup peer
        InitRequest request = new InitRequest(p_backupPeer, p_rangeID);

        try {
            m_network.sendMessage(request);
        } catch (final NetworkException e) {
            // #if LOGGER == ERROR
            LOGGER.error("Replicating backup range 0x%X to 0x%X failed. Could not initialize backup range", p_rangeID, p_backupPeer);
            // #endif /* LOGGER == ERROR */
            return;
        }

        // Gather all chunks of backup range
        chunks = new Chunk[p_chunkIDRanges.length];
        m_memoryManager.lockAccess();
        for (int i = 0; i < p_chunkIDRanges.length; i += 2) {
            for (long currentChunkID = p_chunkIDRanges[i]; currentChunkID <= p_chunkIDRanges[i + 1]; currentChunkID++) {
                currentChunk = new Chunk(currentChunkID);

                m_memoryManager.get(currentChunk);

                chunks[counter++] = currentChunk;
            }
        }
        m_memoryManager.unlockAccess();

        // Send all chunks to backup peer
        try {
            m_network.sendMessage(new LogMessage(p_backupPeer, p_rangeID, chunks));
        } catch (final NetworkException ignore) {

        }
    }

    /**
     * Replicates all local Chunks to a specific backup peer
     *
     * @param p_backupPeer
     *     the new backup peer
     * @param p_chunkIDs
     *     the ChunkIDs of the Chunks to replicate
     * @param p_rangeID
     *     the RangeID
     */
    public void replicateBackupRange(final short p_backupPeer, final short p_rangeID, final long[] p_chunkIDs) {
        int counter = 0;
        Chunk currentChunk;
        Chunk[] chunks;

        // Initialize backup range on backup peer
        InitRequest request = new InitRequest(p_backupPeer, p_rangeID);

        try {
            m_network.sendMessage(request);
        } catch (final NetworkException e) {
            // #if LOGGER == ERROR
            LOGGER.error("Replicating backup range 0x%X to 0x%X failed. Could not initialize backup range", p_rangeID, p_backupPeer);
            // #endif /* LOGGER == ERROR */
            return;
        }

        // Gather all chunks of backup range
        chunks = new Chunk[p_chunkIDs.length];
        m_memoryManager.lockAccess();
        for (int i = 0; i < p_chunkIDs.length; i++) {
            currentChunk = new Chunk(p_chunkIDs[i]);

            // The ChunkID list might contain migrated and deleted chunks -> only replicate locally existing chunks
            if (m_memoryManager.get(currentChunk)) {
                chunks[counter++] = currentChunk;
            }
        }
        m_memoryManager.unlockAccess();

        // Send all chunks to backup peer
        try {
            m_network.sendMessage(new LogMessage(p_backupPeer, p_rangeID, chunks));
        } catch (final NetworkException ignore) {

        }
    }

    /**
     * Put recovered chunks into local memory.
     *
     * @param p_chunkIDs
     *     ChunkIDs of recovered Chunks.
     * @param p_data
     *     the byte array all recovered Chunks are stored in (contains also not to be recovered, invalid Chunks).
     * @param p_offsets
     *     the offsets within the byte array.
     * @param p_lengths
     *     the Chunks lengths.
     * @param p_usedEntries
     *     the number of actually used entries within the arrays (might be smaller than the array lengths).
     * @lock manage lock from memory manager component must be locked
     */
    public boolean putRecoveredChunks(final long[] p_chunkIDs, final byte[] p_data, final int[] p_offsets, final int[] p_lengths, final int p_usedEntries) {
        m_memoryManager.createAndPutRecovered(p_chunkIDs, p_data, p_offsets, p_lengths, p_usedEntries);
        // #if LOGGER == TRACE
        LOGGER.trace("Stored %d recovered chunks locally", p_usedEntries);
        // #endif /* LOGGER == TRACE */

        return true;
    }

    /**
     * Put a recovered chunks into local memory.
     *
     * @param p_chunks
     *     Chunks to put.
     */
    public void putRecoveredChunks(final Chunk[] p_chunks) {

        m_memoryManager.lockManage();
        for (Chunk chunk : p_chunks) {

            m_memoryManager.create(chunk.getID(), chunk.getDataSize());
            m_memoryManager.put(chunk);

            // #if LOGGER == TRACE
            LOGGER.trace("Stored recovered chunk 0x%X locally", chunk.getID());
            // #endif /* LOGGER == TRACE */
        }
        m_memoryManager.unlockManage();
    }

    public void startBlockRecovery() {
        m_memoryManager.lockManage();
    }

    public void stopBlockRecovery() {
        m_memoryManager.unlockManage();
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        return true;
    }

}

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

package de.hhu.bsinfo.dxram.chunk;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DSByteArray;
import de.hhu.bsinfo.dxram.data.DataStructure;
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
    private static final int MAXIMUM_QUEUE_SIZE = 10;

    // component dependencies
    private MemoryManagerComponent m_memoryManager;
    private AbstractBootComponent m_boot;
    private NetworkComponent m_network;

    private ConcurrentLinkedQueue<Entry> m_recoveryChunkQueue;

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
     * @return the number of replicated Chunks
     */
    public int replicateBackupRange(final short p_backupPeer, final long[] p_chunkIDRanges, final short p_rangeID) {
        int numberOfChunks = 0;

        for (int i = 0; i < p_chunkIDRanges.length; i += 2) {
            if (ChunkID.getLocalID(p_chunkIDRanges[i + 1]) == 0xFFFFFFFFFFFFL) {
                // This is the current backup range -> end of range is unknown at this moment -> current end is highest used LocalID
                m_memoryManager.lockAccess();
                p_chunkIDRanges[i + 1] = ((long) m_boot.getNodeID() << 48) + m_memoryManager.getHighestUsedLocalID();
                m_memoryManager.unlockAccess();
            }
            numberOfChunks += p_chunkIDRanges[i + 1] - p_chunkIDRanges[i] + 1;
        }

        return replicateBackupRange(p_backupPeer, p_chunkIDRanges, numberOfChunks, p_rangeID);
    }

    /**
     * Replicates all local Chunks to a specific backup peer
     *
     * @param p_backupPeer
     *     the new backup peer
     * @param p_chunkIDRanges
     *     the ChunkIDs of the Chunks to replicate arranged in ranges
     * @param p_numberOfChunks
     *     the number of Chunks
     * @param p_rangeID
     *     the RangeID
     * @return the number of replicated Chunks
     */
    public int replicateBackupRange(final short p_backupPeer, final long[] p_chunkIDRanges, final int p_numberOfChunks, final short p_rangeID) {
        int counter = 0;
        byte[] data;
        DataStructure[] chunks;

        // Initialize backup range on backup peer
        InitRequest request = new InitRequest(p_backupPeer, p_rangeID);

        try {
            m_network.sendSync(request);
        } catch (final NetworkException e) {
            // #if LOGGER == ERROR
            LOGGER.error("Replicating backup range 0x%X to 0x%X failed. Could not initialize backup range", p_rangeID, p_backupPeer);
            // #endif /* LOGGER == ERROR */
            return 0;
        }

        // Gather all chunks of backup range
        chunks = new DataStructure[p_numberOfChunks];
        m_memoryManager.lockAccess();
        for (int i = 0; i < p_chunkIDRanges.length; i += 2) {
            for (long currentChunkID = p_chunkIDRanges[i]; currentChunkID <= p_chunkIDRanges[i + 1]; currentChunkID++) {
                data = m_memoryManager.get(currentChunkID);
                if (data != null) {
                    DataStructure currentChunk = new DSByteArray(currentChunkID, data);
                    chunks[counter++] = currentChunk;
                }
            }
        }
        m_memoryManager.unlockAccess();

        // Send all chunks to backup peer
        try {
            m_network.sendMessage(new LogMessage(p_backupPeer, p_rangeID, chunks));
        } catch (final NetworkException ignore) {

        }

        return counter;
    }

    /**
     * Initializes a new thread for storing all recovered Chunks in memory
     *
     * @return the Thread
     */
    public RecoveryWriterThread initRecoveryThread() {
        m_recoveryChunkQueue = new ConcurrentLinkedQueue<Entry>();

        RecoveryWriterThread thread = new RecoveryWriterThread();
        thread.setName("Recovery: Writer-Thread");
        thread.start();

        return thread;
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

        while (m_recoveryChunkQueue.size() >= MAXIMUM_QUEUE_SIZE) {
            Thread.yield();
        }
        m_recoveryChunkQueue.add(new Entry(p_chunkIDs, p_data, p_offsets, p_lengths, p_usedEntries));

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
     * @return the number of created and put Chunks
     */
    public int putRecoveredChunks(final DataStructure[] p_chunks) {
        int ret = 0;

        m_memoryManager.lockManage();
        m_memoryManager.createMulti(p_chunks);
        for (DataStructure chunk : p_chunks) {
            if (m_memoryManager.put(chunk)) {
                ret++;
            }

            // #if LOGGER == TRACE
            LOGGER.trace("Stored recovered chunk 0x%X locally", chunk.getID());
            // #endif /* LOGGER == TRACE */
        }
        m_memoryManager.unlockManage();

        return ret;
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
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        // Add DXRAMComponentOrder.Init value if something is put here
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        // Add DXRAMComponentOrder.Shutdown value if something is put here
        return true;
    }

    private static final class Entry {

        private long[] m_chunkIDs;
        private byte[] m_data;
        private int[] m_offsets;
        private int[] m_lengths;
        private int m_usedEntries;

        private Entry(final long[] p_chunkIDs, final byte[] p_data, final int[] p_offsets, final int[] p_lengths, final int p_usedEntries) {
            m_chunkIDs = p_chunkIDs;
            m_data = p_data;
            m_offsets = p_offsets;
            m_lengths = p_lengths;
            m_usedEntries = p_usedEntries;
        }
    }

    /**
     * Recovery helper thread. Writes all given Chunks to memory.
     */
    public class RecoveryWriterThread extends Thread {

        private int m_timeToPut = 0;

        /**
         * Returns the duration for putting all chunks
         *
         * @return the time in ms
         */
        public int getTimeToPut() {
            return m_timeToPut;
        }

        /**
         * Returns if all chunks were already put to memory
         *
         * @return true if queue is empty
         */
        public boolean finished() {
            return m_recoveryChunkQueue.isEmpty();
        }

        @Override
        public void run() {
            long time;
            Entry entry;

            while (true) {
                entry = null;
                while (entry == null) {
                    entry = m_recoveryChunkQueue.poll();

                    if (entry == null) {
                        if (Thread.currentThread().isInterrupted()) {
                            return;
                        }

                        Thread.yield();
                    }
                }

                time = System.currentTimeMillis();
                m_memoryManager.lockManage();
                m_memoryManager.createAndPutRecovered(entry.m_chunkIDs, entry.m_data, entry.m_offsets, entry.m_lengths, entry.m_usedEntries);
                m_memoryManager.unlockManage();
                m_timeToPut += System.currentTimeMillis() - time;
            }
        }
    }

}

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

package de.hhu.bsinfo.dxram.chunk;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.log.messages.InitBackupRangeRequest;
import de.hhu.bsinfo.dxram.log.messages.LogBufferMessage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Component for chunk handling.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class ChunkBackupComponent extends AbstractDXRAMComponent<ChunkBackupComponentConfig> {

    // component dependencies
    private AbstractBootComponent m_boot;
    private ChunkComponent m_chunk;
    private NetworkComponent m_network;

    /**
     * Constructor
     */
    public ChunkBackupComponent() {
        super(DXRAMComponentOrder.Init.CHUNK, DXRAMComponentOrder.Shutdown.CHUNK, ChunkBackupComponentConfig.class);
    }

    /**
     * Replicates all local Chunks to a specific backup peer
     *
     * @param p_backupPeer
     *         the new backup peer
     * @param p_chunkIDRanges
     *         the ChunkIDs of the Chunks to replicate arranged in ranges
     * @param p_rangeID
     *         the RangeID
     * @return the number of replicated Chunks
     */
    public int replicateBackupRange(final short p_backupPeer, final long[] p_chunkIDRanges, final short p_rangeID) {
        int numberOfChunks = 0;

        for (int i = 0; i < p_chunkIDRanges.length; i += 2) {
            if (ChunkID.getLocalID(p_chunkIDRanges[i + 1]) == 0xFFFFFFFFFFFFL) {
                // This is the current backup range -> end of range is unknown at this moment
                // -> current end is highest used LocalID
                p_chunkIDRanges[i + 1] =
                        ((long) m_boot.getNodeId() << 48) + m_chunk.getMemory().cidStatus().getHighestUsedLocalID();
            }
            numberOfChunks += p_chunkIDRanges[i + 1] - p_chunkIDRanges[i] + 1;
        }

        return replicateBackupRange(p_backupPeer, p_chunkIDRanges, numberOfChunks, p_rangeID);
    }

    /**
     * Replicates all local Chunks to a specific backup peer
     *
     * @param p_backupPeer
     *         the new backup peer
     * @param p_chunkIDRanges
     *         the ChunkIDs of the Chunks to replicate arranged in ranges
     * @param p_numberOfChunks
     *         the number of Chunks
     * @param p_rangeID
     *         the RangeID
     * @return the number of replicated Chunks
     */
    public int replicateBackupRange(final short p_backupPeer, final long[] p_chunkIDRanges, final int p_numberOfChunks,
            final short p_rangeID) {
        int counter = 0;
        int allCounter = 0;

        // Initialize backup range on backup peer
        InitBackupRangeRequest request = new InitBackupRangeRequest(p_backupPeer, p_rangeID);

        try {
            m_network.sendSync(request);
        } catch (final NetworkException ignored) {
            LOGGER.error("Replicating backup range 0x%X to 0x%X failed. Could not initialize backup range", p_rangeID,
                    p_backupPeer);
            return 0;
        }

        // TODO: Replicates all created chunks including chunks that have not been put

        // Gather all chunks of backup range
        // FIXME: this limits the size of a chunk to 32 MB if i am not mistaken. thus, replication won't work
        // for larger chunks
        byte[] chunkArray = new byte[32 * 1024 * 1024];
        ByteBuffer chunkBuffer = ByteBuffer.wrap(chunkArray);

        for (int i = 0; i < p_chunkIDRanges.length; i += 2) {
            for (long currentChunkID = p_chunkIDRanges[i]; currentChunkID <= p_chunkIDRanges[i + 1]; currentChunkID++) {
                // Store payload behind ChunkID and size
                int bytes = m_chunk.getMemory().get()
                        .get(currentChunkID, chunkArray, chunkBuffer.position() + Long.BYTES + Integer.BYTES,
                                chunkArray.length, ChunkLockOperation.NONE, -1);

                if (bytes == 0) {
                    // Chunk does not fit in current buffer -> send buffer and repeat
                    chunkBuffer.flip();

                    try {
                        m_network.sendMessage(new LogBufferMessage(p_backupPeer, p_rangeID, counter, chunkBuffer));
                    } catch (final NetworkException ignore) {

                    }

                    chunkBuffer.clear();
                    allCounter += counter;
                    counter = 0;

                    bytes = m_chunk.getMemory().get()
                            .get(currentChunkID, chunkArray, chunkBuffer.position() + Long.BYTES + Integer.BYTES,
                                    chunkArray.length, ChunkLockOperation.NONE, -1);
                }

                if (bytes < 0) {
                    LOGGER.error("Could not replicate 0x%X: %s", currentChunkID, ChunkState.values()[-bytes]);
                    continue;
                }

                chunkBuffer.putLong(currentChunkID);
                chunkBuffer.putInt(bytes);
                chunkBuffer.position(chunkBuffer.position() + bytes);
                counter++;
            }
        }

        allCounter += counter;

        return allCounter;
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
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config, final DXRAMJNIManager p_jniManager) {
        // Add DXRAMComponentOrder.Init value if something is put here
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        // Add DXRAMComponentOrder.Shutdown value if something is put here
        return true;
    }

}

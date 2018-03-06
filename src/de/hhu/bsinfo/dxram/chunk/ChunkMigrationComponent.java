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

import java.util.ArrayList;

import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.data.DSByteArray;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Component for chunk handling.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class ChunkMigrationComponent extends AbstractDXRAMComponent<ChunkMigrationComponentConfig> {
    // component dependencies
    private BackupComponent m_backup;
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;

    /**
     * Constructor
     */
    public ChunkMigrationComponent() {
        super(DXRAMComponentOrder.Init.CHUNK, DXRAMComponentOrder.Shutdown.CHUNK, ChunkMigrationComponentConfig.class);
    }

    /**
     * Puts migrated Chunks
     *
     * @param p_chunkIDs
     *         The chunk IDs of the migrated chunks
     * @param p_data
     *         The data of the migrated chunks
     * @return whether storing foreign chunks was successful or not
     */
    public boolean putMigratedChunks(final long[] p_chunkIDs, final byte[][] p_data) {
        short rangeID = 0;
        BackupRange backupRange;
        ArrayList<BackupRange> backupRanges;
        ArrayList<Long> cutChunkIDs;

        backupRanges = new ArrayList<>();
        cutChunkIDs = new ArrayList<>();
        m_memoryManager.lockManage();
        for (int i = 0; i < p_chunkIDs.length; i++) {

            m_memoryManager.create(p_chunkIDs[i], p_data[i].length);
            m_memoryManager.put(p_chunkIDs[i], p_data[i]);

            // #if LOGGER == TRACE
            LOGGER.trace("Stored migrated chunk 0x%X locally", p_chunkIDs[i]);
            // #endif /* LOGGER == TRACE */

            if (m_backup.isActive()) {
                backupRange = m_backup.registerChunk(p_chunkIDs[i], p_data[i].length);

                if (rangeID != backupRange.getRangeID()) {
                    backupRanges.add(backupRange);
                    cutChunkIDs.add(p_chunkIDs[i]);
                    rangeID = backupRange.getRangeID();
                }
            }
        }
        m_memoryManager.unlockManage();

        // Send backups after unlocking memory manager lock
        if (m_backup.isActive()) {
            replicateMigratedChunks(p_chunkIDs, p_data, backupRanges, cutChunkIDs);
        }

        return true;
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
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {
        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        return true;
    }

    /**
     * Replicate migrated chunks to corresponding backup ranges
     *
     * @param p_chunkIDs
     *         The chunk IDs of the chunks to replicate
     * @param p_data
     *         The data of the chunks to replicate
     * @param p_backupRanges
     *         a list of all relevant backup ranges
     * @param p_cutChunkIDs
     *         a list of ChunkIDs. For every listed ChunkID the backup range must be replaced by next element in p_backupRanges
     */
    private void replicateMigratedChunks(final long[] p_chunkIDs, final byte[][] p_data, final ArrayList<BackupRange> p_backupRanges,
            final ArrayList<Long> p_cutChunkIDs) {
        int counter = 1;
        short rangeID;
        long cutChunkID;
        BackupPeer[] backupPeers;
        BackupRange backupRange;

        backupRange = p_backupRanges.get(0);
        cutChunkID = p_cutChunkIDs.get(0);
        backupPeers = backupRange.getBackupPeers();
        rangeID = backupRange.getRangeID();

        for (int i = 0; i < p_chunkIDs.length; i++) {

            if (p_chunkIDs[i] == cutChunkID) {
                backupRange = p_backupRanges.get(counter);
                cutChunkID = p_cutChunkIDs.get(counter);
                counter++;

                backupPeers = backupRange.getBackupPeers();
                rangeID = backupRange.getRangeID();
            }

            for (BackupPeer backupPeer : backupPeers) {
                if (backupPeer != null) {
                    try {
                        m_network.sendMessage(new LogMessage(backupPeer.getNodeID(), rangeID, new DSByteArray(p_chunkIDs[i], p_data[i])));
                    } catch (final NetworkException ignore) {

                    }
                }
            }
        }
    }

}

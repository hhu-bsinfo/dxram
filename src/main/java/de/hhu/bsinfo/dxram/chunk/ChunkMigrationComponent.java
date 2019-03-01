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

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxram.engine.ModuleAccessor;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Component for migrating chunks
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = false, supportsPeer = true)
@AbstractDXRAMComponent.Attributes(priorityInit = DXRAMComponentOrder.Init.CHUNK,
        priorityShutdown = DXRAMComponentOrder.Shutdown.CHUNK)
public class ChunkMigrationComponent extends AbstractDXRAMComponent<DXRAMModuleConfig> {
    // component dependencies
    private BackupComponent m_backup;
    private ChunkComponent m_chunk;
    private NetworkComponent m_network;

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

        for (int i = 0; i < p_chunkIDs.length; i++) {
            m_chunk.getMemory().createReserved().createReserved(p_chunkIDs[i], p_data[i].length);
            m_chunk.getMemory().put().put(p_chunkIDs[i], p_data[i], ChunkLockOperation.NONE, -1);

            LOGGER.trace("Stored migrated chunk 0x%X locally", p_chunkIDs[i]);

            if (m_backup.isActive()) {
                backupRange = m_backup.registerChunk(p_chunkIDs[i], p_data[i].length);

                if (rangeID != backupRange.getRangeID()) {
                    backupRanges.add(backupRange);
                    cutChunkIDs.add(p_chunkIDs[i]);
                    rangeID = backupRange.getRangeID();
                }
            }
        }

        // Send backups after unlocking memory manager lock
        if (m_backup.isActive()) {
            replicateMigratedChunks(p_chunkIDs, p_data, backupRanges, cutChunkIDs);
        }

        return true;
    }

    @Override
    protected void resolveComponentDependencies(final ModuleAccessor p_moduleAccessor) {
        m_backup = p_moduleAccessor.getComponent(BackupComponent.class);
        m_chunk = p_moduleAccessor.getComponent(ChunkComponent.class);
        m_network = p_moduleAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
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
     *         a list of ChunkIDs. For every listed ChunkID the backup range must be replaced by next
     *         element in p_backupRanges
     */
    private void replicateMigratedChunks(final long[] p_chunkIDs, final byte[][] p_data,
            final ArrayList<BackupRange> p_backupRanges,
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
                        m_network.sendMessage(new LogMessage(backupPeer.getNodeID(), rangeID,
                                new ChunkByteArray(p_chunkIDs[i], p_data[i])));
                    } catch (final NetworkException ignore) {

                    }
                }
            }
        }
    }

}

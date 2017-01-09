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
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.ethnet.NetworkException;

/**
 * Component for chunk handling.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class ChunkMigrationComponent extends AbstractDXRAMComponent {

    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkMigrationComponent.class.getSimpleName());

    // dependent components
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;
    private LogComponent m_log;

    /**
     * Constructor
     */
    public ChunkMigrationComponent() {
        super(DXRAMComponentOrder.Init.CHUNK, DXRAMComponentOrder.Shutdown.CHUNK);
    }

    /**
     * Puts migrated Chunks
     *
     * @param p_chunks
     *     the Chunks
     * @return whether storing foreign chunks was successful or not
     */
    public boolean putMigratedChunks(final Chunk[] p_chunks) {
        byte rangeID;
        int logEntrySize;
        long size = 0;
        long cutChunkID = -1;
        short[] backupPeers = null;

        m_memoryManager.lockManage();
        for (Chunk chunk : p_chunks) {

            m_memoryManager.create(chunk.getID(), chunk.getDataSize());
            m_memoryManager.put(chunk);

            // #if LOGGER == TRACE
            LOGGER.trace("Stored migrated chunk %s locally", ChunkID.toHexString(chunk.getID()));
            // #endif /* LOGGER == TRACE */

            if (m_backup.isActive()) {
                logEntrySize = chunk.getDataSize() +
                    m_log.getApproxHeaderSize(ChunkID.getCreatorID(chunk.getID()), ChunkID.getLocalID(chunk.getID()), chunk.getDataSize());
                if (m_backup.fitsInCurrentMigrationBackupRange(size, logEntrySize)) {
                    // Chunk fits in current migration backup range
                    size += logEntrySize;
                } else {
                    // Chunk does not fit -> initialize new migration backup range and remember cut
                    size = logEntrySize;
                    cutChunkID = chunk.getID();

                    m_backup.initNewMigrationBackupRange();
                }
            }
        }
        m_memoryManager.unlockManage();

        if (m_backup.isActive()) {
            for (Chunk chunk : p_chunks) {

                if (chunk.getID() == cutChunkID) {
                    // All following chunks are in the new migration backup range
                    backupPeers = m_backup.getCopyOfCurrentMigrationBackupPeers();
                }

                rangeID = m_backup.addMigratedChunk(chunk);

                if (backupPeers != null) {
                    for (short backupPeer : backupPeers) {
                        if (backupPeer != m_boot.getNodeID() && backupPeer != -1) {
                            try {
                                m_network.sendMessage(new LogMessage(backupPeer, rangeID, new Chunk[] {chunk}));
                            } catch (final NetworkException ignore) {

                            }
                        }
                    }
                }
            }
        }

        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_log = p_componentAccessor.getComponent(LogComponent.class);
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

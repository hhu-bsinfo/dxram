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
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Component for chunk handling.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class ChunkComponent extends AbstractDXRAMComponent {

    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkComponent.class.getSimpleName());

    // dependent components
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;

    /**
     * Constructor
     */
    public ChunkComponent() {
        super(DXRAMComponentOrder.Init.CHUNK, DXRAMComponentOrder.Shutdown.CHUNK);
    }

    /**
     * Create index chunk for the nameservice.
     *
     * @param p_size
     *     Size of the index chunk.
     * @return Chunkid of the index chunk.
     */
    public long createIndexChunk(final int p_size) {
        long chunkId;

        m_memoryManager.lockManage();
        chunkId = m_memoryManager.createIndex(p_size);
        if (chunkId != -1) {
            m_backup.initBackupRange(chunkId, p_size);
        }
        m_memoryManager.unlockManage();

        return chunkId;
    }

    /**
     * Internal chunk put for management data.
     *
     * @param p_dataStructure
     *     Data structure to put
     * @return True if successful, false otherwise
     */
    public boolean putChunk(final DataStructure p_dataStructure) {
        boolean ret;

        m_memoryManager.lockAccess();
        ret = m_memoryManager.put(p_dataStructure);
        m_memoryManager.unlockAccess();
        if (!ret) {
            return false;
        }

        if (m_backup.isActive()) {
            short[] backupPeers = m_backup.getCopyOfBackupPeersForLocalChunks(p_dataStructure.getID());

            if (backupPeers != null) {
                for (short peer : backupPeers) {
                    if (peer != m_boot.getNodeID() && peer != NodeID.INVALID_ID) {
                        // #if LOGGER == TRACE
                        LOGGER.trace("Logging 0x%x to %s", p_dataStructure.getID(), NodeID.toHexString(peer));
                        // #endif /* LOGGER == TRACE */

                        try {
                            m_network.sendMessage(new LogMessage(peer, p_dataStructure));
                        } catch (final NetworkException ignore) {

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

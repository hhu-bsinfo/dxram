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

import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Component for chunk handling.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public class ChunkComponent extends AbstractDXRAMComponent<ChunkComponentConfig> {
    // component dependencies
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;

    /**
     * Constructor
     */
    public ChunkComponent() {
        super(DXRAMComponentOrder.Init.CHUNK, DXRAMComponentOrder.Shutdown.CHUNK, ChunkComponentConfig.class);
    }

    /**
     * Create index chunk for the nameservice.
     *
     * @param p_size
     *         Size of the index chunk.
     * @return Chunkid of the index chunk.
     */
    public long createIndexChunk(final int p_size) {
        long chunkId;

        m_memoryManager.lockManage();
        chunkId = m_memoryManager.createIndex(p_size);
        m_memoryManager.unlockManage();

        return chunkId;
    }

    /**
     * Register index chunk for the nameservice. Is called for the first nameservice entry added.
     *
     * @param p_chunkID
     *         the ChunkID of the nameservice index chunk
     * @param p_size
     *         Size of the index chunk.
     */
    public void registerIndexChunk(final long p_chunkID, final int p_size) {
        m_memoryManager.lockManage();
        if (p_chunkID != ChunkID.INVALID_ID) {
            m_backup.registerChunk(p_chunkID, p_size);
        }
        m_memoryManager.unlockManage();
    }

    /**
     * Internal chunk put for management data.
     *
     * @param p_dataStructure
     *         Data structure to put
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
            BackupRange backupRange = m_backup.getBackupRange(p_dataStructure.getID());
            BackupPeer[] backupPeers = backupRange.getBackupPeers();

            if (backupPeers != null) {
                for (BackupPeer peer : backupPeers) {
                    if (peer != null && peer.getNodeID() != m_boot.getNodeId()) {

                        LOGGER.trace("Logging 0x%x to %s", p_dataStructure.getID(),
                                NodeID.toHexString(peer.getNodeID()));

                        try {
                            m_network.sendMessage(
                                    new LogMessage(peer.getNodeID(), backupRange.getRangeID(), p_dataStructure));
                        } catch (final NetworkException ignore) {

                        }
                    }
                }
            }
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
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
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

}

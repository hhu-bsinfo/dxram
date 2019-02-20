/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.chunk.operation;

import de.hhu.bsinfo.dxmem.operations.Pinning.PinnedMemory;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Pin chunks (local only).
 * 
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, 19.02.2019
 *
 */
public class PinningLocal extends AbstractOperation {

    private static final ValuePool SOP_PIN = new ValuePool(ChunkLocalService.class, "Pin");
    private static final ValuePool SOP_UNPIN = new ValuePool(ChunkLocalService.class, "Unpin");

    static {
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_PIN);
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_UNPIN);
    }

    /**
     * Constructor.
     *
     * @param p_parentService
     *         Instance of parent service this operation belongs to
     * @param p_boot
     *         Instance of BootComponent
     * @param p_backup
     *         Instance of BackupComponent
     * @param p_chunk
     *         Instance of ChunkComponent
     * @param p_network
     *         Instance of NetworkComponent
     * @param p_lookup
     *         Instance of LookupComponent
     * @param p_nameservice
     *         Instance of NameserviceComponent
     */
    public PinningLocal(
            final Class<? extends AbstractDXRAMService<? extends DXRAMModuleConfig>> p_parentService,
            final AbstractBootComponent<?> p_boot,
            final BackupComponent p_backup,
            final ChunkComponent p_chunk,
            final NetworkComponent p_network,
            final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);
    }

    /**
     * Pin a chunk.
     *
     * @param p_cid
     *         Cid of chunk to pin
     * @return PinnedMemory object with ChunkState determining the result of the operation
     */
    public PinnedMemory pin(final long p_cid) {
        return pin(p_cid, -1);
    }

    /**
     * Pin a chunk.
     *
     * @param p_cid
     *         Cid of chunk to pin
     * @param p_acquireLockTimeoutMs
     *         -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and &gt; 0 for a timeout value in ms
     * @return PinnedMemory object with ChunkState determining the result of the operation
     */
    public PinnedMemory pin(final long p_cid, final int p_acquireLockTimeoutMs) {
        SOP_PIN.inc();
        return m_chunk.getMemory().pinning().pin(p_cid, p_acquireLockTimeoutMs);
    }

    /**
     * Unpin a pinned chunk. Depending on how many chunks are currently stored, this call is very slow because it
     * has to perform a depth search on the CIDTable to find the CIDTable entry.
     *
     * @param p_pinnedChunkAddress
     *         Address of pinned chunk
     * @return CID of unpinned chunk on success, INVALID_ID on failure
     */
    public long unpin(final long p_pinnedChunkAddress) {
        SOP_UNPIN.inc();
        return m_chunk.getMemory().pinning().unpin(p_pinnedChunkAddress);
    }

    /**
     * Unpin a pinned chunk using the CID.
     *
     * @param p_cidOfPinnedChunk
     *         CID of pinned chunk
     */
    public void unpinCID(final long p_cidOfPinnedChunk) {
        SOP_UNPIN.inc();
        m_chunk.getMemory().pinning().unpinCID(p_cidOfPinnedChunk);
    }
}

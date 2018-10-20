package de.hhu.bsinfo.dxram.chunk.operation;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ThroughputPool;
import de.hhu.bsinfo.dxutils.stats.Value;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Get the stored data of an existing chunk (local only and optimized)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class GetLocal extends AbstractOperation {
    private static final ThroughputPool SOP_GET =
            new ThroughputPool(ChunkLocalService.class, "Get", Value.Base.B_10);

    private static final ValuePool SOP_GET_ERROR = new ValuePool(ChunkLocalService.class, "GetError");

    static {
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_GET);
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_GET_ERROR);
    }

    /**
     * Constructor
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
    public GetLocal(final Class<? extends AbstractDXRAMService> p_parentService,
            final AbstractBootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);
    }

    /**
     * Get the data of one or multiple chunks
     *
     * @param p_chunks
     *         Chunks to get
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int get(final AbstractChunk... p_chunks) {
        return get(0, p_chunks.length, ChunkLockOperation.NONE, p_chunks);
    }

    /**
     * Get the data of one or multiple chunks
     * )
     *
     * @param p_lockOperation
     *         Lock operation to execute for each get operation
     * @param p_chunks
     *         Chunks to get
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int get(final ChunkLockOperation p_lockOperation, final AbstractChunk... p_chunks) {
        return get(0, p_chunks.length, p_lockOperation, p_chunks);
    }

    /**
     * Get the data of one or multiple chunks
     *
     * @param p_offset
     *         Offset in array where to start get operations
     * @param p_count
     *         Number of chunks to get (might be less array size/number of chunks provided)
     * @param p_lockOperation
     *         Lock operation to execute for each get operation
     * @param p_chunks
     *         Chunks to get
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int get(final int p_offset, final int p_count, final ChunkLockOperation p_lockOperation,
            final AbstractChunk... p_chunks) {
        m_logger.trace("get[offset %d, count %d, lock op %s, chunks (%d) ...", p_offset, p_count, p_lockOperation,
                p_chunks.length);

        SOP_GET.start();

        int successful = 0;

        for (int i = p_offset; i < p_count; i++) {
            m_chunk.getMemory().get().get(p_chunks[i], p_lockOperation, -1);

            if (p_chunks[i].isStateOk()) {
                successful++;
            }
        }

        if (successful < p_count) {
            SOP_GET_ERROR.add(p_count - successful);
        }

        SOP_GET.stop(successful);

        return successful;
    }

    /**
     * Get the data for a single chunk. Avoids array allocation of variadic chunk parameter version for a single chunk
     *
     * @param p_chunk
     *         Chunk to get
     * @return True if successful, false on error (check the chunk object state for errors)
     */
    public boolean get(final AbstractChunk p_chunk) {
        return get(p_chunk, ChunkLockOperation.NONE);
    }

    /**
     * Get the data for a single chunk. Avoids array allocation of variadic chunk parameter version for a single chunk
     *
     * @param p_chunk
     *         Chunk to get
     * @param p_lockOperation
     *         Lock operation to execute
     * @return True if successful, false on error (check the chunk object state for errors)
     */
    public boolean get(final AbstractChunk p_chunk , final ChunkLockOperation p_lockOperation) {
        m_chunk.getMemory().get().get(p_chunk, p_lockOperation, -1);

        return p_chunk.isStateOk();
    }
}

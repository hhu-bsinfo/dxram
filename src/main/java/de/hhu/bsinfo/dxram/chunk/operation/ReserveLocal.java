package de.hhu.bsinfo.dxram.chunk.operation;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ThroughputPool;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * Reserve the local chunk ids without allocating memory. (local only)
 * Make sure to actually use the reserved CIDs with a CreateReserved operation.
 * 
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, 11.02.2019
 */
public class ReserveLocal extends Operation {

    private static final ThroughputPool SOP_RESERVE =
            new ThroughputPool(ChunkLocalService.class, "Reserve", Value.Base.B_10);

    static {
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_RESERVE);
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
    public ReserveLocal(final Class<? extends Service> p_parentService,
            final BootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);
    }

    /**
     * Reserve a CID but do not allocate memory for it. Allocation must be executed by the user explicitly using
     * the CreateReserved operation.
     *
     * @return New CID
     */
    public long reserve() {
        SOP_RESERVE.start();
        m_logger.trace("reserve a single CID ...");
        final long reservedCID = m_chunk.getMemory().reserve().reserve();
        SOP_RESERVE.stop(1);
        return reservedCID;
    }

    /**
     * Reserve multiple CIDs but do not allocate memory for them. Allocation must be executed by the user explicitly
     * using the CreateReserved operation.
     *
     * @param p_array
     *         Reference to pre-allocated array to write CIDs to
     * @param p_offset
     *         Start offset in array
     * @param p_count
     *         Number of CIDs to reserve
     */
    public void reserve(final long[] p_array, final int p_offset, final int p_count) {
        SOP_RESERVE.start();
        m_logger.trace("reserve[offset %d, count %d] ...", p_offset, p_count);
        m_chunk.getMemory().reserve().reserve(p_array, p_offset, p_count);
        SOP_RESERVE.stop(p_count);
    }

    /**
     * Reserve multiple CIDs but do not allocate memory for them. Allocation must be executed by the user explicitly
     * using the CreateReserved operation.
     *
     * @param p_count
     *         Number of CIDs to reserve
     * @return Array with CIDs reserved
     */
    public long[] reserve(final int p_count) {
        SOP_RESERVE.start();
        m_logger.trace("reserve[count %d] ...", p_count);
        final long[] reservedCID = m_chunk.getMemory().reserve().reserve(p_count);
        SOP_RESERVE.stop(p_count);
        return reservedCID;
    }
}

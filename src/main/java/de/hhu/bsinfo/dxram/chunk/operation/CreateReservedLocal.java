package de.hhu.bsinfo.dxram.chunk.operation;

import java.util.Arrays;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
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
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Create chunks for reserved CIDs (local only and optimized)
 *
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, 13.02.2019
 */
public class CreateReservedLocal extends Operation {
    private static final ThroughputPool SOP_CREATE_RESERVED =
            new ThroughputPool(ChunkLocalService.class, "CreateReserved", Value.Base.B_10);
    private static final ThroughputPool SOP_CREATE_RESERVED_DS =
            new ThroughputPool(ChunkLocalService.class, "CreateReservedDS", Value.Base.B_10);

    private static final ValuePool SOP_CREATE_RESERVED_ERROR = new ValuePool(ChunkLocalService.class, "CreateReservedError");
    private static final ValuePool SOP_CREATE_RESERVED_DS_ERROR = new ValuePool(ChunkLocalService.class, "CreateReservedDSError");

    static {
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_CREATE_RESERVED);
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_CREATE_RESERVED_DS);
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_CREATE_RESERVED_ERROR);
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_CREATE_RESERVED_DS_ERROR);
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
    public CreateReservedLocal(final Class<? extends Service> p_parentService, final BootComponent p_boot,
            final BackupComponent p_backup, final ChunkComponent p_chunk, final NetworkComponent p_network,
            final LookupComponent p_lookup, final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);
    }

    /**
     * Create one or multiple chunks
     *
     * @param p_reserved_cids
     *         Pre-allocated array with the reserved CIDs
     * @param p_offset
     *         Offset in array to start getting the CIDs from
     * @param p_count
     *         Number of chunks to allocate
     * @param p_sizes
     *         Sizes to allocate chunks for
     * @return Number of chunks successfully created
     */
    public int create(final long[] p_reserved_cids, final int p_offset, final int p_count, final int[] p_sizes) {
        return create(p_reserved_cids, null, p_offset, p_count, p_sizes);
    }

    /**
     * Create one or multiple chunks
     *
     * @param p_reserved_cids
     *         Pre-allocated array with the reserved CIDs
     * @param p_addresses
     *         Optional (can be null): Array to return the raw addresses of the allocate chunks
     * @param p_offset
     *         Offset in array to start getting the CIDs from
     * @param p_count
     *         Number of chunks to allocate
     * @param p_sizes
     *         Sizes to allocate chunks for
     * @return Number of chunks successfully created
     */
    public int create(final long[] p_reserved_cids, final long[] p_addresses, final int p_offset, final int p_count, final int[] p_sizes) {
        m_logger.trace("create[cids.length %d, offset %d, sizes %d, count %d]", p_reserved_cids.length, p_offset,
                p_sizes, p_count);

        SOP_CREATE_RESERVED.start();

        m_backup.blockCreation();

        int created = m_chunk.getMemory().createReserved().createReserved(p_reserved_cids, p_addresses, p_sizes, 0, p_sizes.length);

        // Initialize a new backup range every e.g. 256 MB and inform superpeer
        m_backup.registerChunks(p_reserved_cids, p_offset, created, p_sizes);

        m_backup.unblockCreation();

        if (created < p_count) {
            SOP_CREATE_RESERVED_ERROR.add(p_count - created);
        }

        SOP_CREATE_RESERVED.stop(created);

        return created;
    }

    /**
     * Create one or multiple chunks
     *
     * @param p_reserved_cids
     *         Pre-allocated array with the reserved CIDs
     * @param p_count
     *         Number of chunks to allocate
     * @param p_sizes
     *         Sizes to allocate chunks for
     * @return Number of chunks successfully created
     */
    public int create(final long[] p_reserved_cids, final int p_count, final int[] p_sizes) {
        return create(p_reserved_cids, 0, p_count, p_sizes);
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_reserved_cids
     *         Pre-allocated array with the reserved CIDs
     * @param p_offset
     *         Offset in array to start putting the CIDs to
     * @param p_sizes
     *         One or multiple (different) sizes. The amount of sizes declared here denotes the number of
     *         chunks to create
     * @return Number of chunks successfully created
     */
    public int createSizes(final long[] p_reserved_cids, final int p_offset, int... p_sizes) {
        return createSizes(p_reserved_cids, p_offset, p_sizes);
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_reserved_cids
     *         Pre-allocated array with the reserved CIDs
     * @param p_sizes
     *         One or multiple (different) sizes. The amount of sizes declared here denotes the number of
     *         chunks to create
     * @return Number of chunks successfully created
     */
    public int createSizes(final long[] p_reserved_cids, int... p_sizes) {
        return createSizes(p_reserved_cids, 0, p_sizes);
    }

    /**
     * Create one or multiple chunks using Chunk instances (with different sizes)
     *
     * @param p_offset
     *         Offset in array to start putting the CIDs to
     * @param p_count
     *         Number of chunks to create (might be less than objects provided)
     * @param p_chunks
     *         Instances of chunk objects to allocate storage for. On success, the CID is assigned to the object
     *         and the state is set to OK.
     * @return Number of chunks successfully created. If less than expected, check the chunk objects states for errors.
     */
    public int create(final int p_offset, final int p_count, final AbstractChunk... p_chunks) {
        return create(p_offset, p_count, null, p_chunks);
    }

    /**
     * Create one or multiple chunks using Chunk instances (with different sizes)
     *
     * @param p_offset
     *         Offset in array to start putting the CIDs to
     * @param p_count
     *         Number of chunks to create (might be less than objects provided)
     * @param p_addresses
     *         Optional (can be null): Array to return the raw addresses of the allocate chunks
     * @param p_chunks
     *         Instances of chunk objects to allocate storage for. On success, the CID is assigned to the object
     *         and the state is set to OK.
     * @return Number of chunks successfully created. If less than expected, check the chunk objects states for errors.
     */
    public int create(final int p_offset, final int p_count, final long[] p_addresses, final AbstractChunk... p_chunks) {
        m_logger.trace("create[offset %d, count %d, chunks with sizes (%d): %s]", p_offset, p_count,
                p_chunks.length, AbstractChunk.toSizeListString(p_chunks));

        SOP_CREATE_RESERVED_DS.start();

        m_backup.blockCreation();

        final AbstractChunk[] selectedChunks = Arrays.copyOfRange(p_chunks, p_offset, p_offset + p_count - 1);
        int created = m_chunk.getMemory().createReserved().createReserved(selectedChunks, p_addresses);

        // Initialize a new backup range every e.g. 256 MB and inform superpeer
        m_backup.registerChunks(p_offset, created, p_chunks);

        m_backup.unblockCreation();

        if (created != p_count) {
            SOP_CREATE_RESERVED_DS_ERROR.add(p_count - created);
        }

        SOP_CREATE_RESERVED_DS.stop(created);

        return created;
    }

    /**
     * Create one or multiple chunks using Chunk instances (with different sizes)
     *
     * @param p_chunks
     *         Instances of chunk objects to allocate storage for. On success, the CID is assigned to the object
     *         and the state is set to OK.
     * @return Number of chunks successfully created. If less than expected, check the chunk objects states for errors.
     */
    public int create(final AbstractChunk... p_chunks) {
        return create(null, p_chunks);
    }

    /**
     * Create one or multiple chunks using Chunk instances (with different sizes)
     *
     * @param p_addresses
     *         Optional (can be null): Array to return the raw addresses of the allocate chunks
     * @param p_chunks
     *         Instances of chunk objects to allocate storage for. On success, the CID is assigned to the object
     *         and the state is set to OK.
     * @return Number of chunks successfully created. If less than expected, check the chunk objects states for errors.
     */
    public int create(final long[] p_addresses, final AbstractChunk... p_chunks) {
        SOP_CREATE_RESERVED_DS.start();

        m_backup.blockCreation();

        int created = m_chunk.getMemory().createReserved().createReserved(p_chunks, p_addresses);

        // Initialize a new backup range every e.g. 256 MB and inform superpeer
        m_backup.registerChunks(0, created, p_chunks);

        m_backup.unblockCreation();

        if (created != p_chunks.length) {
            SOP_CREATE_RESERVED_DS_ERROR.add(p_chunks.length - created);
        }

        SOP_CREATE_RESERVED_DS.stop(created);

        return created;
    }
}
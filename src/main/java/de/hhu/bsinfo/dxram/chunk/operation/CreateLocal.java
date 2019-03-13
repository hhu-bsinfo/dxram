package de.hhu.bsinfo.dxram.chunk.operation;

import java.util.Arrays;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
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
 * Create chunks (local only and optimized)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class CreateLocal extends Operation {
    private static final ThroughputPool SOP_CREATE =
            new ThroughputPool(ChunkLocalService.class, "Create", Value.Base.B_10);
    private static final ThroughputPool SOP_CREATE_SIZES =
            new ThroughputPool(ChunkLocalService.class, "CreateSizes", Value.Base.B_10);
    private static final ThroughputPool SOP_CREATE_DS =
            new ThroughputPool(ChunkLocalService.class, "CreateDS", Value.Base.B_10);

    private static final ValuePool SOP_CREATE_ERROR = new ValuePool(ChunkLocalService.class, "CreateError");
    private static final ValuePool SOP_CREATE_SIZES_ERROR = new ValuePool(ChunkLocalService.class, "CreateSizesError");
    private static final ValuePool SOP_CREATE_DS_ERROR = new ValuePool(ChunkLocalService.class, "CreateDsError");

    static {
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_CREATE);
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_CREATE_SIZES);
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_CREATE_DS);

        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_CREATE_ERROR);
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_CREATE_SIZES_ERROR);
        StatisticsManager.get().registerOperation(ChunkLocalService.class, SOP_CREATE_DS_ERROR);
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
    public CreateLocal(final Class<? extends Service> p_parentService, final BootComponent p_boot,
            final BackupComponent p_backup, final ChunkComponent p_chunk, final NetworkComponent p_network,
            final LookupComponent p_lookup, final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);
    }

    /**
     * Create one or multiple chunks of the same size
     *
     * @param p_cids
     *         Pre-allocated array for the CIDs returned
     * @param p_offset
     *         Offset in array to start putting the CIDs to
     * @param p_count
     *         Number of chunks to allocate
     * @param p_size
     *         Size of a single chunk
     * @param p_consecutive
     *         True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *         consecutive CIDs if available.
     * @return Number of chunks successfully created
     */
    public int create(final long[] p_cids, final int p_offset, final int p_count, final int p_size,
            final boolean p_consecutive) {
        m_logger.trace("create[cids.length %d, offset %d, size %d, count %d, consecutive %b]", p_cids.length, p_offset,
                p_size, p_count, p_consecutive);

        SOP_CREATE.start();

        m_backup.blockCreation();

        int created = m_chunk.getMemory().create().create(p_cids, p_offset, p_count, p_size, p_consecutive);

        if (created < p_count) {
            for (int i = created; i < p_count; i++) {
                p_cids[i] = ChunkID.INVALID_ID;
            }
        }

        // Initialize a new backup range every e.g. 256 MB and inform superpeer
        m_backup.registerChunks(p_cids, p_offset, created, p_size);

        m_backup.unblockCreation();

        if (created < p_count) {
            SOP_CREATE_ERROR.add(p_count - created);
        }

        SOP_CREATE.stop(created);

        return created;
    }

    /**
     * Create one or multiple chunks of the same size
     *
     * @param p_cids
     *         Pre-allocated array for the CIDs returned
     * @param p_offset
     *         Offset in array to start putting the CIDs to
     * @param p_count
     *         Number of chunks to allocate
     * @param p_size
     *         Size of a single chunk
     * @return Number of chunks successfully created
     */
    public int create(final long[] p_cids, final int p_offset, final int p_count, final int p_size) {
        return create(p_cids, p_offset, p_count, p_size, false);
    }

    /**
     * Create one or multiple chunks of the same size
     *
     * @param p_cids
     *         Pre-allocated array for the CIDs returned
     * @param p_count
     *         Number of chunks to allocate
     * @param p_size
     *         Size of a single chunk
     * @param p_consecutive
     *         True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *         consecutive CIDs if available.
     * @return Number of chunks successfully created
     */
    public int create(final long[] p_cids, final int p_count, final int p_size, final boolean p_consecutive) {
        return create(p_cids, 0, p_count, p_size, p_consecutive);
    }

    /**
     * Create one or multiple chunks of the same size
     *
     * @param p_cids
     *         Pre-allocated array for the CIDs returned
     * @param p_count
     *         Number of chunks to allocate
     * @param p_size
     *         Size of a single chunk
     * @return Number of chunks successfully created
     */
    public int create(final long[] p_cids, final int p_count, final int p_size) {
        return create(p_cids, 0, p_count, p_size, false);
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_cids
     *         Pre-allocated array for the CIDs returned
     * @param p_offset
     *         Offset in array to start putting the CIDs to
     * @param p_consecutive
     *         True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *         consecutive CIDs if available.
     * @param p_sizes
     *         One or multiple (different) sizes. The amount of sizes declared here denotes the number of
     *         chunks to create
     * @return Number of chunks successfully created
     */
    public int createSizes(final long[] p_cids, final int p_offset, final boolean p_consecutive, final int... p_sizes) {
        m_logger.trace("createSizes[cids.length %d, offset %d, consecutive %b, sizes (%d): %s]", p_cids.length,
                p_offset, p_consecutive, p_sizes.length, Arrays.toString(p_sizes));

        SOP_CREATE_SIZES.start();

        m_backup.blockCreation();

        int created = m_chunk.getMemory().create().create(p_cids, p_offset, p_consecutive, p_sizes);

        if (created < p_sizes.length) {
            for (int i = created; i < p_sizes.length; i++) {
                p_cids[i] = ChunkID.INVALID_ID;
            }
        }

        // Initialize a new backup range every e.g. 256 MB and inform superpeer
        m_backup.registerChunks(p_cids, p_offset, created, p_sizes);

        m_backup.unblockCreation();

        if (created < p_sizes.length) {
            SOP_CREATE_SIZES_ERROR.add(p_sizes.length - created);
        }

        SOP_CREATE_SIZES.stop(created);

        return created;
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_cids
     *         Pre-allocated array for the CIDs returned
     * @param p_offset
     *         Offset in array to start putting the CIDs to
     * @param p_sizes
     *         One or multiple (different) sizes. The amount of sizes declared here denotes the number of
     *         chunks to create
     * @return Number of chunks successfully created
     */
    public int createSizes(final long[] p_cids, final int p_offset, int... p_sizes) {
        return createSizes(p_cids, p_offset, false, p_sizes);
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_cids
     *         Pre-allocated array for the CIDs returned
     * @param p_consecutive
     *         True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *         consecutive CIDs if available.
     * @param p_sizes
     *         One or multiple (different) sizes. The amount of sizes declared here denotes the number of
     *         chunks to create
     * @return Number of chunks successfully created
     */
    public int createSizes(final long[] p_cids, final boolean p_consecutive, int... p_sizes) {
        return createSizes(p_cids, 0, p_consecutive, p_sizes);
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_cids
     *         Pre-allocated array for the CIDs returned
     * @param p_sizes
     *         One or multiple (different) sizes. The amount of sizes declared here denotes the number of
     *         chunks to create
     * @return Number of chunks successfully created
     */
    public int createSizes(final long[] p_cids, int... p_sizes) {
        return createSizes(p_cids, 0, false, p_sizes);
    }

    /**
     * Create one or multiple chunks using Chunk instances (with different sizes)
     *
     * @param p_offset
     *         Offset in array to start putting the CIDs to
     * @param p_count
     *         Number of chunks to create (might be less than objects provided)
     * @param p_consecutive
     *         True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *         consecutive CIDs if available.
     * @param p_chunks
     *         Instances of chunk objects to allocate storage for. On success, the CID is assigned to the object
     *         and the state is set to OK.
     * @return Number of chunks successfully created. If less than expected, check the chunk objects states for errors.
     */
    public int create(final int p_offset, final int p_count, final boolean p_consecutive,
            final AbstractChunk... p_chunks) {
        m_logger.trace("create[offset %d, count %d, consecutive %b, chunks with sizes (%d): %s]", p_offset, p_count,
                p_consecutive, p_chunks.length, AbstractChunk.toSizeListString(p_chunks));

        SOP_CREATE_DS.start();

        m_backup.blockCreation();

        int created = m_chunk.getMemory().create().create(p_offset, p_count, p_consecutive, p_chunks);

        // Initialize a new backup range every e.g. 256 MB and inform superpeer
        m_backup.registerChunks(p_offset, created, p_chunks);

        m_backup.unblockCreation();

        if (created != p_count) {
            SOP_CREATE_DS_ERROR.add(p_count - created);
        }

        SOP_CREATE_DS.stop(created);

        return created;
    }

    /**
     * Create one or multiple chunks using Chunk instances (with different sizes)
     *
     * @param p_consecutive
     *         True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *         consecutive CIDs if available.
     * @param p_chunks
     *         Instances of chunk objects to allocate storage for. On success, the CID is assigned to the object
     *         and the state is set to OK.
     * @return Number of chunks successfully created. If less than expected, check the chunk objects states for errors.
     */
    public int create(final boolean p_consecutive, final AbstractChunk... p_chunks) {
        return create(0, p_chunks.length, p_consecutive, p_chunks);
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
        return create(0, p_chunks.length, false, p_chunks);
    }
}

package de.hhu.bsinfo.dxram.chunk.operation;

import java.util.Arrays;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.CreateRequest;
import de.hhu.bsinfo.dxram.chunk.messages.CreateResponse;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.InvalidNodeRoleException;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ThroughputPool;
import de.hhu.bsinfo.dxutils.stats.Value;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Create chunks (locally and remotely)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Create extends AbstractOperation implements MessageReceiver {
    private static final ThroughputPool SOP_LOCAL =
            new ThroughputPool(ChunkService.class, "CreateLocal", Value.Base.B_10);
    private static final ThroughputPool SOP_REMOTE =
            new ThroughputPool(ChunkService.class, "CreateRemote", Value.Base.B_10);
    private static final ThroughputPool SOP_INCOMING =
            new ThroughputPool(ChunkService.class, "CreateIncoming", Value.Base.B_10);
    private static final ValuePool SOP_LOCAL_ERROR = new ValuePool(ChunkService.class, "CreateLocalError");
    private static final ValuePool SOP_REMOTE_ERROR = new ValuePool(ChunkService.class, "CreateRemoteError");
    private static final ValuePool SOP_INCOMING_ERROR = new ValuePool(ChunkService.class, "CreateIncomingError");

    static {
        StatisticsManager.get().registerOperation(Create.class, SOP_LOCAL);
        StatisticsManager.get().registerOperation(Create.class, SOP_REMOTE);
        StatisticsManager.get().registerOperation(Create.class, SOP_INCOMING);
        StatisticsManager.get().registerOperation(Create.class, SOP_LOCAL_ERROR);
        StatisticsManager.get().registerOperation(Create.class, SOP_REMOTE_ERROR);
        StatisticsManager.get().registerOperation(Create.class, SOP_INCOMING_ERROR);
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
    public Create(final Class<? extends AbstractDXRAMService> p_parentService, final AbstractBootComponent p_boot,
            final BackupComponent p_backup, final ChunkComponent p_chunk, final NetworkComponent p_network,
            final LookupComponent p_lookup, final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_CREATE_REQUEST,
                CreateRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_CREATE_RESPONSE,
                CreateResponse.class);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_CREATE_REQUEST, this);
    }

    /**
     * Create one or multiple chunks of the same size
     *
     * @param p_targetNodeId
     *         Node id of target remote node to create chunk(s) on
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
    public int create(final short p_targetNodeId, final long[] p_cids, final int p_offset, final int p_count,
            final int p_size, final boolean p_consecutive) {
        m_logger.trace("create[nodeId %X, cids.length %d, offset %d, size %d, count %d, consecutive %b]",
                p_targetNodeId, p_cids.length, p_offset, p_size, p_count, p_consecutive);

        NodeRole role = m_boot.getNodeRole(p_targetNodeId);

        if (role == null || role != NodeRole.PEER) {
            throw new InvalidNodeRoleException(
                    "Remote node " + NodeID.toHexString(p_targetNodeId) + " does not exist or is not a peer");
        }

        int created;

        if (p_targetNodeId == m_boot.getNodeId()) {
            SOP_LOCAL.start(p_count);

            m_backup.blockCreation();

            created = m_chunk.getMemory().create().create(p_cids, p_offset, p_count, p_size, p_consecutive);

            if (created < p_count) {
                for (int i = created; i < p_count; i++) {
                    p_cids[i] = ChunkID.INVALID_ID;
                }
            }

            // Initialize a new backup range every e.g. 256 MB and inform superpeer
            m_backup.registerChunks(p_cids, p_offset, created, p_size);

            m_backup.unblockCreation();

            if (created < p_count) {
                SOP_LOCAL_ERROR.add(p_count - created);
            }

            SOP_LOCAL.stop(created);
        } else {
            SOP_REMOTE.start(p_count);

            // FIXME optimize and reduce message size by creating another create request which takes size and count
            // as parameters
            int[] sizes = new int[p_count];
            Arrays.fill(sizes, p_size);

            CreateRequest request = new CreateRequest(p_targetNodeId, p_consecutive, sizes);

            try {
                m_network.sendSync(request);

                CreateResponse response = request.getResponse(CreateResponse.class);

                System.arraycopy(response.getChunkIDs(), 0, p_cids, p_offset, response.getChunkIDs().length);

                // fill up with invalid IDs if not matching requested number
                for (int i = response.getChunkIDs().length; i < p_count; i++) {
                    p_cids[p_offset + response.getChunkIDs().length + i] = ChunkID.INVALID_ID;
                }

                created = response.getChunkIDs().length;
            } catch (final NetworkException e) {
                m_logger.error("Sending CreateRequest to peer %s failed: %s", NodeID.toHexString(p_targetNodeId), e);

                created = 0;
            }

            if (created < p_count) {
                SOP_REMOTE_ERROR.add(p_count - created);
            }

            SOP_REMOTE.stop();
        }

        return created;
    }

    /**
     * Create one or multiple chunks of the same size
     *
     * @param p_targetNodeId
     *         Node id of target remote node to create chunk(s) on
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
    public int create(final short p_targetNodeId, final long[] p_cids, final int p_offset, final int p_count,
            final int p_size) {
        return create(p_targetNodeId, p_cids, p_offset, p_count, p_size, false);
    }

    /**
     * Create one or multiple chunks of the same size
     *
     * @param p_targetNodeId
     *         Node id of target remote node to create chunk(s) on
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
    public int create(final short p_targetNodeId, final long[] p_cids, final int p_count, final int p_size,
            final boolean p_consecutive) {
        return create(p_targetNodeId, p_cids, 0, p_count, p_size, p_consecutive);
    }

    /**
     * Create one or multiple chunks of the same size
     *
     * @param p_targetNodeId
     *         Node id of target remote node to create chunk(s) on
     * @param p_cids
     *         Pre-allocated array for the CIDs returned
     * @param p_count
     *         Number of chunks to allocate
     * @param p_size
     *         Size of a single chunk
     * @return Number of chunks successfully created
     */
    public int create(final short p_targetNodeId, final long[] p_cids, final int p_count, final int p_size) {
        return create(p_targetNodeId, p_cids, 0, p_count, p_size, false);
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_targetNodeId
     *         Node id of target remote node to create chunk(s) on
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
    public int createSizes(final short p_targetNodeId, final long[] p_cids, final int p_offset,
            final boolean p_consecutive, int... p_sizes) {
        m_logger.trace("createSizes[nodeId %X, cids.length %d, offset %d, consecutive %b, sizes(%d): %s]",
                p_targetNodeId, p_cids.length, p_offset, p_consecutive, p_sizes.length, Arrays.toString(p_sizes));

        NodeRole role = m_boot.getNodeRole(p_targetNodeId);

        if (role == null || role != NodeRole.PEER) {
            throw new InvalidNodeRoleException(
                    "Remote node " + NodeID.toHexString(p_targetNodeId) + " does not exist or is not a peer");
        }

        int created;

        if (p_targetNodeId == m_boot.getNodeId()) {
            SOP_LOCAL.start();

            m_backup.blockCreation();

            created = m_chunk.getMemory().create().create(p_cids, p_offset, p_consecutive, p_sizes);

            if (created < p_sizes.length) {
                for (int i = created; i < p_sizes.length; i++) {
                    p_cids[i] = ChunkID.INVALID_ID;
                }
            }

            // Initialize a new backup range every e.g. 256 MB and inform superpeer
            m_backup.registerChunks(p_cids, p_offset, created, p_sizes);

            m_backup.unblockCreation();

            if (created < p_sizes.length) {
                SOP_LOCAL_ERROR.add(p_sizes.length - created);
            }

            SOP_LOCAL.stop(created);
        } else {
            // FIXME optimize and reduce message size by creating another create request which takes size and count
            // as parameters
            SOP_REMOTE.start(p_sizes.length);

            CreateRequest request = new CreateRequest(p_targetNodeId, p_consecutive, p_sizes);

            try {
                m_network.sendSync(request);

                CreateResponse response = request.getResponse(CreateResponse.class);

                System.arraycopy(response.getChunkIDs(), 0, p_cids, p_offset, response.getChunkIDs().length);

                // fill up with invalid IDs if not matching requested number
                for (int i = response.getChunkIDs().length; i < p_sizes.length; i++) {
                    p_cids[p_offset + response.getChunkIDs().length + i] = ChunkID.INVALID_ID;
                }

                created = response.getChunkIDs().length;
            } catch (final NetworkException e) {
                m_logger.error("Sending CreateRequest to peer %s failed: %s", NodeID.toHexString(p_targetNodeId), e);

                created = 0;
            }

            if (created < p_sizes.length) {
                SOP_REMOTE_ERROR.add(p_sizes.length - created);
            }

            SOP_REMOTE.stop();
        }

        return created;
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_targetNodeId
     *         Node id of target remote node to create chunk(s) on
     * @param p_cids
     *         Pre-allocated array for the CIDs returned
     * @param p_offset
     *         Offset in array to start putting the CIDs to
     * @param p_sizes
     *         One or multiple (different) sizes. The amount of sizes declared here denotes the number of
     *         chunks to create
     * @return Number of chunks successfully created
     */
    public int createSizes(final short p_targetNodeId, final long[] p_cids, final int p_offset, int... p_sizes) {
        return createSizes(p_targetNodeId, p_cids, p_offset, false, p_sizes);
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_targetNodeId
     *         Node id of target remote node to create chunk(s) on
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
    public int createSizes(final short p_targetNodeId, final long[] p_cids, final boolean p_consecutive,
            int... p_sizes) {
        return createSizes(p_targetNodeId, p_cids, 0, p_consecutive, p_sizes);
    }

    /**
     * Create one or multiple chunks with different sizes
     *
     * @param p_targetNodeId
     *         Node id of target remote node to create chunk(s) on
     * @param p_cids
     *         Pre-allocated array for the CIDs returned
     * @param p_sizes
     *         One or multiple (different) sizes. The amount of sizes declared here denotes the number of
     *         chunks to create
     * @return Number of chunks successfully created
     */
    public int createSizes(final short p_targetNodeId, final long[] p_cids, int... p_sizes) {
        return createSizes(p_targetNodeId, p_cids, 0, false, p_sizes);
    }

    /**
     * Create one or multiple chunks using Chunk instances (with different sizes)
     *
     * @param p_targetNodeId
     *         Node id of target remote node to create chunk(s) on
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
    public int create(final short p_targetNodeId, final int p_offset, final int p_count, final boolean p_consecutive,
            final AbstractChunk... p_chunks) {
        m_logger.trace("create[nodeId %X, offset %d, count %d, consecutive %b, sizes (%d): %s]", p_targetNodeId,
                p_offset, p_count, p_consecutive, p_chunks.length, AbstractChunk.toSizeListString(p_chunks));

        NodeRole role = m_boot.getNodeRole(p_targetNodeId);

        if (role == null || role != NodeRole.PEER) {
            throw new InvalidNodeRoleException(
                    "Remote node " + NodeID.toHexString(p_targetNodeId) + " does not exist or is not a peer");
        }

        int created;

        if (p_targetNodeId == m_boot.getNodeId()) {
            SOP_LOCAL.start();

            m_backup.blockCreation();

            created = m_chunk.getMemory().create().create(p_offset, p_count, p_consecutive, p_chunks);

            // Initialize a new backup range every e.g. 256 MB and inform superpeer
            m_backup.registerChunks(p_offset, created, p_chunks);

            m_backup.unblockCreation();

            if (created != p_count) {
                SOP_LOCAL_ERROR.add(p_count - created);
            }

            SOP_LOCAL.stop(created);
        } else {
            SOP_REMOTE.start(p_chunks.length);

            int[] sizes = new int[p_count];

            for (int i = 0; i < sizes.length; i++) {
                sizes[i] = p_chunks[p_offset + i].sizeofObject();
            }

            CreateRequest request = new CreateRequest(p_targetNodeId, p_consecutive, sizes);

            try {
                m_network.sendSync(request);

                CreateResponse response = request.getResponse(CreateResponse.class);

                for (int i = 0; i < response.getChunkIDs().length; i++) {
                    p_chunks[p_offset + i].setID(response.getChunkIDs()[i]);
                    p_chunks[p_offset + i].setState(ChunkState.OK);
                }

                // set invalid id and error state if allocations failed
                for (int i = response.getChunkIDs().length; i < p_count; i++) {
                    p_chunks[p_offset + i].setID(ChunkID.INVALID_ID);
                    p_chunks[p_offset + i].setState(ChunkState.UNDEFINED);
                }

                created = response.getChunkIDs().length;
            } catch (final NetworkException e) {
                m_logger.error("Sending CreateRequest to peer %s failed: %s", NodeID.toHexString(p_targetNodeId), e);

                created = 0;
            }

            if (created < p_count) {
                SOP_REMOTE_ERROR.add(p_count - created);
            }

            SOP_REMOTE.stop();
        }

        return created;
    }

    /**
     * Create one or multiple chunks using Chunk instances (with different sizes)
     *
     * @param p_targetNodeId
     *         Node id of target remote node to create chunk(s) on
     * @param p_consecutive
     *         True to enforce consecutive CIDs for all chunks to allocate, false might assign non
     *         consecutive CIDs if available.
     * @param p_chunks
     *         Instances of chunk objects to allocate storage for. On success, the CID is assigned to the object
     *         and the state is set to OK.
     * @return Number of chunks successfully created. If less than expected, check the chunk objects states for errors.
     */
    public int create(final short p_targetNodeId, final boolean p_consecutive, final AbstractChunk... p_chunks) {
        return create(p_targetNodeId, 0, p_chunks.length, p_consecutive, p_chunks);
    }

    /**
     * Create one or multiple chunks using Chunk instances (with different sizes)
     *
     * @param p_targetNodeId
     *         Node id of target remote node to create chunk(s) on
     * @param p_chunks
     *         Instances of chunk objects to allocate storage for. On success, the CID is assigned to the object
     *         and the state is set to OK.
     * @return Number of chunks successfully created. If less than expected, check the chunk objects states for errors.
     */
    public int create(final short p_targetNodeId, final AbstractChunk... p_chunks) {
        return create(p_targetNodeId, 0, p_chunks.length, false, p_chunks);
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE &&
                p_message.getSubtype() == ChunkMessages.SUBTYPE_CREATE_REQUEST) {
            CreateRequest request = (CreateRequest) p_message;

            m_logger.trace("incoming create[consecutive %b, sizes (%d): %s]", request.isConsecutive(),
                    request.getSizes().length, Arrays.toString(request.getSizes()));

            SOP_INCOMING.start(request.getSizes().length);

            long[] cids = new long[request.getSizes().length];

            int created = m_chunk.getMemory().create().create(cids, 0, request.isConsecutive(), request.getSizes());

            // Initialize a new backup range every e.g. 256 MB and inform superpeer
            // TODO memory manager write lock does not exist anymore, how and where to lock?
            m_backup.registerChunks(cids, request.getSizes());

            CreateResponse response = new CreateResponse(request, cids, created);

            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {
                m_logger.error("Sending CreateResponse to request %s failed: ", request, e);

                // delete created chunks again to avoid memory leaks because the remote did not receive
                // any chunk IDs
                for (int i = 0; i < created; i++) {
                    m_chunk.getMemory().remove().remove(cids[i]);
                }

                created = 0;
            }

            if (created < cids.length) {
                SOP_INCOMING_ERROR.add(cids.length - created);
            }

            SOP_INCOMING.stop();
        }
    }
}

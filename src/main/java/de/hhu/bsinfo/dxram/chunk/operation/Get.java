package de.hhu.bsinfo.dxram.chunk.operation;

import java.util.ArrayList;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.NetworkResponseDelayedException;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.GetMultiRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetMultiResponse;
import de.hhu.bsinfo.dxram.chunk.messages.GetRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetResponse;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupState;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.ArrayListShort;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.NodeIDBitfield;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ThroughputPool;
import de.hhu.bsinfo.dxutils.stats.Value;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

/**
 * Get the stored data of an existing chunk (locally and remotely)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Get extends AbstractOperation implements MessageReceiver {
    private static final ThroughputPool SOP_DEFAULT = new ThroughputPool(ChunkService.class, "Get", Value.Base.B_10);
    private static final ThroughputPool SOP_MULTI =
            new ThroughputPool(ChunkService.class, "GetMulti", Value.Base.B_10);
    private static final ThroughputPool SOP_INCOMING =
            new ThroughputPool(ChunkService.class, "GetIncoming", Value.Base.B_10);
    private static final ThroughputPool SOP_MULTI_INCOMING =
            new ThroughputPool(ChunkService.class, "GetMultiIncoming", Value.Base.B_10);
    private static final ValuePool SOP_ERROR = new ValuePool(ChunkService.class, "GetError");
    private static final ValuePool SOP_MULTI_ERROR = new ValuePool(ChunkService.class, "GetMultiError");
    private static final ValuePool SOP_INCOMING_ERROR = new ValuePool(ChunkService.class, "GetIncomingError");
    private static final ValuePool SOP_MULTI_INCOMING_ERROR =
            new ValuePool(ChunkService.class, "GetMultiIncomingError");

    static {
        StatisticsManager.get().registerOperation(Get.class, SOP_DEFAULT);
        StatisticsManager.get().registerOperation(Get.class, SOP_MULTI);
        StatisticsManager.get().registerOperation(Get.class, SOP_INCOMING);
        StatisticsManager.get().registerOperation(Get.class, SOP_MULTI_INCOMING);
        StatisticsManager.get().registerOperation(Get.class, SOP_ERROR);
        StatisticsManager.get().registerOperation(Get.class, SOP_MULTI_ERROR);
        StatisticsManager.get().registerOperation(Get.class, SOP_INCOMING_ERROR);
        StatisticsManager.get().registerOperation(Get.class, SOP_MULTI_INCOMING_ERROR);
    }

    // TODO have a max number of threads configuration parameter somewhere in engine settings?
    private final ArrayListShort[] m_threadLocalLocationIndexBuffer = new ArrayListShort[4096];
    private final ArrayListShort[] m_threadLocalRemotesBuffer = new ArrayListShort[4096];
    private final NodeIDBitfield[] m_threadLocalNodeIDBitfield = new NodeIDBitfield[4096];
    private final ArrayList<GetMultiRequest>[] m_threadLocalPendingRequests = new ArrayList[4096];

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
    public Get(final Class<? extends AbstractDXRAMService> p_parentService,
            final AbstractBootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_REQUEST,
                GetRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_RESPONSE,
                GetResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_MULTI_REQUEST,
                GetMultiRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_MULTI_RESPONSE,
                GetMultiResponse.class);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_REQUEST, this);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_MULTI_REQUEST, this);
    }

    /**
     * Get the data for a single chunk. Avoids array allocation of variadic chunk parameter version for a single chunk
     *
     * @param p_chunk
     *         Chunk to get
     * @return True if successful, false on error (check the chunk object state for errors)
     */
    public boolean get(final AbstractChunk p_chunk) {
        return get(p_chunk, ChunkLockOperation.NONE, -1);
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
    public boolean get(final AbstractChunk p_chunk, final ChunkLockOperation p_lockOperation) {
        return get(p_chunk, p_lockOperation, -1);
    }

    /**
     * Get the data for a single chunk. Avoids array allocation of variadic chunk parameter version for a single chunk
     *
     * @param p_chunk
     *         Chunk to get
     * @param p_lockOperation
     *         Lock operation to execute
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation in ms (-1 for unlimited. Be careful with remote chunks here! This might lead
     *         to network timeouts instead)
     * @return True if successful, false on error (check the chunk object state for errors)
     */
    public boolean get(final AbstractChunk p_chunk, final ChunkLockOperation p_lockOperation,
            final int p_lockOperationTimeoutMs) {
        m_logger.trace("get[chunk %X, lock op %s, lock timeout %d]", p_chunk.getID(), p_lockOperation,
                p_lockOperationTimeoutMs);

        boolean result = false;

        SOP_DEFAULT.start();

        // filter null values
        if (p_chunk != null) {
            // filter by invalid ID
            if (p_chunk.getID() == ChunkID.INVALID_ID) {
                p_chunk.setState(ChunkState.INVALID_ID);
            } else {
                // try to get locally, will check first if it exists
                if (m_chunk.isStorageEnabled()) {
                    m_chunk.getMemory().get().get(p_chunk, p_lockOperation, p_lockOperationTimeoutMs);
                } else {
                    p_chunk.setState(ChunkState.DOES_NOT_EXIST);
                }

                if (p_chunk.getState() == ChunkState.OK) {
                    result = true;
                } else if (p_chunk.getState() == ChunkState.DOES_NOT_EXIST) {
                    // seems like it's not available locally, check remotes for remote chunk or migrated
                    LookupRange location = m_lookup.getLookupRange(p_chunk.getID());

                    while (location.getState() == LookupState.DATA_TEMPORARY_UNAVAILABLE) {
                        try {
                            Thread.sleep(100);
                        } catch (final InterruptedException ignore) {
                        }

                        location = m_lookup.getLookupRange(p_chunk.getID());
                    }

                    if (location.getState() == LookupState.OK) {
                        // currently undefined because we still have to get it from remote
                        p_chunk.setState(ChunkState.UNDEFINED);

                        short peer = location.getPrimaryPeer();

                        if (peer == m_boot.getNodeId()) {
                            result = m_chunk.getMemory().get().get(p_chunk, p_lockOperation, p_lockOperationTimeoutMs);
                        } else {
                            // Remote get from specified peer
                            GetRequest request = new GetRequest(peer, p_lockOperation, p_lockOperationTimeoutMs,
                                    p_chunk);

                            try {
                                m_network.sendSync(request);

                                // received data is stored to chunk in request instead of copied from response

                                result = p_chunk.isStateOk();

                                if (!result) {
                                    m_lookup.invalidateRange(p_chunk.getID());
                                }

                                // Chunk data is written directly to the provided data structure on receive
                            } catch (final NetworkException e) {
                                ChunkState errorState;

                                // handle various error states and report to the user

                                if (m_backup.isActive()) {
                                    errorState = ChunkState.DATA_TEMPORARY_UNAVAILABLE;
                                } else {
                                    if (e instanceof NetworkResponseDelayedException) {
                                        errorState = ChunkState.REMOTE_REQUEST_TIMEOUT;
                                    } else {
                                        errorState = ChunkState.DATA_LOST;
                                    }
                                }

                                p_chunk.setState(errorState);
                                m_lookup.invalidate(p_chunk.getID());
                            }
                        }
                    } else if (location.getState() == LookupState.DOES_NOT_EXIST) {
                        p_chunk.setState(ChunkState.DOES_NOT_EXIST);
                    } else if (location.getState() == LookupState.DATA_LOST) {
                        p_chunk.setState(ChunkState.DATA_LOST);
                    }
                }
            }
        }

        if (!result) {
            SOP_ERROR.inc();
        }

        SOP_DEFAULT.stop();

        return result;
    }

    /**
     * Get the data of one or multiple chunks
     *
     * @param p_chunks
     *         Chunks to get
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int get(final AbstractChunk... p_chunks) {
        return get(0, p_chunks.length, ChunkLockOperation.NONE, -1, p_chunks);
    }

    /**
     * Get the data of one or multiple chunks
     *
     * @param p_lockOperation
     *         Lock operation to execute for each get operation
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation in ms
     * @param p_chunks
     *         Chunks to get
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int get(final ChunkLockOperation p_lockOperation, final int p_lockOperationTimeoutMs,
            final AbstractChunk... p_chunks) {
        return get(0, p_chunks.length, p_lockOperation, p_lockOperationTimeoutMs, p_chunks);
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
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation in ms (-1 for unlimited. Be careful with remote chunks here! This might lead
     *         to network timeouts instead)
     * @param p_chunks
     *         Chunks to get
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int get(final int p_offset, final int p_count, final ChunkLockOperation p_lockOperation,
            final int p_lockOperationTimeoutMs, final AbstractChunk... p_chunks) {
        m_logger.trace("get[offset %d, count %d, lock op %s, lock timeout %d, chunks (%d): %s]", p_offset,
                p_count, p_lockOperation, p_lockOperationTimeoutMs, p_chunks.length,
                AbstractChunk.toChunkIDListString(p_chunks));

        int totalChunksGot = 0;

        SOP_MULTI.start();

        ArrayListShort remoteLocIndexBuffer = getThreadLocalLocationIndexBuffer();
        ArrayListShort remotes = getThreadLocalRemotesBuffer();
        NodeIDBitfield nodeIDBitfield = getThreadLocalNodeIDBitfield();

        for (int i = p_offset; i < p_count; i++) {
            // filter null values and skip
            if (p_chunks[i] == null) {
                continue;
            }

            // filter by invalid IDs and skip
            if (p_chunks[i].getID() == ChunkID.INVALID_ID) {
                p_chunks[i].setState(ChunkState.INVALID_ID);
                continue;
            }

            // try to get locally, will check first if it exists
            if (m_chunk.isStorageEnabled()) {
                m_chunk.getMemory().get().get(p_chunks[i], p_lockOperation, p_lockOperationTimeoutMs);
            } else {
                p_chunks[i].setState(ChunkState.DOES_NOT_EXIST);
            }

            if (p_chunks[i].getState() == ChunkState.OK) {
                totalChunksGot++;

                // start at index 0 for location buffer, remote invalid because local
                remoteLocIndexBuffer.add(i - p_offset, NodeID.INVALID_ID);
            } else if (p_chunks[i].getState() == ChunkState.DOES_NOT_EXIST) {
                // seems like it's not available locally, check remotes for remote chunk or migrated
                LookupRange location = m_lookup.getLookupRange(p_chunks[i].getID());

                while (location.getState() == LookupState.DATA_TEMPORARY_UNAVAILABLE) {
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException ignore) {
                    }

                    location = m_lookup.getLookupRange(p_chunks[i].getID());
                }

                short remotePeer;

                if (location.getState() == LookupState.OK) {
                    // currently undefined because we still have to get it from remote
                    p_chunks[i].setState(ChunkState.UNDEFINED);

                    remotePeer = location.getPrimaryPeer();

                    if (!nodeIDBitfield.set(remotePeer, true)) {
                        remotes.add(remotePeer);
                    }
                } else if (location.getState() == LookupState.DOES_NOT_EXIST) {
                    p_chunks[i].setState(ChunkState.DOES_NOT_EXIST);
                    remotePeer = NodeID.INVALID_ID;
                } else if (location.getState() == LookupState.DATA_LOST) {
                    p_chunks[i].setState(ChunkState.DATA_LOST);
                    remotePeer = NodeID.INVALID_ID;
                } else {
                    throw new IllegalStateException("Unhandled state, location state: " + location.getState());
                }

                // start at index 0 for location buffer
                remoteLocIndexBuffer.add(i - p_offset, remotePeer);
            } else {
                throw new IllegalStateException("Unhandled chunk state: " + p_chunks[i].getState());
            }
        }

        ArrayList<GetMultiRequest> pendingRequests = getThreadLocalPendingReqArray();

        // generate requests
        for (int i = 0; i < remotes.getSize(); i++) {
            short remote = remotes.get(i);
            nodeIDBitfield.set(remote, false);

            GetMultiRequest req = new GetMultiRequest(remote, p_lockOperation, p_lockOperationTimeoutMs,
                    remoteLocIndexBuffer, remote, p_offset, p_chunks);

            pendingRequests.add(req);
        }

        // count failures instead of success
        int failures = 0;

        // send requests
        for (int i = 0; i < pendingRequests.size(); i++) {
            GetMultiRequest request = pendingRequests.get(i);

            try {
                m_network.sendSync(request, false);
            } catch (final NetworkException e) {
                ChunkState errorState;

                if (m_backup.isActive()) {
                    errorState = ChunkState.DATA_TEMPORARY_UNAVAILABLE;
                } else {
                    errorState = ChunkState.DATA_LOST;
                }

                AbstractChunk[] chunks = request.getChunks();
                int startOffset = request.getChunksStartOffset();
                ArrayListShort locaIndexBuf = request.getLocationIndexBuffer();

                for (int j = 0; j < locaIndexBuf.getSize(); j++) {
                    if (locaIndexBuf.get(j) == request.getTargetRemoteLocation()) {
                        chunks[startOffset + j].setState(errorState);
                        m_lookup.invalidateRange(chunks[startOffset + j].getID());
                    }
                }

                failures += request.getChunkCount();
                pendingRequests.set(i, null);
            }
        }

        // collect responses
        for (int i = 0; i < pendingRequests.size(); i++) {
            GetMultiRequest request = pendingRequests.get(i);

            if (request != null) {
                try {
                    request.waitForResponse(10000);
                } catch (final NetworkException e) {
                    m_network.cancelRequest(request);

                    ChunkState errorState = ChunkState.REMOTE_REQUEST_TIMEOUT;

                    AbstractChunk[] chunks = request.getChunks();
                    int startOffset = request.getChunksStartOffset();
                    ArrayListShort locaIndexBuf = request.getLocationIndexBuffer();

                    for (int j = 0; j < locaIndexBuf.getSize(); j++) {
                        if (locaIndexBuf.get(j) == request.getTargetRemoteLocation()) {
                            chunks[startOffset + j].setState(errorState);
                        }
                    }

                    failures += request.getChunkCount();
                }
            }
        }

        pendingRequests.clear();
        remotes.clear();
        remoteLocIndexBuffer.clear();

        totalChunksGot = p_count - failures;

        if (totalChunksGot < p_count) {
            SOP_MULTI_ERROR.add(p_count - totalChunksGot);
        }

        SOP_MULTI.stop(totalChunksGot);

        return totalChunksGot;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE) {
            if (p_message.getSubtype() == ChunkMessages.SUBTYPE_GET_REQUEST) {
                GetRequest request = (GetRequest) p_message;

                m_logger.trace("incoming get[lock op %s, lock timeout %d, chunk: %s]", request.getLockOperation(),
                        request.getLockOperationTimeoutMs(), ChunkID.toHexString(request.getChunkID()));

                SOP_INCOMING.start();

                boolean successful;

                ChunkByteArray chunk = m_chunk.getMemory().get().get(request.getChunkID(), request.getLockOperation(),
                        request.getLockOperationTimeoutMs());

                successful = chunk.isStateOk();

                GetResponse response = new GetResponse(request, chunk);

                try {
                    m_network.sendMessage(response);
                } catch (final NetworkException e) {
                    m_logger.error("Sending GetResponse for chunk %s failed: %s", ChunkID.toHexString(chunk.getID()),
                            e);

                    successful = false;
                }

                if (!successful) {
                    SOP_INCOMING_ERROR.inc();
                }

                SOP_INCOMING.stop();
            } else if (p_message.getSubtype() == ChunkMessages.SUBTYPE_GET_MULTI_REQUEST) {
                GetMultiRequest request = (GetMultiRequest) p_message;

                m_logger.trace("incoming getMulti[lock op %s, lock timeout %d, chunks (%d): %s]",
                        request.getLockOperation(), request.getLockOperationTimeoutMs(), request.getChunkIDs().length,
                        ChunkID.chunkIDArrayToString(request.getChunkIDs()));

                SOP_MULTI_INCOMING.start(request.getChunkIDs().length);

                ChunkByteArray[] chunks = new ChunkByteArray[request.getChunkIDs().length];
                int successfulGets = 0;

                for (int i = 0; i < chunks.length; i++) {
                    chunks[i] = m_chunk.getMemory().get().get(request.getChunkIDs()[i], request.getLockOperation(),
                            request.getLockOperationTimeoutMs());

                    if (chunks[i].isStateOk()) {
                        successfulGets++;
                    }
                }

                GetMultiResponse response = new GetMultiResponse(request, chunks);

                try {
                    m_network.sendMessage(response);
                } catch (final NetworkException e) {
                    m_logger.error("Sending GetMultiResponse for %d chunks failed: %s", chunks.length, e);

                    successfulGets = 0;
                }

                if (successfulGets < chunks.length) {
                    SOP_MULTI_INCOMING_ERROR.add(chunks.length - successfulGets);
                }

                SOP_MULTI_INCOMING.stop();
            }
        }
    }

    /**
     * Get a thread local instance avoiding allocations
     *
     * @return Thread local instance
     */
    private ArrayListShort getThreadLocalLocationIndexBuffer() {
        ArrayListShort locationIndexBuffer =
                m_threadLocalLocationIndexBuffer[(int) Thread.currentThread().getId()];

        if (locationIndexBuffer == null) {
            locationIndexBuffer = new ArrayListShort(100);
            m_threadLocalLocationIndexBuffer[(int) Thread.currentThread().getId()] = locationIndexBuffer;
        }

        return locationIndexBuffer;
    }

    private ArrayListShort getThreadLocalRemotesBuffer() {
        ArrayListShort remotesBuffer = m_threadLocalRemotesBuffer[(int) Thread.currentThread().getId()];

        if (remotesBuffer == null) {
            remotesBuffer = new ArrayListShort(10);
            m_threadLocalRemotesBuffer[(int) Thread.currentThread().getId()] = remotesBuffer;
        }

        return remotesBuffer;
    }

    private NodeIDBitfield getThreadLocalNodeIDBitfield() {
        NodeIDBitfield bitfield = m_threadLocalNodeIDBitfield[(int) Thread.currentThread().getId()];

        if (bitfield == null) {
            bitfield = new NodeIDBitfield();
            m_threadLocalNodeIDBitfield[(int) Thread.currentThread().getId()] = bitfield;
        }

        return bitfield;
    }

    private ArrayList<GetMultiRequest> getThreadLocalPendingReqArray() {
        ArrayList<GetMultiRequest> requests = m_threadLocalPendingRequests[(int) Thread.currentThread().getId()];

        if (requests == null) {
            requests = new ArrayList<>(10);
            m_threadLocalPendingRequests[(int) Thread.currentThread().getId()] = requests;
        }

        return requests;
    }
}

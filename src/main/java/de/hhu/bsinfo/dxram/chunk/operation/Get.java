package de.hhu.bsinfo.dxram.chunk.operation;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

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
import de.hhu.bsinfo.dxram.chunk.messages.GetRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetResponse;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupState;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
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
    private static final ThroughputPool SOP_INCOMING =
            new ThroughputPool(ChunkService.class, "GetIncoming", Value.Base.B_10);

    private static final ValuePool SOP_ERROR = new ValuePool(ChunkService.class, "GetError");
    private static final ValuePool SOP_INCOMING_ERROR = new ValuePool(ChunkService.class, "GetIncomingError");

    static {
        StatisticsManager.get().registerOperation(Get.class, SOP_DEFAULT);
        StatisticsManager.get().registerOperation(Get.class, SOP_INCOMING);
        StatisticsManager.get().registerOperation(Get.class, SOP_ERROR);
        StatisticsManager.get().registerOperation(Get.class, SOP_INCOMING_ERROR);
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
    public Get(final Class<? extends AbstractDXRAMService> p_parentService,
            final AbstractBootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_REQUEST,
                GetRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_RESPONSE,
                GetResponse.class);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_REQUEST, this);
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
    public boolean get(final AbstractChunk p_chunk , final ChunkLockOperation p_lockOperation) {
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
    public boolean get(final AbstractChunk p_chunk , final ChunkLockOperation p_lockOperation,
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
                m_chunk.getMemory().get().get(p_chunk, p_lockOperation, p_lockOperationTimeoutMs);

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

        SOP_DEFAULT.start();

        // sort by local and remote data: process local first, remote further below
        Map<Short, ArrayList<AbstractChunk>> remoteChunksByPeers = new TreeMap<>();

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
            m_chunk.getMemory().get().get(p_chunks[i], p_lockOperation, p_lockOperationTimeoutMs);

            if (p_chunks[i].getState() == ChunkState.OK) {
                totalChunksGot++;
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

                if (location.getState() == LookupState.OK) {
                    // currently undefined because we still have to get it from remote
                    p_chunks[i].setState(ChunkState.UNDEFINED);

                    short peer = location.getPrimaryPeer();

                    ArrayList<AbstractChunk> remoteChunksOfPeer =
                            remoteChunksByPeers.computeIfAbsent(peer, a -> new ArrayList<>());
                    remoteChunksOfPeer.add(p_chunks[i]);
                } else if (location.getState() == LookupState.DOES_NOT_EXIST) {
                    p_chunks[i].setState(ChunkState.DOES_NOT_EXIST);
                } else if (location.getState() == LookupState.DATA_LOST) {
                    p_chunks[i].setState(ChunkState.DATA_LOST);
                }
            }
        }

        // go for remote ones by each peer
        for (final Map.Entry<Short, ArrayList<AbstractChunk>> peerWithChunks : remoteChunksByPeers.entrySet()) {
            short peer = peerWithChunks.getKey();
            ArrayList<AbstractChunk> remoteChunks = peerWithChunks.getValue();

            if (peer == m_boot.getNodeId()) {
                // local get, migrated data to current node
                for (AbstractChunk chunk : remoteChunks) {
                    m_chunk.getMemory().get().get(chunk, p_lockOperation, p_lockOperationTimeoutMs);

                    if (chunk.isStateOk()) {
                        totalChunksGot++;
                    }
                }
            } else {
                // Remote get from specified peer
                GetRequest request = new GetRequest(peer, p_lockOperation, p_lockOperationTimeoutMs,
                        remoteChunks.toArray(new AbstractChunk[remoteChunks.size()]));

                try {
                    m_network.sendSync(request);

                    // received data is stored to chunks in request instead of copied from response

                    for (AbstractChunk chunk : remoteChunks) {
                        if (chunk.getState() != ChunkState.OK) {
                            m_lookup.invalidateRange(chunk.getID());
                        } else {
                            totalChunksGot++;
                        }
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

                    for (AbstractChunk chunk : remoteChunks) {
                        chunk.setState(errorState);
                        m_lookup.invalidate(chunk.getID());
                    }
                }
            }
        }

        if (totalChunksGot < p_count) {
            SOP_ERROR.add(p_count - totalChunksGot);
        }

        SOP_DEFAULT.stop(totalChunksGot);

        return totalChunksGot;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE &&
                p_message.getSubtype() == ChunkMessages.SUBTYPE_GET_REQUEST) {
            GetRequest request = (GetRequest) p_message;

            m_logger.trace("incoming get[lock op %s, lock timeout %d, chunks (%d): %s]", request.getLockOperation(),
                    request.getLockOperationTimeoutMs(), request.getChunkIDs().length,
                    ChunkID.chunkIDArrayToString(request.getChunkIDs()));

            SOP_INCOMING.start(request.getChunkIDs().length);

            ChunkByteArray[] chunks = new ChunkByteArray[request.getChunkIDs().length];
            int successfulGets = 0;

            for (int i = 0; i < chunks.length; i++) {
                chunks[i] = m_chunk.getMemory().get().get(request.getChunkIDs()[i], request.getLockOperation(),
                        request.getLockOperationTimeoutMs());

                if (chunks[i].isStateOk()) {
                    successfulGets++;
                }
            }

            GetResponse response = new GetResponse(request, chunks);

            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {
                m_logger.error("Sending GetResponse for %d chunks failed: %s", chunks.length, e);

                successfulGets = 0;
            }

            if (successfulGets < chunks.length) {
                SOP_INCOMING_ERROR.add(chunks.length - successfulGets);
            }

            SOP_INCOMING.stop();
        }
    }
}

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
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.PutRequest;
import de.hhu.bsinfo.dxram.chunk.messages.PutResponse;
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
 * Put data of a chunk to the key-value memory (locally and remotely)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Put extends AbstractOperation implements MessageReceiver {
    private static final ThroughputPool SOP_DEFAULT = new ThroughputPool(ChunkService.class, "Put", Value.Base.B_10);
    private static final ThroughputPool SOP_INCOMING =
            new ThroughputPool(ChunkService.class, "PutIncoming", Value.Base.B_10);

    private static final ValuePool SOP_ERROR = new ValuePool(ChunkService.class, "PutError");
    private static final ValuePool SOP_INCOMING_ERROR = new ValuePool(ChunkService.class, "PutIncomingError");

    static {
        StatisticsManager.get().registerOperation(Put.class, SOP_DEFAULT);
        StatisticsManager.get().registerOperation(Put.class, SOP_INCOMING);
        StatisticsManager.get().registerOperation(Put.class, SOP_ERROR);
        StatisticsManager.get().registerOperation(Put.class, SOP_INCOMING_ERROR);
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
    public Put(final Class<? extends AbstractDXRAMService> p_parentService,
            final AbstractBootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_REQUEST,
                PutRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_RESPONSE,
                PutResponse.class);

        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_REQUEST, this);
    }

    /**
     * Put the data for a single chunk. Avoids array allocation of variadic chunk parameter version for a single chunk
     *
     * @param p_chunk
     *         Chunk to put
     * @return True if successful, false on error (check the chunk object state for errors)
     */
    public boolean put(final AbstractChunk p_chunk) {
        return put(p_chunk, ChunkLockOperation.NONE, -1);
    }

    /**
     * Put the data for a single chunk. Avoids array allocation of variadic chunk parameter version for a single chunk
     *
     * @param p_chunk
     *         Chunk to put
     * @param p_lockOperation
     *         Lock operation to execute
     * @return True if successful, false on error (check the chunk object state for errors)
     */
    public boolean put(final AbstractChunk p_chunk, final ChunkLockOperation p_lockOperation) {
        return put(p_chunk, p_lockOperation, -1);
    }

    /**
     * Put the data for a single chunk. Avoids array allocation of variadic chunk parameter version for a single chunk
     *
     * @param p_chunk
     *         Chunk to put
     * @param p_lockOperation
     *         Lock operation to execute
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation in ms (-1 for unlimited. Be careful with remote chunks here! This might lead
     *         to network timeouts instead)
     * @return True if successful, false on error (check the chunk object state for errors)
     */
    public boolean put(final AbstractChunk p_chunk , final ChunkLockOperation p_lockOperation,
            final int p_lockOperationTimeoutMs) {
        m_logger.trace("put[chunk %s, lock op %s, lock timeout %d]", ChunkID.toHexString(p_chunk.getID()),
                p_lockOperation, p_lockOperationTimeoutMs);

        boolean result = false;

        Map<BackupRange, ArrayList<AbstractChunk>> remoteChunksByBackupRange = new TreeMap<>();

        SOP_DEFAULT.start();

        // filter null value
        if (p_chunk != null) {
            // filter by invalid ID
            if (p_chunk.getID() == ChunkID.INVALID_ID) {
                p_chunk.setState(ChunkState.INVALID_ID);
            } else {
                m_chunk.getMemory().put().put(p_chunk, p_lockOperation, p_lockOperationTimeoutMs);

                if (p_chunk.getState() == ChunkState.OK) {
                    result = true;

                    // log changes to backup
                    if (m_backup.isActive()) {
                        // sort by backup peers
                        BackupRange backupRange = m_backup.getBackupRange(p_chunk.getID());
                        ArrayList<AbstractChunk> remoteChunksOfBackupRange =
                                remoteChunksByBackupRange.computeIfAbsent(backupRange, a -> new ArrayList<>());
                        remoteChunksOfBackupRange.add(p_chunk);
                    }
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
                            // local put, migrated data to current node
                            result = m_chunk.getMemory().put().put(p_chunk, p_lockOperation, p_lockOperationTimeoutMs);
                        } else {
                            // Remote get from specified peer
                            PutRequest request = new PutRequest(peer, p_lockOperation, p_lockOperationTimeoutMs,
                                    p_chunk);

                            try {
                                m_network.sendSync(request);

                                PutResponse response = request.getResponse(PutResponse.class);

                                byte[] statusCodes = response.getStatusCodes();

                                result = statusCodes[0] == ChunkState.OK.ordinal();

                                if (!result) {
                                    m_lookup.invalidateRange(p_chunk.getID());
                                }
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
     * Put the data of one or multiple chunks
     *
     * @param p_chunks
     *         Chunks to put
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int put(final AbstractChunk... p_chunks) {
        return put(0, p_chunks.length, ChunkLockOperation.NONE, -1, p_chunks);
    }

    /**
     * Put the data of one or multiple chunks
     *
     * @param p_lockOperation
     *         Lock operation to execute for each put operation
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation in ms (-1 for unlimited. Be careful with remote chunks here! This might lead
     *         to network timeouts instead)
     * @param p_chunks
     *         Chunks to put
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int put(final ChunkLockOperation p_lockOperation, final int p_lockOperationTimeoutMs,
            final AbstractChunk... p_chunks) {
        return put(0, p_chunks.length, p_lockOperation, p_lockOperationTimeoutMs, p_chunks);
    }

    /**
     * Put the data of one or multiple chunks
     *
     * @param p_offset
     *         Offset in array where to start put operations
     * @param p_count
     *         Number of chunks to put (might be less array size/number of chunks provided)
     * @param p_lockOperation
     *         Lock operation to execute for each put operation
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation in ms (-1 for unlimited. Be careful with remote chunks here! This might lead
     *         to network timeouts instead)
     * @param p_chunks
     *         Chunks to put
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int put(final int p_offset, final int p_count, final ChunkLockOperation p_lockOperation,
            final int p_lockOperationTimeoutMs, final AbstractChunk... p_chunks) {
        m_logger.trace("put[offset %d, count %d, lock op %s, lock timeout %d, chunks (%d): %s]", p_offset,
                p_count, p_lockOperation, p_lockOperationTimeoutMs, p_chunks.length,
                AbstractChunk.toChunkIDListString(p_chunks));

        int totalChunksPut = 0;

        SOP_DEFAULT.start();

        // sort by local and remote data: process local first, remote further below
        Map<Short, ArrayList<AbstractChunk>> remoteChunksByPeers = new TreeMap<>();
        Map<BackupRange, ArrayList<AbstractChunk>> remoteChunksByBackupRange = new TreeMap<>();

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

            m_chunk.getMemory().put().put(p_chunks[i], p_lockOperation, p_lockOperationTimeoutMs);

            if (p_chunks[i].getState() == ChunkState.OK) {
                totalChunksPut++;

                // log changes to backup
                if (m_backup.isActive()) {
                    // sort by backup peers
                    BackupRange backupRange = m_backup.getBackupRange(p_chunks[i + p_offset].getID());
                    ArrayList<AbstractChunk> remoteChunksOfBackupRange =
                            remoteChunksByBackupRange.computeIfAbsent(backupRange, a -> new ArrayList<>());
                    remoteChunksOfBackupRange.add(p_chunks[i + p_offset]);
                }
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
                // local put, migrated data to current node
                for (AbstractChunk chunk : remoteChunks) {
                    m_chunk.getMemory().put().put(chunk, p_lockOperation, p_lockOperationTimeoutMs);

                    if (chunk.isStateOk()) {
                        totalChunksPut++;
                    }
                }
            } else {
                // Remote get from specified peer
                PutRequest request = new PutRequest(peer, p_lockOperation, p_lockOperationTimeoutMs,
                        remoteChunks.toArray(new AbstractChunk[remoteChunks.size()]));

                try {
                    m_network.sendSync(request);

                    PutResponse response = request.getResponse(PutResponse.class);

                    byte[] statusCodes = response.getStatusCodes();

                    // try short cut, i.e. all puts successful
                    if (statusCodes.length == 1 && statusCodes[0] == ChunkState.OK.ordinal()) {
                        totalChunksPut += remoteChunks.size();

                        for (AbstractChunk ds : remoteChunks) {
                            ds.setState(ChunkState.OK);
                        }
                    } else {
                        for (int i = 0; i < statusCodes.length; i++) {
                            remoteChunks.get(i).setState(ChunkState.values()[statusCodes[i]]);

                            if (statusCodes[i] == ChunkState.OK.ordinal()) {
                                totalChunksPut++;
                            } else {
                                m_lookup.invalidateRange(remoteChunks.get(i).getID());
                            }
                        }
                    }
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

        if (totalChunksPut < p_count) {
            SOP_ERROR.add(p_count - totalChunksPut);
        }

        SOP_DEFAULT.stop();

        return totalChunksPut;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE &&
                p_message.getSubtype() == ChunkMessages.SUBTYPE_PUT_REQUEST) {
            PutRequest request = (PutRequest) p_message;

            m_logger.trace("incoming put[lock op %s, lock timeout %d, chunks (%d): %s]", request.getLockOperation(),
                    request.getLockOperationTimeoutMs(), request.getChunkIDs().length,
                    ChunkID.chunkIDArrayToString(request.getChunkIDs()));

            SOP_INCOMING.start(request.getChunkIDs().length);

            long[] chunkIDs = request.getChunkIDs();
            byte[][] data = request.getChunkData();

            byte[] statusChunks = new byte[chunkIDs.length];
            int successfulPuts = 0;

            Map<BackupRange, ArrayList<AbstractChunk>> remoteChunksByBackupRange = new TreeMap<>();

            for (int i = 0; i < chunkIDs.length; i++) {
                ChunkState state = m_chunk.getMemory().put().put(request.getChunkIDs()[i], request.getChunkData()[i],
                        request.getLockOperation(), request.getLockOperationTimeoutMs());
                statusChunks[i] = (byte) state.ordinal();

                if (state == ChunkState.OK) {
                    if (m_backup.isActive()) {
                        // sort by backup peers
                        BackupRange backupRange = m_backup.getBackupRange(chunkIDs[i]);
                        ArrayList<AbstractChunk> remoteChunksOfBackupRange =
                                remoteChunksByBackupRange.computeIfAbsent(backupRange, k -> new ArrayList<>());
                        remoteChunksOfBackupRange.add(new ChunkByteArray(chunkIDs[i], data[i]));
                    }

                    successfulPuts++;
                }
            }

            // send response to remote
            PutResponse response;

            // cut message length if all were successful
            if (successfulPuts == chunkIDs.length) {
                response = new PutResponse(request, (byte) ChunkState.OK.ordinal());
            } else {
                // we got errors, default message
                response = new PutResponse(request, statusChunks);
            }

            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {
                m_logger.error("Sending PutResponse to request %s failed: %s", request, e);

                successfulPuts = 0;
            }

            if (successfulPuts < chunkIDs.length) {
                SOP_INCOMING_ERROR.add(chunkIDs.length - successfulPuts);
            }

            SOP_INCOMING.stop();
        }
    }
}

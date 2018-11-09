package de.hhu.bsinfo.dxram.chunk.operation;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
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
import de.hhu.bsinfo.dxram.chunk.messages.LockRequest;
import de.hhu.bsinfo.dxram.chunk.messages.LockResponse;
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
 * Single lock operation for one or multiple chunks locally or remotely
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.11.2018
 */
public class Lock extends AbstractOperation implements MessageReceiver {
    private static final ThroughputPool SOP_DEFAULT = new ThroughputPool(ChunkService.class, "Lock", Value.Base.B_10);
    private static final ThroughputPool SOP_INCOMING =
            new ThroughputPool(ChunkService.class, "LockIncoming", Value.Base.B_10);

    private static final ValuePool SOP_ERROR = new ValuePool(ChunkService.class, "LockError");
    private static final ValuePool SOP_INCOMING_ERROR = new ValuePool(ChunkService.class, "LockIncomingError");

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
    public Lock(final Class<? extends AbstractDXRAMService> p_parentService,
            final AbstractBootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_LOCK_REQUEST,
                LockRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_LOCK_RESPONSE,
                LockResponse.class);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_LOCK_REQUEST, this);
    }

    /**
     * Lock one or multiple chunks
     *
     * @param p_lock
     *         True to lock, false to unlock
     * @param p_writeLock
     *         True for write lock, false for read lock
     * @param p_lockTimeoutMs
     *         Timeout for lock operation in ms (-1 for unlimited. Be careful with remote chunks here! This might lead
     *         to network timeouts instead)
     * @param p_chunks
     *         Chunks to lock
     * @return Number of chunks successfully locked/unlocked
     */
    public int lock(final boolean p_lock, final boolean p_writeLock, final int p_lockTimeoutMs,
            final AbstractChunk... p_chunks) {
        return lock(p_lock, p_writeLock, p_lockTimeoutMs, p_chunks, 0, p_chunks.length);
    }

    /**
     * Lock one or multiple chunks
     *
     * @param p_lock
     *         True to lock, false to unlock
     * @param p_writeLock
     *         True for write lock, false for read lock
     * @param p_lockTimeoutMs
     *         Timeout for lock operation in ms (-1 for unlimited. Be careful with remote chunks here! This might lead
     *         to network timeouts instead)
     * @param p_chunks
     *         Chunks to lock
     * @param p_offset
     *         Offset in array where to start the operation
     * @param p_count
     *         Number of chunks to lock/unlock (might be less array size/number of chunks provided)
     * @return Number of chunks successfully locked/unlocked
     */
    public int lock(final boolean p_lock, final boolean p_writeLock, final int p_lockTimeoutMs,
            final AbstractChunk[] p_chunks, final int p_offset, final int p_count) {
        m_logger.trace("lock[lock %b, writeLock %b, timeout %d, offset %d, count %d, chunks (%d): %s]", p_lock,
                p_writeLock, p_lock, p_offset, p_count, p_chunks.length, AbstractChunk.toChunkIDListString(p_chunks));

        int totalChunks = 0;

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

            // try locally
            if (p_lock) {
                m_chunk.getMemory().lock().lock(p_chunks[i], p_writeLock, p_lockTimeoutMs);
            } else {
                m_chunk.getMemory().lock().unlock(p_chunks[i], p_writeLock);
            }

            if (p_chunks[i].getState() == ChunkState.OK) {
                totalChunks++;
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
                // local, migrated data to current node
                for (AbstractChunk chunk : remoteChunks) {
                    chunk.setState(m_chunk.getMemory().resize().resize(chunk.getID(), chunk.sizeofObject()));

                    if (chunk.isStateOk()) {
                        totalChunks++;
                    }
                }
            } else {
                // Remote from specified peer
                LockRequest request = new LockRequest(peer, p_lock, p_writeLock, p_lockTimeoutMs,
                        remoteChunks.toArray(new AbstractChunk[remoteChunks.size()]));

                try {
                    m_network.sendSync(request);

                    for (AbstractChunk chunk : remoteChunks) {
                        if (chunk.getState() != ChunkState.OK) {
                            m_lookup.invalidateRange(chunk.getID());
                        } else {
                            totalChunks++;
                        }
                    }

                    LockResponse response = (LockResponse) request.getResponse();

                    for (int i = 0; i < remoteChunks.size(); i++) {
                        remoteChunks.get(i).setState(ChunkState.values()[response.getStatusCodes()[i]]);

                        if (remoteChunks.get(i).isStateOk()) {
                            totalChunks++;
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

        if (totalChunks < p_count) {
            SOP_ERROR.add(p_count - totalChunks);
        }

        SOP_DEFAULT.stop(totalChunks);

        return totalChunks;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE &&
                p_message.getSubtype() == ChunkMessages.SUBTYPE_LOCK_REQUEST) {
            LockRequest request = (LockRequest) p_message;

            m_logger.trace("incoming lock[lock %b, writeLock %b, timeout %d, chunks (%d): %s]",
                    request.lock(), request.isWriteLock(), request.getLockOperationTimeoutMs(),
                    request.getChunkIDs().length, ChunkID.chunkIDArrayToString(request.getChunkIDs()));

            SOP_INCOMING.start(request.getChunkIDs().length);

            int successful = 0;
            byte[] chunkStates = new byte[request.getChunkIDs().length];

            for (int i = 0; i < chunkStates.length; i++) {
                ChunkState state;

                if (request.lock()) {
                    state = m_chunk.getMemory().lock().lock(request.getChunkIDs()[i], request.isWriteLock(),
                            request.getLockOperationTimeoutMs());
                } else {
                    state =  m_chunk.getMemory().lock().unlock(request.getChunkIDs()[i], request.isWriteLock());
                }

                chunkStates[i] = (byte) state.ordinal();

                if (state == ChunkState.OK) {
                    successful++;
                }
            }

            LockResponse response = new LockResponse(request, chunkStates);

            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {
                m_logger.error("Sending LockResponse for %d chunks failed: %s", request.getChunkIDs().length, e);

                successful = 0;
            }

            if (successful < chunkStates.length) {
                SOP_INCOMING_ERROR.add(chunkStates.length - successful);
            }

            SOP_INCOMING.stop();
        }
    }
}

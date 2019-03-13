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
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.ResizeRequest;
import de.hhu.bsinfo.dxram.chunk.messages.ResizeResponse;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupState;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ThroughputPool;
import de.hhu.bsinfo.dxutils.stats.Value;
import de.hhu.bsinfo.dxutils.stats.ValuePool;

public class Resize extends Operation implements MessageReceiver {
    private static final ThroughputPool SOP_DEFAULT = new ThroughputPool(ChunkService.class, "Resize", Value.Base.B_10);
    private static final ThroughputPool SOP_INCOMING =
            new ThroughputPool(ChunkService.class, "ResizeIncoming", Value.Base.B_10);

    private static final ValuePool SOP_ERROR = new ValuePool(ChunkService.class, "ResizeError");
    private static final ValuePool SOP_INCOMING_ERROR = new ValuePool(ChunkService.class, "ResizeIncomingError");

    static {
        StatisticsManager.get().registerOperation(Put.class, SOP_DEFAULT);
        StatisticsManager.get().registerOperation(Put.class, SOP_INCOMING);
        StatisticsManager.get().registerOperation(Put.class, SOP_ERROR);
        StatisticsManager.get().registerOperation(Put.class, SOP_INCOMING_ERROR);
    }

    public Resize(final Class<? extends Service> p_parentService,
            final BootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_RESIZE_REQUEST,
                ResizeRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_RESIZE_RESPONSE,
                ResizeResponse.class);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_RESIZE_REQUEST, this);
    }

    public int resize(final AbstractChunk... p_chunks) {
        return resize(p_chunks, 0, p_chunks.length);
    }

    public int resize(final AbstractChunk[] p_chunks, final int p_offset, final int p_count) {
        m_logger.trace("resize[offset %d, count %d, chunks (%d): %s]", p_offset,
                p_count, p_chunks.length, AbstractChunk.toChunkIDListString(p_chunks));

        int totalChunks = 0;

        SOP_DEFAULT.start();

        // sort by local and remote data: process local first, remote further below
        Map<Short, ArrayList<AbstractChunk>> remoteChunksByPeers = new TreeMap<>();
        int sizesPos = 0;

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

            // try to resize locally
            p_chunks[i].setState(m_chunk.getMemory().resize().resize(p_chunks[i].getID(), p_chunks[i].sizeofObject()));

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
                ResizeRequest request = new ResizeRequest(peer,
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

                    ResizeResponse response = (ResizeResponse) request.getResponse();

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
                p_message.getSubtype() == ChunkMessages.SUBTYPE_RESIZE_REQUEST) {
            ResizeRequest request = (ResizeRequest) p_message;

            m_logger.trace("incoming resize[chunks (%d): %s]", request.getChunkIDs().length,
                    ChunkID.chunkIDArrayToString(request.getChunkIDs()));

            SOP_INCOMING.start(request.getChunkIDs().length);

            int successful = 0;
            byte[] chunkStates = new byte[request.getChunkIDs().length];

            for (int i = 0; i < chunkStates.length; i++) {
                ChunkState state = m_chunk.getMemory().resize().resize(request.getChunkIDs()[i],
                        request.getNewSizes()[i]);
                chunkStates[i] = (byte) state.ordinal();

                if (state == ChunkState.OK) {
                    successful++;
                }
            }

            ResizeResponse response = new ResizeResponse(request, chunkStates);

            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {
                m_logger.error("Sending ResizeResponse for %d chunks failed: %s", request.getChunkIDs().length, e);

                successful = 0;
            }

            if (successful < chunkStates.length) {
                SOP_INCOMING_ERROR.add(chunkStates.length - successful);
            }

            SOP_INCOMING.stop();
        }
    }
}

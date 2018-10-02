package de.hhu.bsinfo.dxram.chunk.operation;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.data.ChunkAnon;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.PutAnonRequest;
import de.hhu.bsinfo.dxram.chunk.messages.PutAnonResponse;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.log.messages.LogAnonMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupState;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ThroughputPool;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * Put data of a chunk to the key-value memory (local only and optimized)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class PutAnon extends AbstractOperation implements MessageReceiver {
    private static final ThroughputPool SOP_PUT =
            new ThroughputPool(PutAnon.class, "Put", Value.Base.B_10);
    private static final ThroughputPool SOP_INCOMING =
            new ThroughputPool(PutAnon.class, "PutIncoming", Value.Base.B_10);

    static {
        StatisticsManager.get().registerOperation(PutAnon.class, SOP_PUT);
        StatisticsManager.get().registerOperation(PutAnon.class, SOP_INCOMING);
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
    public PutAnon(final Class<? extends AbstractDXRAMService> p_parentService,
            final AbstractBootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_ANON_REQUEST,
                PutAnonRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_ANON_RESPONSE,
                PutAnonResponse.class);

        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_ANON_REQUEST, this);
    }

    /**
     * Get the data of an existing chunk
     *
     * @param p_chunks
     *         Array with anonymous chunks to put
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int put(final ChunkAnon... p_chunks) {
        return put(p_chunks, 0, p_chunks.length, ChunkLockOperation.ACQUIRE_OP_RELEASE, -1);
    }

    /**
     * Get the data of an existing chunk
     *
     * @param p_chunks
     *         Array with anonymous chunks to put
     * @param p_lockOperation
     *         Lock operation to execute for each put operation
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation in ms (-1 for unlimited. Be careful with remote chunks here! This might lead
     *         to network timeouts instead)
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int put(final ChunkLockOperation p_lockOperation, final int p_lockOperationTimeoutMs,
            final ChunkAnon... p_chunks) {
        return put(p_chunks, 0, p_chunks.length, p_lockOperation, p_lockOperationTimeoutMs);
    }

    /**
     * Get the data of an existing chunk
     *
     * @param p_chunks
     *         Array with anonymous chunks to put
     * @param p_offset
     *         Offset in array where to start put operations
     * @param p_count
     *         Number of chunks to put (might be less array size/number of chunks provided)
     * @param p_lockOperation
     *         Lock operation to execute for each put operation
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation in ms (-1 for unlimited. Be careful with remote chunks here! This might lead
     *         to network timeouts instead)
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int put(final ChunkAnon[] p_chunks, final int p_offset, final int p_count,
            final ChunkLockOperation p_lockOperation, final int p_lockOperationTimeoutMs) {
        int chunksPut = 0;

        m_logger.trace("put[offset %d, count %d, lock op %s, lock timeout %d: %s]", p_offset, p_count,
                p_lockOperation, p_lockOperationTimeoutMs, AbstractChunk.toChunkIDListString(p_chunks));

        SOP_PUT.start(p_count);

        Map<Short, ArrayList<ChunkAnon>> remoteChunksByPeers = new TreeMap<>();
        Map<BackupRange, ArrayList<ChunkAnon>> remoteChunksByBackupRange = new TreeMap<>();

        // sort by local/remote chunks
        for (int i = 0; i < p_count; i++) {
            // filter null values
            if (p_chunks[i + p_offset] == null) {
                continue;
            }

            // try to put every chunk locally, returns false if it does not exist
            // and saves us an additional check
            if (m_chunk.getMemory().put().put(p_chunks[i + p_offset], p_lockOperation, p_lockOperationTimeoutMs)) {
                chunksPut++;

                if (m_backup.isActive()) {
                    // sort by backup peers
                    BackupRange backupRange = m_backup.getBackupRange(p_chunks[i + p_offset].getID());
                    ArrayList<ChunkAnon> remoteChunksOfBackupRange =
                            remoteChunksByBackupRange.computeIfAbsent(backupRange, a -> new ArrayList<>());
                    remoteChunksOfBackupRange.add(p_chunks[i + p_offset]);
                }
            } else {
                // remote or migrated, figure out location and sort by peers
                LookupRange location = m_lookup.getLookupRange(p_chunks[i + p_offset].getID());

                while (location.getState() == LookupState.DATA_TEMPORARY_UNAVAILABLE) {
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException ignore) {
                    }

                    location = m_lookup.getLookupRange(p_chunks[i + p_offset].getID());
                }

                if (location.getState() == LookupState.OK) {
                    // currently undefined because we still have to get it from remote
                    p_chunks[i + p_offset].setState(ChunkState.UNDEFINED);
                    short peer = location.getPrimaryPeer();

                    ArrayList<ChunkAnon> remoteChunksOfPeer =
                            remoteChunksByPeers.computeIfAbsent(peer, a -> new ArrayList<>());
                    remoteChunksOfPeer.add(p_chunks[i + p_offset]);
                } else if (location.getState() == LookupState.DOES_NOT_EXIST) {
                    p_chunks[i + p_offset].setState(ChunkState.DOES_NOT_EXIST);
                } else if (location.getState() == LookupState.DATA_LOST) {
                    p_chunks[i + p_offset].setState(ChunkState.DATA_LOST);
                }
            }
        }

        // go for remote chunks
        for (Map.Entry<Short, ArrayList<ChunkAnon>> entry : remoteChunksByPeers.entrySet()) {
            short peer = entry.getKey();

            if (peer == m_boot.getNodeId()) {
                // local put, migrated data to current node
                for (final ChunkAnon chunk : entry.getValue()) {
                    if (m_chunk.getMemory().put().put(chunk, p_lockOperation, p_lockOperationTimeoutMs)) {
                        chunksPut++;
                    }
                }
            } else {
                // Remote put
                ArrayList<ChunkAnon> chunksToPut = entry.getValue();
                PutAnonRequest request = new PutAnonRequest(peer, p_lockOperation, p_lockOperationTimeoutMs,
                        chunksToPut.toArray(new ChunkAnon[0]));

                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    if (m_backup.isActive()) {
                        for (ChunkAnon chunk : chunksToPut) {
                            chunk.setState(ChunkState.DATA_TEMPORARY_UNAVAILABLE);
                        }
                    } else {
                        for (ChunkAnon chunk : chunksToPut) {
                            chunk.setState(ChunkState.DATA_LOST);
                        }
                    }

                    for (ChunkAnon chunk : chunksToPut) {
                        m_lookup.invalidate(chunk.getID());
                    }

                    continue;
                }

                PutAnonResponse response = request.getResponse(PutAnonResponse.class);

                byte[] statusCodes = response.getStatusCodes();

                // try short cut, i.e. all puts successful
                if (statusCodes.length == 1 && statusCodes[0] == ChunkState.OK.ordinal()) {
                    chunksPut += chunksToPut.size();

                    for (ChunkAnon chunk : chunksToPut) {
                        chunk.setState(ChunkState.OK);
                    }
                } else {
                    for (int i = 0; i < statusCodes.length; i++) {
                        chunksToPut.get(i).setState(ChunkState.values()[statusCodes[i]]);

                        if (statusCodes[i] == ChunkState.OK.ordinal()) {
                            chunksPut++;
                        }
                    }
                }
            }
        }

        // Send backups
        if (m_backup.isActive()) {
            BackupRange backupRange;
            BackupPeer[] backupPeers;
            ChunkAnon[] chunks;

            for (Map.Entry<BackupRange, ArrayList<ChunkAnon>> entry : remoteChunksByBackupRange.entrySet()) {
                backupRange = entry.getKey();
                chunks = entry.getValue().toArray(new ChunkAnon[0]);

                backupPeers = backupRange.getBackupPeers();

                for (BackupPeer backupPeer : backupPeers) {
                    if (backupPeer != null) {
                        m_logger.trace("Logging %d chunks to 0x%X", chunks.length, backupPeer.getNodeID());

                        try {
                            m_network.sendMessage(
                                    new LogAnonMessage(backupPeer.getNodeID(), backupRange.getRangeID(), chunks));
                        } catch (final NetworkException ignore) {

                        }
                    }
                }
            }
        }

        SOP_PUT.stop();

        return chunksPut;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE &&
                p_message.getSubtype() == ChunkMessages.SUBTYPE_PUT_ANON_REQUEST) {
            PutAnonRequest request = (PutAnonRequest) p_message;

            byte[] chunkStates = new byte[request.getChunkIDs().length];
            boolean allSuccessful = true;

            SOP_INCOMING.start(chunkStates.length);

            Map<BackupRange, ArrayList<ChunkAnon>> remoteChunksByBackupRange = new TreeMap<>();

            for (int i = 0; i < chunkStates.length; i++) {
                ChunkState state = m_chunk.getMemory().put().put(request.getChunkIDs()[i], request.getChunkData()[i],
                        request.getLockOperation(), request.getLockOperationTimeoutMs());

                if (state != ChunkState.OK) {
                    allSuccessful = false;
                }

                chunkStates[i] = (byte) state.ordinal();

                if (m_backup.isActive()) {
                    // sort by backup peers
                    BackupRange backupRange = m_backup.getBackupRange(request.getChunkIDs()[i]);
                    ArrayList<ChunkAnon> remoteChunksOfBackupRange =
                            remoteChunksByBackupRange.computeIfAbsent(backupRange, k -> new ArrayList<>());
                    remoteChunksOfBackupRange.add(new ChunkAnon(request.getChunkIDs()[i], request.getChunkData()[i]));
                }
            }

            PutAnonResponse response;

            // cut message length if all were successful
            if (allSuccessful) {
                response = new PutAnonResponse(request, (byte) ChunkState.OK.ordinal());
            } else {
                // we got errors, default message
                response = new PutAnonResponse(request, chunkStates);
            }

            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {
                m_logger.error("Sending chunk put respond to request %s failed: %s", request, e);
            }

            // Send backups
            if (m_backup.isActive()) {
                BackupRange backupRange;
                BackupPeer[] backupPeers;
                ChunkAnon[] chunks;

                for (Map.Entry<BackupRange, ArrayList<ChunkAnon>> entry : remoteChunksByBackupRange.entrySet()) {
                    backupRange = entry.getKey();
                    chunks = entry.getValue().toArray(new ChunkAnon[0]);

                    backupPeers = backupRange.getBackupPeers();

                    for (BackupPeer backupPeer : backupPeers) {
                        if (backupPeer != null) {
                            m_logger.trace("Logging %d chunks to 0x%X", chunks.length, backupPeer.getNodeID());

                            try {
                                m_network.sendMessage(
                                        new LogAnonMessage(backupPeer.getNodeID(), backupRange.getRangeID(), chunks));
                            } catch (final NetworkException ignore) {

                            }
                        }
                    }
                }
            }

            SOP_INCOMING.stop();
        }
    }
}

package de.hhu.bsinfo.dxram.chunk.operation;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.data.ChunkState;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.data.ChunkAnon;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.GetAnonRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetAnonResponse;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupState;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ThroughputPool;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * Get the stored data of an existing chunk with unknown size (locally and remotely)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class GetAnon extends AbstractOperation implements MessageReceiver {
    private static final ThroughputPool SOP_GET =
            new ThroughputPool(GetAnon.class, "Get", Value.Base.B_10);
    private static final ThroughputPool SOP_INCOMING_GET =
            new ThroughputPool(GetAnon.class, "Incoming", Value.Base.B_10);

    static {
        StatisticsManager.get().registerOperation(ChunkAnon.class, SOP_GET);
        StatisticsManager.get().registerOperation(ChunkAnon.class, SOP_INCOMING_GET);
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
    public GetAnon(final Class<? extends AbstractDXRAMService> p_parentService,
            final AbstractBootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_ANON_REQUEST,
                GetAnonRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_ANON_RESPONSE,
                GetAnonResponse.class);

        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_ANON_REQUEST, this);
    }

    /**
     * Get the data of an existing chunk
     *
     * @param p_retChunks
     *         Pre-allocated array to assign retrieved anonymous chunk instances to
     * @param p_cids
     *         Array of CIDs of chunks to get
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int get(final ChunkAnon[] p_retChunks, final long... p_cids) {
        return get(p_retChunks, p_cids, 0, p_cids.length, ChunkLockOperation.NONE, -1);
    }

    /**
     * Get the data of an existing chunk
     *
     * @param p_retChunks
     *         Pre-allocated array to assign retrieved anonymous chunk instances to
     * @param p_cids
     *         Array of CIDs of chunks to get
     * @param p_lockOperation
     *         Lock operation to execute for each get operation
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation in ms (-1 for unlimited. Be careful with remote chunks here! This might lead
     *         to network timeouts instead)
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int get(final ChunkLockOperation p_lockOperation, final int p_lockOperationTimeoutMs,
            final ChunkAnon[] p_retChunks, final long... p_cids) {
        return get(p_retChunks, p_cids, 0, p_cids.length, p_lockOperation, p_lockOperationTimeoutMs);
    }

    /**
     * Get the data of an existing chunk
     *
     * @param p_retChunks
     *         Pre-allocated array to assign retrieved anonymous chunk instances to
     * @param p_cids
     *         Array of CIDs of chunks to get
     * @param p_offset
     *         Offset in array where to start get operations
     * @param p_count
     *         Number of chunks to get (might be less array size/number of chunks provided)
     * @param p_lockOperation
     *         Lock operation to execute for each get operation
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation in ms (-1 for unlimited. Be careful with remote chunks here! This might lead
     *         to network timeouts instead)
     * @return Number of successful operations. If less than expected, check the chunk object states for errors
     */
    public int get(final ChunkAnon[] p_retChunks, final long[] p_cids, final int p_offset, final int p_count,
            final ChunkLockOperation p_lockOperation, final int p_lockOperationTimeoutMs) {
        int numChunks = 0;

        m_logger.trace("get[retChunks %d, offset %d, count %d, lock op %s, lock timeout %d: %s]", p_retChunks.length,
                p_offset, p_count, p_lockOperation, p_lockOperationTimeoutMs, ChunkID.chunkIDArrayToString(p_cids));

        SOP_GET.start(p_count);

        // sort by local and remote data first
        Map<Short, ArrayList<Integer>> remoteChunkIDsByPeers = new TreeMap<>();

        for (int i = 0; i < p_count; i++) {
            // try to get locally, will check first if it exists and
            // returns false if it doesn't exist
            ChunkByteArray data = m_chunk.getMemory().get().get(p_cids[p_offset + i], p_lockOperation,
                    p_lockOperationTimeoutMs);

            if (data.isStateOk()) {
                p_retChunks[i] = new ChunkAnon(p_cids[p_offset + i], data.getData());
                p_retChunks[i].setState(ChunkState.OK);
                numChunks++;
            } else {
                // remote or migrated, figure out location and sort by peers
                LookupRange lookupRange;

                lookupRange = m_lookup.getLookupRange(p_cids[p_offset + i]);

                while (lookupRange.getState() == LookupState.DATA_TEMPORARY_UNAVAILABLE) {
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException ignore) {
                    }

                    lookupRange = m_lookup.getLookupRange(p_cids[p_offset + i]);
                }

                if (lookupRange.getState() == LookupState.OK) {
                    short peer = lookupRange.getPrimaryPeer();

                    ArrayList<Integer> remoteChunkIDsOfPeer =
                            remoteChunkIDsByPeers.computeIfAbsent(peer, a -> new ArrayList<>());
                    // Add the index in ChunkID array not the ChunkID itself
                    remoteChunkIDsOfPeer.add(p_offset + i);
                }
            }
        }

        // go for remote ones by each peer
        for (final Map.Entry<Short, ArrayList<Integer>> peerWithChunks : remoteChunkIDsByPeers.entrySet()) {
            short peer = peerWithChunks.getKey();
            ArrayList<Integer> remoteChunkIDIndexes = peerWithChunks.getValue();

            if (peer == m_boot.getNodeId()) {
                // local get, migrated data to current node
                for (final int index : remoteChunkIDIndexes) {
                    ChunkByteArray data = m_chunk.getMemory().get().get(p_cids[index], ChunkLockOperation.NONE, -1);

                    p_retChunks[numChunks] = new ChunkAnon(p_cids[index], data.getData());
                    p_retChunks[numChunks].setState(ChunkState.OK);
                    numChunks++;
                }
            } else {
                // Remote get from specified peer
                int i = 0;
                ChunkAnon[] chunks = new ChunkAnon[remoteChunkIDIndexes.size()];

                for (int index : remoteChunkIDIndexes) {
                    p_retChunks[index] = new ChunkAnon(p_cids[index]);
                    chunks[i++] = p_retChunks[index];
                }

                GetAnonRequest request = new GetAnonRequest(peer, p_lockOperation, p_lockOperationTimeoutMs, chunks);

                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    if (m_backup.isActive()) {
                        for (ChunkAnon chunk : chunks) {
                            chunk.setState(ChunkState.DATA_TEMPORARY_UNAVAILABLE);
                        }
                    } else {
                        for (ChunkAnon chunk : chunks) {
                            chunk.setState(ChunkState.DATA_LOST);
                        }
                    }

                    m_logger.error("Sending chunk get request to peer 0x%X failed: %s", peer, e);

                    continue;
                }

                // check chunk status written to chunk
                for (ChunkAnon chunk : chunks) {
                    if (chunk.isStateOk()) {
                        numChunks++;
                    }
                }
            }
        }

        SOP_GET.stop();

        return numChunks;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE &&
                p_message.getSubtype() == ChunkMessages.SUBTYPE_GET_ANON_REQUEST) {
            GetAnonRequest request = (GetAnonRequest) p_message;

            long[] chunkIDs = request.getChunkIDs();
            ChunkByteArray[] chunks = new ChunkByteArray[chunkIDs.length];

            SOP_INCOMING_GET.start(request.getChunkIDs().length);

            for (int i = 0; i < chunks.length; i++) {
                chunks[i] = m_chunk.getMemory().get().get(chunkIDs[i], ChunkLockOperation.NONE, -1);
            }

            GetAnonResponse response = new GetAnonResponse(request, chunks);

            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {
                m_logger.error("Sending GetAnonResponse chunks failed: %s", e);
            }

            SOP_INCOMING_GET.stop();
        }
    }
}

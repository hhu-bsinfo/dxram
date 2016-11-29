/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.chunk;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.CreateRequest;
import de.hhu.bsinfo.dxram.chunk.messages.CreateResponse;
import de.hhu.bsinfo.dxram.chunk.messages.GetLocalChunkIDRangesRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetLocalChunkIDRangesResponse;
import de.hhu.bsinfo.dxram.chunk.messages.GetMigratedChunkIDRangesRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetMigratedChunkIDRangesResponse;
import de.hhu.bsinfo.dxram.chunk.messages.GetRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetResponse;
import de.hhu.bsinfo.dxram.chunk.messages.PutRequest;
import de.hhu.bsinfo.dxram.chunk.messages.PutResponse;
import de.hhu.bsinfo.dxram.chunk.messages.RemoveRequest;
import de.hhu.bsinfo.dxram.chunk.messages.RemoveResponse;
import de.hhu.bsinfo.dxram.chunk.messages.StatusRequest;
import de.hhu.bsinfo.dxram.chunk.messages.StatusResponse;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.lock.AbstractLockComponent;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.log.messages.RemoveMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent.MemoryErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * This service provides access to the backend storage system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class ChunkService extends AbstractDXRAMService implements MessageReceiver {

    private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkService.class.getSimpleName());

    // statistics recording
    private static final StatisticsOperation SOP_CREATE = StatisticsRecorderManager.getOperation(ChunkService.class, "Create");
    private static final StatisticsOperation SOP_REMOTE_CREATE = StatisticsRecorderManager.getOperation(ChunkService.class, "RemoteCreate");
    private static final StatisticsOperation SOP_GET = StatisticsRecorderManager.getOperation(ChunkService.class, "Get");
    private static final StatisticsOperation SOP_PUT = StatisticsRecorderManager.getOperation(ChunkService.class, "Put");
    private static final StatisticsOperation SOP_REMOVE = StatisticsRecorderManager.getOperation(ChunkService.class, "Remove");
    private static final StatisticsOperation SOP_INCOMING_CREATE = StatisticsRecorderManager.getOperation(ChunkService.class, "IncomingCreate");
    private static final StatisticsOperation SOP_INCOMING_GET = StatisticsRecorderManager.getOperation(ChunkService.class, "IncomingGet");
    private static final StatisticsOperation SOP_INCOMING_PUT = StatisticsRecorderManager.getOperation(ChunkService.class, "IncomingPut");
    private static final StatisticsOperation SOP_INCOMING_REMOVE = StatisticsRecorderManager.getOperation(ChunkService.class, "IncomingRemove");

    // dependent components
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;
    private LookupComponent m_lookup;
    private AbstractLockComponent m_lock;

    /**
     * Constructor
     */
    public ChunkService() {
        super("chunk");
    }

    /**
     * Get the status of the chunk service.
     *
     * @return Status object with current status of the service.
     */
    public Status getStatus() {
        Status status = new Status();

        MemoryManagerComponent.Status memManStatus = m_memoryManager.getStatus();

        if (memManStatus != null) {
            status.m_freeMemoryBytes = memManStatus.getFreeMemory();
            status.m_totalMemoryBytes = memManStatus.getTotalMemory();
            status.m_totalPayloadMemoryBytes = memManStatus.getTotalPayloadMemory();
            status.m_numberOfActiveMemoryBlocks = memManStatus.getNumberOfActiveMemoryBlocks();
            status.m_numberOfActiveChunks = memManStatus.getNumberOfActiveChunks();
            status.m_totalChunkPayloadMemory = memManStatus.getTotalChunkMemory();
            status.m_cidTableCount = memManStatus.getCIDTableCount();
            status.m_totalMemoryCIDTables = memManStatus.getTotalMemoryCIDTables();
        }

        return status;
    }

    /**
     * Get the total amount of memory.
     *
     * @return Total amount of memory in bytes.
     */
    public long getTotalMemory() {
        MemoryManagerComponent.Status memManStatus;

        memManStatus = m_memoryManager.getStatus();

        if (memManStatus != null) {
            return memManStatus.getTotalMemory();
        } else {
            return -1;
        }
    }

    /**
     * Get the amounf of free memory.
     *
     * @return Amount of free memory in bytes.
     */
    public long getFreeMemory() {
        MemoryManagerComponent.Status memManStatus;

        memManStatus = m_memoryManager.getStatus();

        if (memManStatus != null) {
            return memManStatus.getFreeMemory();
        } else {
            return -1;
        }
    }

    /**
     * Get all chunk ID ranges of all migrated chunks stored on this node.
     *
     * @return List of migrated chunk ID ranges with blocks of start ID and end ID.
     */
    public ArrayList<Long> getAllMigratedChunkIDRanges() {
        ArrayList<Long> list;

        m_memoryManager.lockAccess();
        list = m_memoryManager.getCIDOfAllMigratedChunks();
        m_memoryManager.unlockAccess();

        return list;
    }

    /**
     * Get all chunk ID ranges of all locally stored chunks.
     *
     * @return List of local chunk ID ranges with blocks of start ID and end ID.
     */
    private ArrayList<Long> getAllLocalChunkIDRanges() {
        ArrayList<Long> list;

        m_memoryManager.lockAccess();
        list = m_memoryManager.getCIDRangesOfAllLocalChunks();
        m_memoryManager.unlockAccess();

        return list;
    }

    /**
     * Get the status of a remote node specified by a node id.
     *
     * @param p_nodeID
     *     Node id to get the status from.
     * @return Status object with status information of the remote node or null if getting status failed.
     */
    public Status getStatus(final short p_nodeID) {
        Status status = null;

        if (p_nodeID == NodeID.INVALID_ID) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid node id on get status");
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // own status?
        if (p_nodeID == m_boot.getNodeID()) {
            status = getStatus();
        } else {
            // grab from remote
            StatusRequest request = new StatusRequest(p_nodeID);
            try {
                m_network.sendSync(request);

                StatusResponse response = request.getResponse(StatusResponse.class);
                status = response.getStatus();
            } catch (final NetworkException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Sending get status request to peer %s failed: %s", NodeID.toHexString(p_nodeID), e);
                // #endif /* LOGGER >= ERROR */
            }
        }

        return status;
    }

    /**
     * Create a new chunk.
     *
     * @param p_size
     *     Size of the new chunk.
     * @param p_count
     *     Number of chunks to create with the specified size.
     * @return ChunkIDs/Handles identifying the created chunks.
     */
    public long[] create(final int p_size, final int p_count) {
        long[] chunkIDs = null;

        assert p_size > 0 && p_count > 0;

        // #if LOGGER == TRACE
        LOGGER.trace("create[size %d, count %d]", p_size, p_count);
        // #endif /* LOGGER == TRACE */

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s must not create chunks", role);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // #ifdef STATISTICS
        SOP_CREATE.enter(p_count);
        // #endif /* STATISTICS */

        if (p_count == 1) {
            m_memoryManager.lockManage();
            long chunkId = m_memoryManager.create(p_size);
            m_memoryManager.unlockManage();

            if (chunkId != -1) {
                chunkIDs = new long[] {chunkId};
            }
        } else {
            m_memoryManager.lockManage();
            chunkIDs = m_memoryManager.createMulti(p_size, p_count);
            m_memoryManager.unlockManage();
        }

        if (chunkIDs == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Multi create for size %d, count %d failed", p_size, p_count);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // keep loop tight and execute everything
        // that we don't have to lock outside of this section
        for (int i = 0; i < p_count; i++) {
            // tell the superpeer overlay about our newly created chunks, otherwise they can not be found
            // by other peers (network traffic for backup range initialization only (e.g. every 256 MB))
            m_backup.initBackupRange(chunkIDs[i], p_size);
        }

        // #ifdef STATISTICS
        SOP_CREATE.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("create[size %d, count %d] -> %s, ...", p_size, p_count, ChunkID.toHexString(chunkIDs[0]));
        // #endif /* LOGGER == TRACE */

        return chunkIDs;
    }

    /**
     * Create new chunks according to the data structures provided.
     * Important: This does NOT put/write the contents of the data structure provided.
     * It creates chunks with the sizes of the data structures and sets the IDs.
     *
     * @param p_dataStructures
     *     Data structures to create chunks for.
     * @return Number of successfully created chunks.
     */
    public int create(final DataStructure... p_dataStructures) {
        int count = 0;

        if (p_dataStructures.length == 0) {
            return count;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("create[numDataStructures %d...]", p_dataStructures.length);
        // #endif /* LOGGER == TRACE */

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s must not create chunks", role);
            // #endif /* LOGGER >= ERROR */
            return count;
        }

        // #ifdef STATISTICS
        SOP_CREATE.enter(p_dataStructures.length);
        // #endif /* STATISTICS */

        if (p_dataStructures.length == 1) {
            m_memoryManager.lockManage();
            long chunkID = m_memoryManager.create(p_dataStructures[0].sizeofObject());
            m_memoryManager.unlockManage();
            if (chunkID != ChunkID.INVALID_ID) {
                count++;
                p_dataStructures[0].setID(chunkID);
                // tell the superpeer overlay about our newly created chunks, otherwise they can not be found
                // by other peers (network traffic for backup range initialization only (e.g. every 256 MB))
                m_backup.initBackupRange(p_dataStructures[0].getID(), p_dataStructures[0].sizeofObject());
            } else {
                p_dataStructures[0].setID(ChunkID.INVALID_ID);
            }
        } else {
            m_memoryManager.lockManage();
            long[] chunkIDs = m_memoryManager.createMulti(p_dataStructures);
            m_memoryManager.unlockManage();

            if (chunkIDs == null) {
                // #if LOGGER >= ERROR
                LOGGER.error("Multi create chunks failed");
                // #endif /* LOGGER >= ERROR */
                return count;
            }

            for (int i = 0; i < chunkIDs.length; i++) {
                p_dataStructures[i].setID(chunkIDs[i]);
                // tell the superpeer overlay about our newly created chunks, otherwise they can not be found
                // by other peers (network traffic for backup range initialization only (e.g. every 256 MB))
                m_backup.initBackupRange(p_dataStructures[i].getID(), p_dataStructures[i].sizeofObject());
            }

            count += chunkIDs.length;
        }

        // #ifdef STATISTICS
        SOP_CREATE.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("create[numDataStructures(%d)] -> %d", p_dataStructures.length, count);
        // #endif /* LOGGER == TRACE */

        return count;
    }

    /**
     * Create chunks with different sizes.
     *
     * @param p_sizes
     *     List of sizes to create chunks for.
     * @return ChunkIDs/Handles identifying the created chunks.
     */
    public long[] createSizes(final int... p_sizes) {
        long[] chunkIDs;

        if (p_sizes.length == 0) {
            return new long[0];
        }

        // #if LOGGER == TRACE
        LOGGER.trace("create[sizes(%d) %d, ...]", p_sizes.length, p_sizes[0]);
        // #endif /* LOGGER == TRACE */

        NodeRole role = m_boot.getNodeRole();
        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s must not create chunks", role);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // #ifdef STATISTICS
        SOP_CREATE.enter(p_sizes.length);
        // #endif /* STATISTICS */

        chunkIDs = new long[p_sizes.length];

        if (p_sizes.length == 1) {
            m_memoryManager.lockManage();
            chunkIDs[0] = m_memoryManager.create(p_sizes[0]);
            m_memoryManager.unlockManage();

            // tell the superpeer overlay about our newly created chunks, otherwise they can not be found
            // by other peers (network traffic for backup range initialization only (e.g. every 256 MB))
            m_backup.initBackupRange(chunkIDs[0], p_sizes[0]);
        } else {
            m_memoryManager.lockManage();
            chunkIDs = m_memoryManager.createMultiSizes(p_sizes);
            m_memoryManager.unlockManage();

            if (chunkIDs == null) {
                // #if LOGGER >= ERROR
                LOGGER.error("Multi create chunks failed");
                // #endif /* LOGGER >= ERROR */
                return null;
            }

            // keep loop tight and execute everything
            // that we don't have to lock outside of this section
            for (int i = 0; i < p_sizes.length; i++) {
                // tell the superpeer overlay about our newly created chunks, otherwise they can not be found
                // by other peers (network traffic for backup range initialization only (e.g. every 256 MB))
                m_backup.initBackupRange(chunkIDs[i], p_sizes[i]);
            }
        }

        // #ifdef STATISTICS
        SOP_CREATE.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("create[sizes(%d) %d, ...] -> %s, ...", p_sizes.length, p_sizes[0], ChunkID.toHexString(chunkIDs[0]));
        // #endif /* LOGGER == TRACE */

        return chunkIDs;
    }

    /**
     * Create chunks on another node.
     *
     * @param p_peer
     *     NodeID of the peer to create the chunks on.
     * @param p_dataStructures
     *     Data structures to create chunks for.
     * @return Number of successfully created chunks.
     */
    public int createRemote(final short p_peer, final DataStructure... p_dataStructures) {
        int[] sizes = new int[p_dataStructures.length];

        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = p_dataStructures[i].sizeofObject();
        }

        int count = 0;
        long[] ids = createRemote(p_peer, sizes);
        if (ids != null) {
            for (int i = 0; i < ids.length; i++) {
                p_dataStructures[i].setID(ids[i]);
            }
        }

        return count;
    }

    /**
     * Create chunks on another node.
     *
     * @param p_peer
     *     NodeID of the peer to create the chunks on.
     * @param p_sizes
     *     Sizes to create chunks of.
     * @return ChunkIDs/Handles identifying the created chunks.
     */
    public long[] createRemote(final short p_peer, final int... p_sizes) {
        long[] chunkIDs = null;

        if (p_sizes.length == 0) {
            return new long[0];
        }

        // #if LOGGER == TRACE
        LOGGER.trace("createRemote[peer %s, sizes(%d) %d, ...]", NodeID.toHexString(p_peer), p_sizes.length, p_sizes[0]);
        // #endif /* LOGGER == TRACE */

        // check if remote node is a peer
        NodeRole role = m_boot.getNodeRole(p_peer);
        if (role == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Remote node %s does not exist for remote create", NodeID.toHexString(p_peer));
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("%s %s is not allowed to create chunks", role, NodeID.toHexString(p_peer));
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // #ifdef STATISTICS
        SOP_REMOTE_CREATE.enter(p_sizes.length);
        // #endif /* STATISTICS */

        CreateRequest request = new CreateRequest(p_peer, p_sizes);
        try {
            m_network.sendSync(request);

            CreateResponse response = request.getResponse(CreateResponse.class);
            chunkIDs = response.getChunkIDs();
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending chunk create request to peer %s failed: %s", NodeID.toHexString(p_peer), e);
            // #endif /* LOGGER >= ERROR */
        }

        // #ifdef STATISTICS
        SOP_REMOTE_CREATE.leave();
        // #endif /* STATISTICS */

        if (chunkIDs != null) {
            // #if LOGGER == TRACE
            LOGGER.trace("createRemote[peer %s, sizes(%d) %d, ...] -> %s, ...", NodeID.toHexString(p_peer), p_sizes.length, p_sizes[0],
                ChunkID.toHexString(chunkIDs[0]));
            // #endif /* LOGGER == TRACE */
        } else {
            // #if LOGGER == TRACE
            LOGGER.trace("createRemote[peer %s, sizes(%d) %d, ...] -> -1", NodeID.toHexString(p_peer), p_sizes.length, p_sizes[0]);
            // #endif /* LOGGER == TRACE */
        }

        return chunkIDs;
    }

    /**
     * Remove chunks/data structures from the storage.
     *
     * @param p_dataStructures
     *     Data structures to remove from the storage.
     * @return Number of successfully removed data structures.
     */
    public int remove(final DataStructure... p_dataStructures) {
        long[] chunkIDs = new long[p_dataStructures.length];
        for (int i = 0; i < chunkIDs.length; i++) {
            chunkIDs[i] = p_dataStructures[i].getID();
        }

        return remove(chunkIDs);
    }

    /**
     * Remove chunks/data structures from the storage (by handle/ID).
     *
     * @param p_chunkIDs
     *     ChunkIDs/Handles of the data structures to remove. Invalid values are ignored.
     * @return Number of successfully removed data structures.
     */
    public int remove(final long... p_chunkIDs) {
        int chunksRemoved = 0;

        if (p_chunkIDs.length == 0) {
            return chunksRemoved;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("remove[dataStructures(%d) %s, ...]", p_chunkIDs.length, ChunkID.toHexString(p_chunkIDs[0]));
        // #endif /* LOGGER == TRACE */

        NodeRole role = m_boot.getNodeRole();
        if (role == NodeRole.SUPERPEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s must not remove chunks", role);
            // #endif /* LOGGER >= ERROR */
            return chunksRemoved;
        }

        // #ifdef STATISTICS
        SOP_REMOVE.enter(p_chunkIDs.length);
        // #endif /* STATISTICS */

        // sort by local and remote data first
        Map<Short, ArrayList<Long>> remoteChunksByPeers = new TreeMap<>();
        Map<Long, ArrayList<Long>> remoteChunksByBackupPeers = new TreeMap<>();
        ArrayList<Long> localChunks = new ArrayList<>();

        m_memoryManager.lockAccess();
        for (int i = 0; i < p_chunkIDs.length; i++) {
            // invalid values allowed -> filter
            if (p_chunkIDs[i] == ChunkID.INVALID_ID) {
                continue;
            }

            if (m_memoryManager.exists(p_chunkIDs[i])) {
                // local
                localChunks.add(p_chunkIDs[i]);

                if (m_backup.isActive()) {
                    // sort by backup peers
                    long backupPeersAsLong = m_backup.getBackupPeersForLocalChunks(p_chunkIDs[i]);
                    ArrayList<Long> remoteChunkIDsOfBackupPeers = remoteChunksByBackupPeers.get(backupPeersAsLong);
                    if (remoteChunkIDsOfBackupPeers == null) {
                        remoteChunkIDsOfBackupPeers = new ArrayList<>();
                        remoteChunksByBackupPeers.put(backupPeersAsLong, remoteChunkIDsOfBackupPeers);
                    }
                    remoteChunkIDsOfBackupPeers.add(p_chunkIDs[i]);
                }
            } else {
                // remote or migrated, figure out location and sort by peers
                LookupRange lookupRange;

                lookupRange = m_lookup.getLookupRange(p_chunkIDs[i]);
                if (lookupRange != null) {
                    short peer = lookupRange.getPrimaryPeer();

                    ArrayList<Long> remoteChunksOfPeer = remoteChunksByPeers.get(peer);
                    if (remoteChunksOfPeer == null) {
                        remoteChunksOfPeer = new ArrayList<>();
                        remoteChunksByPeers.put(peer, remoteChunksOfPeer);
                    }
                    remoteChunksOfPeer.add(p_chunkIDs[i]);
                }
            }
        }
        m_memoryManager.unlockAccess();

        // remove local chunks from superpeer overlay first, so cannot be found before being deleted
        for (final Long chunkID : localChunks) {
            m_lookup.removeChunkIDs(new long[] {chunkID});
        }

        // remove local chunkIDs
        m_memoryManager.lockManage();
        for (final Long chunkID : localChunks) {
            MemoryErrorCodes err = m_memoryManager.remove(chunkID, false);
            if (err == MemoryErrorCodes.SUCCESS) {
                chunksRemoved++;
            } else {
                // #if LOGGER >= ERROR
                LOGGER.error("Removing chunk ID %s failed: %s", ChunkID.toHexString(chunkID), err);
                // #endif /* LOGGER >= ERROR */
            }
        }
        m_memoryManager.unlockManage();

        // go for remote ones by each peer
        for (final Entry<Short, ArrayList<Long>> peerWithChunks : remoteChunksByPeers.entrySet()) {
            short peer = peerWithChunks.getKey();
            ArrayList<Long> remoteChunks = peerWithChunks.getValue();

            if (peer == m_boot.getNodeID()) {
                // local remove, migrated data to current node
                m_memoryManager.lockManage();
                for (final Long chunkID : remoteChunks) {
                    MemoryErrorCodes err = m_memoryManager.remove(chunkID, false);
                    if (err == MemoryErrorCodes.SUCCESS) {
                        chunksRemoved++;
                    } else {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Removing chunk ID %s failed: %s", ChunkID.toHexString(chunkID), err);
                        // #endif /* LOGGER >= ERROR */
                    }
                }
                m_memoryManager.unlockManage();
            } else {
                // Remote remove from specified peer
                RemoveRequest request = new RemoveRequest(peer, remoteChunks.toArray(new Long[remoteChunks.size()]));
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Sending chunk remove request to peer 0x%X failed: %s", peer, e);
                    // #endif /* LOGGER >= ERROR */
                    continue;
                }

                RemoveResponse response = request.getResponse(RemoveResponse.class);
                if (response != null) {
                    byte[] statusCodes = response.getStatusCodes();
                    // short cut if everything is ok
                    if (statusCodes[0] == 2) {
                        chunksRemoved += remoteChunks.size();
                    } else {
                        for (int i = 0; i < statusCodes.length; i++) {
                            if (statusCodes[i] < 0) {
                                // #if LOGGER >= ERROR
                                LOGGER.error("Remote removing chunk 0x%X failed: %d", remoteChunks.get(i), statusCodes[i]);
                                // #endif /* LOGGER >= ERROR */
                            } else {
                                chunksRemoved++;
                            }
                        }
                    }
                }
            }
        }

        // Inform backups
        if (m_backup.isActive()) {
            long backupPeersAsLong;
            short[] backupPeers;
            Long[] ids;
            for (Entry<Long, ArrayList<Long>> entry : remoteChunksByBackupPeers.entrySet()) {
                backupPeersAsLong = entry.getKey();
                ids = entry.getValue().toArray(new Long[entry.getValue().size()]);

                backupPeers = BackupRange.convert(backupPeersAsLong);
                for (int i = 0; i < backupPeers.length; i++) {
                    if (backupPeers[i] != m_boot.getNodeID() && backupPeers[i] != -1) {
                        try {
                            m_network.sendMessage(new RemoveMessage(backupPeers[i], ids));
                        } catch (final NetworkException ignore) {

                        }
                    }
                }
            }
        }

        // #ifdef STATISTICS
        SOP_REMOVE.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("remove[dataStructures(%d) 0x%X, ...] -> %d", p_chunkIDs.length, p_chunkIDs[0], chunksRemoved);
        // #endif /* LOGGER == TRACE */

        return chunksRemoved;
    }

    /**
     * Put/Update the contents of the provided data structures in the backend storage.
     *
     * @param p_dataStructres
     *     Data structures to put/update. Null values are ignored.
     * @return Number of successfully updated data structures.
     */
    public int put(final DataStructure... p_dataStructres) {
        return put(ChunkLockOperation.NO_LOCK_OPERATION, p_dataStructres);
    }

    /**
     * Put/Update the contents of the provided data structures in the backend storage.
     *
     * @param p_chunkUnlockOperation
     *     Unlock operation to execute right after the put operation.
     * @param p_dataStructures
     *     Data structures to put/update. Null values or chunks with invalid IDs are ignored.
     * @return Number of successfully updated data structures.
     */
    public int put(final ChunkLockOperation p_chunkUnlockOperation, final DataStructure... p_dataStructures) {
        return put(p_chunkUnlockOperation, p_dataStructures, 0, p_dataStructures.length);
    }

    /**
     * Put/Update the contents of the provided data structures in the backend storage.
     *
     * @param p_chunkUnlockOperation
     *     Unlock operation to execute right after the put operation.
     * @param p_dataStructures
     *     Data structures to put/update. Null values or chunks with invalid IDs are ignored.
     * @param p_offset
     *     Start offset within the array.
     * @param p_count
     *     Number of items to put.
     * @return Number of successfully updated data structures.
     */
    public int put(final ChunkLockOperation p_chunkUnlockOperation, final DataStructure[] p_dataStructures, final int p_offset, final int p_count) {
        int chunksPut = 0;

        if (p_dataStructures.length == 0) {
            return chunksPut;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("put[unlockOp %s, dataStructures(%d) ...]", p_chunkUnlockOperation, p_dataStructures.length);
        // #endif /* LOGGER == TRACE */

        NodeRole role = m_boot.getNodeRole();
        if (role == NodeRole.SUPERPEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s must not put chunks", role);
            // #endif /* LOGGER >= ERROR */
            return chunksPut;
        }

        // #ifdef STATISTICS
        SOP_PUT.enter(p_count);
        // #endif /* STATISTICS */

        Map<Short, ArrayList<DataStructure>> remoteChunksByPeers = new TreeMap<>();
        Map<Long, ArrayList<DataStructure>> remoteChunksByBackupPeers = new TreeMap<>();

        // sort by local/remote chunks
        m_memoryManager.lockAccess();
        for (int i = 0; i < p_count; i++) {
            // filter null values
            if (p_dataStructures[i + p_offset] == null || p_dataStructures[i + p_offset].getID() == ChunkID.INVALID_ID) {
                continue;
            }

            // try to put every chunk locally, returns false if it does not exist
            // and saves us an additional check
            if (m_memoryManager.put(p_dataStructures[i + p_offset]) == MemoryErrorCodes.SUCCESS) {
                chunksPut++;

                // unlock chunk as well
                if (p_chunkUnlockOperation != ChunkLockOperation.NO_LOCK_OPERATION) {
                    boolean writeLock = false;
                    if (p_chunkUnlockOperation == ChunkLockOperation.WRITE_LOCK) {
                        writeLock = true;
                    }

                    m_lock.unlock(p_dataStructures[i + p_offset].getID(), m_boot.getNodeID(), writeLock);
                }

                if (m_backup.isActive()) {
                    // sort by backup peers
                    long backupPeersAsLong = m_backup.getBackupPeersForLocalChunks(p_dataStructures[i + p_offset].getID());
                    ArrayList<DataStructure> remoteChunksOfBackupPeers = remoteChunksByBackupPeers.get(backupPeersAsLong);
                    if (remoteChunksOfBackupPeers == null) {
                        remoteChunksOfBackupPeers = new ArrayList<>();
                        remoteChunksByBackupPeers.put(backupPeersAsLong, remoteChunksOfBackupPeers);
                    }
                    remoteChunksOfBackupPeers.add(p_dataStructures[i + p_offset]);
                }
            } else {
                // remote or migrated, figure out location and sort by peers
                LookupRange location = m_lookup.getLookupRange(p_dataStructures[i + p_offset].getID());
                if (location != null) {
                    short peer = location.getPrimaryPeer();

                    ArrayList<DataStructure> remoteChunksOfPeer = remoteChunksByPeers.get(peer);
                    if (remoteChunksOfPeer == null) {
                        remoteChunksOfPeer = new ArrayList<>();
                        remoteChunksByPeers.put(peer, remoteChunksOfPeer);
                    }
                    remoteChunksOfPeer.add(p_dataStructures[i + p_offset]);
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Location of Chunk is unknown, it cannot be put!");
                    // #endif /* LOGGER >= ERROR */
                }
            }
        }
        m_memoryManager.unlockAccess();

        // go for remote chunks
        for (Entry<Short, ArrayList<DataStructure>> entry : remoteChunksByPeers.entrySet()) {
            short peer = entry.getKey();

            if (peer == m_boot.getNodeID()) {
                // local put, migrated data to current node
                m_memoryManager.lockAccess();
                for (final DataStructure dataStructure : entry.getValue()) {
                    MemoryErrorCodes err = m_memoryManager.put(dataStructure);
                    if (err == MemoryErrorCodes.SUCCESS) {
                        chunksPut++;
                    } else {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Putting local chunk 0x%X failed: %s", dataStructure.getID(), err);
                        // #endif /* LOGGER >= ERROR */
                    }
                }
                m_memoryManager.unlockAccess();
            } else {
                // Remote put
                ArrayList<DataStructure> chunksToPut = entry.getValue();
                PutRequest request = new PutRequest(peer, p_chunkUnlockOperation, chunksToPut.toArray(new DataStructure[chunksToPut.size()]));

                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Sending chunk put request to peer 0x%X failed: %s", peer, e);
                    // #endif /* LOGGER >= ERROR */

                    // TODO
                    // m_lookup.invalidate(dataStructure.getID());

                    continue;
                }

                PutResponse response = request.getResponse(PutResponse.class);
                byte[] statusCodes = response.getStatusCodes();
                // try short cut, i.e. all puts successful
                if (statusCodes.length == 1 && statusCodes[0] == 1) {
                    chunksPut += chunksToPut.size();
                } else {
                    for (int i = 0; i < statusCodes.length; i++) {
                        if (statusCodes[i] < 0) {
                            // #if LOGGER >= ERROR
                            LOGGER.error("Remote put chunk 0x%X failed: %s", chunksToPut.get(i).getID(), statusCodes[i]);
                            // #endif /* LOGGER >= ERROR */
                        } else {
                            chunksPut++;
                        }
                    }
                }
            }
        }

        // Send backups
        if (m_backup.isActive()) {
            long backupPeersAsLong;
            short[] backupPeers;
            DataStructure[] dataStructures;
            for (Entry<Long, ArrayList<DataStructure>> entry : remoteChunksByBackupPeers.entrySet()) {
                backupPeersAsLong = entry.getKey();
                dataStructures = entry.getValue().toArray(new DataStructure[entry.getValue().size()]);

                backupPeers = BackupRange.convert(backupPeersAsLong);
                for (int i = 0; i < backupPeers.length; i++) {
                    if (backupPeers[i] != m_boot.getNodeID() && backupPeers[i] != NodeID.INVALID_ID) {
                        // #if LOGGER == TRACE
                        LOGGER.trace("Logging %d chunks to 0x%X", dataStructures.length, backupPeers[i]);
                        // #endif /* LOGGER == TRACE */

                        try {
                            m_network.sendMessage(new LogMessage(backupPeers[i], dataStructures));
                        } catch (final NetworkException ignore) {

                        }
                    }
                }
            }
        }

        // #ifdef STATISTICS
        SOP_PUT.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("put[unlockOp %s, dataStructures(%d) ...] -> %d", p_chunkUnlockOperation, p_dataStructures.length, chunksPut);
        // #endif /* LOGGER == TRACE */

        return chunksPut;
    }

    /**
     * Get/Read the data stored in the backend storage into the provided data structures.
     *
     * @param p_dataStructures
     *     Data structures to read the stored data into. Null values or invalid IDs are ignored.
     * @return Number of successfully read data structures.
     */
    public int get(final DataStructure... p_dataStructures) {
        return get(p_dataStructures, 0, p_dataStructures.length);
    }

    /**
     * Get/Read the data stored in the backend storage into the provided data structures.
     *
     * @param p_dataStructures
     *     Array with data structures to read the stored data to. Null values or invalid IDs are ignored.
     * @param p_offset
     *     Start offset within the array.
     * @param p_count
     *     Number of elements to read.
     * @return Number of successfully read data structures.
     */
    public int get(final DataStructure[] p_dataStructures, final int p_offset, final int p_count) {
        int totalChunksGot = 0;

        assert p_offset >= 0 || p_count >= 0;

        if (p_dataStructures.length == 0) {
            return totalChunksGot;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("get[dataStructures(%d) ...]", p_count);
        // #endif /* LOGGER == TRACE */

        NodeRole role = m_boot.getNodeRole();
        if (role == NodeRole.SUPERPEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s must not get chunks", role);
            // #endif /* LOGGER >= ERROR */
            return totalChunksGot;
        }

        // #ifdef STATISTICS
        SOP_GET.enter(p_count);
        // #endif /* STATISTICS */

        // sort by local and remote data first
        Map<Short, ArrayList<DataStructure>> remoteChunksByPeers = new TreeMap<>();

        m_memoryManager.lockAccess();
        for (int i = 0; i < p_count; i++) {
            // filter null values
            if (p_dataStructures[i + p_offset] == null || p_dataStructures[i + p_offset].getID() == ChunkID.INVALID_ID) {
                continue;
            }

            // try to get locally, will check first if it exists and
            // returns false if it doesn't exist
            MemoryErrorCodes err = m_memoryManager.get(p_dataStructures[i + p_offset]);
            if (err == MemoryErrorCodes.SUCCESS) {
                totalChunksGot++;
            } else {
                // remote or migrated, figure out location and sort by peers
                LookupRange lookupRange;

                lookupRange = m_lookup.getLookupRange(p_dataStructures[i + p_offset].getID());
                if (lookupRange != null) {
                    short peer = lookupRange.getPrimaryPeer();

                    ArrayList<DataStructure> remoteChunksOfPeer = remoteChunksByPeers.get(peer);
                    if (remoteChunksOfPeer == null) {
                        remoteChunksOfPeer = new ArrayList<>();
                        remoteChunksByPeers.put(peer, remoteChunksOfPeer);
                    }
                    remoteChunksOfPeer.add(p_dataStructures[i + p_offset]);
                }
            }
        }
        m_memoryManager.unlockAccess();

        // go for remote ones by each peer
        for (final Entry<Short, ArrayList<DataStructure>> peerWithChunks : remoteChunksByPeers.entrySet()) {
            short peer = peerWithChunks.getKey();
            ArrayList<DataStructure> remoteChunks = peerWithChunks.getValue();

            if (peer == m_boot.getNodeID()) {
                // local get, migrated data to current node
                m_memoryManager.lockAccess();
                for (final DataStructure dataStructure : remoteChunks) {
                    MemoryErrorCodes err = m_memoryManager.get(dataStructure);
                    if (err == MemoryErrorCodes.SUCCESS) {
                        totalChunksGot++;
                    } else {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Getting local chunk 0x%X failed: %s", dataStructure.getID(), err);
                        // #endif /* LOGGER >= ERROR */
                    }
                }
                m_memoryManager.unlockAccess();
            } else {
                // Remote get from specified peer
                GetRequest request = new GetRequest(peer, remoteChunks.toArray(new DataStructure[remoteChunks.size()]));

                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Sending chunk get request to peer 0x%X failed: %", peer, e);
                    // #endif /* LOGGER >= ERROR */
                    continue;
                }

                GetResponse response = request.getResponse(GetResponse.class);
                if (response != null) {
                    if (response.getNumberOfChunksGot() != remoteChunks.size()) {
                        // TODO not all chunks were found
                        // #if LOGGER >= WARN
                        LOGGER.warn("Could not find all chunks on peer 0x%X for chunk request", peer);
                        // #endif /* LOGGER >= WARN */
                    }

                    totalChunksGot += response.getNumberOfChunksGot();
                }
            }
        }

        // #ifdef STATISTICS
        SOP_GET.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("get[dataStructures(%d) ...] -> %d", p_dataStructures.length, totalChunksGot);
        // #endif /* LOGGER == TRACE */

        return totalChunksGot;
    }

    /**
     * Get/Read the data stored in the backend storage for chunks of unknown size. Use this if the payload size is
     * unknown, only!
     *
     * @param p_chunkIDs
     *     Array with ChunkIDs.
     * @return Int telling how many chunks were successful retrieved and a chunk array with the chunk data
     */
    public Pair<Integer, Chunk[]> get(final long... p_chunkIDs) {
        Pair<Integer, Chunk[]> ret;
        int totalNumberOfChunksGot = 0;

        if (p_chunkIDs.length == 0) {
            return null;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("get[chunkIDs(%d) ...]", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        NodeRole role = m_boot.getNodeRole();
        if (role == NodeRole.SUPERPEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s must not get chunks", role);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // #ifdef STATISTICS
        SOP_GET.enter(p_chunkIDs.length);
        // #endif /* STATISTICS */

        // sort by local and remote data first
        Map<Short, ArrayList<Integer>> remoteChunkIDsByPeers = new TreeMap<>();

        ret = new Pair<>(0, new Chunk[p_chunkIDs.length]);
        m_memoryManager.lockAccess();
        for (int i = 0; i < p_chunkIDs.length; i++) {
            // try to get locally, will check first if it exists and
            // returns false if it doesn't exist
            byte[] data = m_memoryManager.get(p_chunkIDs[i]);
            if (data != null) {
                totalNumberOfChunksGot++;
                ret.second()[i] = new Chunk(p_chunkIDs[i], ByteBuffer.wrap(data));
            } else {
                // remote or migrated, figure out location and sort by peers
                LookupRange lookupRange;

                lookupRange = m_lookup.getLookupRange(p_chunkIDs[i]);
                if (lookupRange != null) {
                    short peer = lookupRange.getPrimaryPeer();

                    ArrayList<Integer> remoteChunkIDsOfPeer = remoteChunkIDsByPeers.get(peer);
                    if (remoteChunkIDsOfPeer == null) {
                        remoteChunkIDsOfPeer = new ArrayList<>();
                        remoteChunkIDsByPeers.put(peer, remoteChunkIDsOfPeer);
                    }
                    // Add the index in ChunkID array not the ChunkID itself
                    remoteChunkIDsOfPeer.add(i);
                }
            }
        }
        m_memoryManager.unlockAccess();

        // go for remote ones by each peer
        for (final Entry<Short, ArrayList<Integer>> peerWithChunks : remoteChunkIDsByPeers.entrySet()) {
            short peer = peerWithChunks.getKey();
            ArrayList<Integer> remoteChunkIDIndexes = peerWithChunks.getValue();

            if (peer == m_boot.getNodeID()) {
                // local get, migrated data to current node
                m_memoryManager.lockAccess();
                for (final int index : remoteChunkIDIndexes) {
                    long chunkID = p_chunkIDs[index];
                    byte[] data = m_memoryManager.get(chunkID);
                    if (data != null) {
                        totalNumberOfChunksGot++;
                        ret.second()[index] = new Chunk(chunkID, ByteBuffer.wrap(data));
                    } else {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Getting local chunk 0x%X failed", chunkID);
                        // #endif /* LOGGER >= ERROR */
                    }
                }
                m_memoryManager.unlockAccess();
            } else {
                // Remote get from specified peer
                int i = 0;
                Chunk[] chunks = new Chunk[remoteChunkIDIndexes.size()];
                for (int index : remoteChunkIDIndexes) {
                    ret.second()[index] = new Chunk(p_chunkIDs[index]);
                    chunks[i++] = ret.second()[index];
                }
                GetRequest request = new GetRequest(peer, chunks);
                // mike foo requests chunk
                try {
                    m_network.sendSync(request);
                } catch (final NetworkException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Sending chunk get request to peer 0x%X failed: %s", peer, e);
                    // #endif /* LOGGER >= ERROR */
                    continue;
                }

                GetResponse response = request.getResponse(GetResponse.class);
                if (response != null) {
                    totalNumberOfChunksGot += response.getNumberOfChunksGot();
                }
            }
        }

        ret.m_first = totalNumberOfChunksGot;
        // mike foo chunk not found

        // #ifdef STATISTICS
        SOP_GET.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("get[chunkIDs(%d) ...] -> %d", p_chunkIDs.length, p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Special local only get version. Use this if you already delegate tasks with non local
     * chunks/data structures to the remote owning them. This speeds up access to local only chunks a lot.
     * Get/Read the data stored in the backend storage into the provided data structures.
     *
     * @param p_dataStructures
     *     Data structures to read the stored data into. Null values or invalid IDs are ignored.
     * @return Number of successfully read data structures.
     */
    public int getLocal(final DataStructure... p_dataStructures) {
        return getLocal(p_dataStructures, 0, p_dataStructures.length);
    }

    /**
     * Special local only get version. Use this if you already delegate tasks with non local
     * chunks/data structures to the remote owning them. This speeds up access to local only chunks a lot.
     * Get/Read the data stored in the backend storage into the provided data structures.
     *
     * @param p_dataStructures
     *     Array with data structures to read the stored data to. Null values or invalid IDs are ignored.
     * @param p_offset
     *     Start offset within the array.
     * @param p_count
     *     Number of elements to read.
     * @return Number of successfully read data structures.
     */
    public int getLocal(final DataStructure[] p_dataStructures, final int p_offset, final int p_count) {
        int totalChunksGot = 0;

        assert p_offset >= 0 || p_count >= 0;

        if (p_dataStructures.length == 0) {
            return totalChunksGot;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("getLocal[dataStructures(%d) ...]", p_count);
        // #endif /* LOGGER == TRACE */

        NodeRole role = m_boot.getNodeRole();
        if (role == NodeRole.SUPERPEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s must not get chunks", role);
            // #endif /* LOGGER >= ERROR */
            return totalChunksGot;
        }

        // #ifdef STATISTICS
        SOP_GET.enter(p_count);
        // #endif /* STATISTICS */

        m_memoryManager.lockAccess();
        for (int i = 0; i < p_count; i++) {
            // filter null values
            if (p_dataStructures[i + p_offset] == null || p_dataStructures[i + p_offset].getID() == ChunkID.INVALID_ID) {
                continue;
            }

            // try to get locally, will check first if it exists and
            // returns false if it doesn't exist
            MemoryErrorCodes err = m_memoryManager.get(p_dataStructures[i + p_offset]);
            if (err == MemoryErrorCodes.SUCCESS) {
                totalChunksGot++;
            } else {
                // #if LOGGER >= ERROR
                LOGGER.error("Getting local chunk 0x%X failed, not available.", p_dataStructures[i + p_offset].getID());
                // #endif /* LOGGER >= ERROR */
            }
        }
        m_memoryManager.unlockAccess();

        // #ifdef STATISTICS
        SOP_GET.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("getLocal[dataStructures(%d) ...] -> %d", p_dataStructures.length, totalChunksGot);
        // #endif /* LOGGER == TRACE */

        return totalChunksGot;
    }

    /**
     * Special local only get version. Use this if you already delegate tasks with non local
     * chunks/data structures to the remote owning them. This speeds up access to local only chunks a lot.
     * Get/Read the data stored in the backend storage for chunks of unknown size. Use this if the payload size is
     * unknown, only!
     *
     * @param p_chunkIDs
     *     Array with ChunkIDs.
     * @return Int telling how many chunks were successful retrieved and a chunk array with the chunk data
     */
    public Pair<Integer, Chunk[]> getLocal(final long... p_chunkIDs) {
        Pair<Integer, Chunk[]> ret;
        int totalNumberOfChunksGot = 0;

        if (p_chunkIDs.length == 0) {
            return null;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("getLocal[chunkIDs(%d) ...]", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        NodeRole role = m_boot.getNodeRole();
        if (role == NodeRole.SUPERPEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("A %s must not get chunks", role);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // #ifdef STATISTICS
        SOP_GET.enter(p_chunkIDs.length);
        // #endif /* STATISTICS */

        ret = new Pair<>(0, new Chunk[p_chunkIDs.length]);
        m_memoryManager.lockAccess();
        for (int i = 0; i < p_chunkIDs.length; i++) {
            // try to get locally, will check first if it exists and
            // returns false if it doesn't exist
            byte[] data = m_memoryManager.get(p_chunkIDs[i]);
            if (data != null) {
                totalNumberOfChunksGot++;
                ret.second()[i] = new Chunk(p_chunkIDs[i], ByteBuffer.wrap(data));
            } else {
                // #if LOGGER >= ERROR
                LOGGER.error("Getting local chunk 0x%X failed, not available", p_chunkIDs[i]);
                // #endif /* LOGGER >= ERROR */
            }
        }
        m_memoryManager.unlockAccess();

        ret.m_first = totalNumberOfChunksGot;

        // #ifdef STATISTICS
        SOP_GET.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("getLocal[chunkIDs(%d) ...] -> %d", p_chunkIDs.length, p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Get all chunk ID ranges of all stored chunks from a specific node.
     * This does not include migrated chunks.
     *
     * @param p_nodeID
     *     NodeID of the node to get the ranges from.
     * @return List of local chunk ID ranges with blocks of start ID and end ID.
     */
    public ArrayList<Long> getAllLocalChunkIDRanges(final short p_nodeID) {
        ArrayList<Long> list;

        // check if remote node is a peer
        NodeRole role = m_boot.getNodeRole(p_nodeID);
        if (role == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Remote node 0x%X does not exist for get local chunk id ranges", p_nodeID);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("%s 0x%X is not allowed to get local chunk id ranges (not allowed to have any)", role, p_nodeID);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        if (p_nodeID == m_boot.getNodeID()) {
            list = getAllLocalChunkIDRanges();
        } else {
            GetLocalChunkIDRangesRequest request = new GetLocalChunkIDRangesRequest(p_nodeID);

            try {
                m_network.sendSync(request);
            } catch (final NetworkException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Sending request to get chunk id ranges of node 0x%X failed: %s", p_nodeID, e);
                // #endif /* LOGGER >= ERROR */
                return null;
            }

            GetLocalChunkIDRangesResponse response = (GetLocalChunkIDRangesResponse) request.getResponse();
            list = response.getChunkIDRanges();
        }

        return list;
    }

    /**
     * Get all chunk ID ranges of all stored chunks from a specific node.
     * This does not include migrated chunks.
     *
     * @param p_nodeID
     *     NodeID of the node to get the ranges from.
     * @return List of local chunk ID ranges with blocks of start ID and end ID.
     */
    public ArrayList<Long> getAllMigratedChunkIDRanges(final short p_nodeID) {
        ArrayList<Long> list;

        // check if remote node is a peer
        NodeRole role = m_boot.getNodeRole(p_nodeID);
        if (role == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Remote node 0x%X does not exist for get migrated chunk id ranges", p_nodeID);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        if (role != NodeRole.PEER) {
            // #if LOGGER >= ERROR
            LOGGER.error("%s 0x%X is not allowed to get migrated chunk id ranges (not allowed to have any)", role, p_nodeID);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        if (p_nodeID == m_boot.getNodeID()) {
            list = getAllLocalChunkIDRanges();
        } else {
            GetMigratedChunkIDRangesRequest request = new GetMigratedChunkIDRangesRequest(p_nodeID);

            try {
                m_network.sendSync(request);
            } catch (final NetworkException e) {
                // #if LOGGER >= ERROR
                LOGGER.error("Sending request to get chunk id ranges of node 0x%X failed: %s", p_nodeID, e);
                // #endif /* LOGGER >= ERROR */
                return null;
            }

            GetMigratedChunkIDRangesResponse response = (GetMigratedChunkIDRangesResponse) request.getResponse();
            list = response.getChunkIDRanges();
        }

        return list;
    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {
        // #if LOGGER == TRACE
        LOGGER.trace("Entering incomingMessage with: p_message=%s", p_message);
        // #endif /* LOGGER == TRACE */

        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case ChunkMessages.SUBTYPE_GET_REQUEST:
                        incomingGetRequest((GetRequest) p_message);
                        break;
                    case ChunkMessages.SUBTYPE_PUT_REQUEST:
                        incomingPutRequest((PutRequest) p_message);
                        break;
                    case ChunkMessages.SUBTYPE_REMOVE_REQUEST:
                        incomingRemoveRequest((RemoveRequest) p_message);
                        break;
                    case ChunkMessages.SUBTYPE_CREATE_REQUEST:
                        incomingCreateRequest((CreateRequest) p_message);
                        break;
                    case ChunkMessages.SUBTYPE_STATUS_REQUEST:
                        incomingStatusRequest((StatusRequest) p_message);
                        break;
                    case ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST:
                        incomingGetLocalChunkIDRangesRequest((GetLocalChunkIDRangesRequest) p_message);
                        break;
                    case ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_REQUEST:
                        incomingGetMigratedChunkIDRangesRequest((GetMigratedChunkIDRangesRequest) p_message);
                        break;
                    default:
                        break;
                }
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting incomingMessage");
        // #endif /* LOGGER == TRACE */
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_lock = p_componentAccessor.getComponent(AbstractLockComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        registerNetworkMessages();
        registerNetworkMessageListener();

        if (m_boot.getNodeRole() == NodeRole.PEER) {
            m_backup.registerPeer();
        }

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

    // -----------------------------------------------------------------------------------

    /**
     * Register network messages we use in here.
     */
    private void registerNetworkMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_REQUEST, GetRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_RESPONSE, GetResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_REQUEST, PutRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_RESPONSE, PutResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REMOVE_REQUEST, RemoveRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REMOVE_RESPONSE, RemoveResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_CREATE_REQUEST, CreateRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_CREATE_RESPONSE, CreateResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_STATUS_REQUEST, StatusRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_STATUS_RESPONSE, StatusResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST,
            GetLocalChunkIDRangesRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_RESPONSE,
            GetLocalChunkIDRangesResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_REQUEST,
            GetMigratedChunkIDRangesRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_RESPONSE,
            GetMigratedChunkIDRangesResponse.class);
    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(GetRequest.class, this);
        m_network.register(PutRequest.class, this);
        m_network.register(RemoveRequest.class, this);
        m_network.register(CreateRequest.class, this);
        m_network.register(StatusRequest.class, this);
        m_network.register(GetLocalChunkIDRangesRequest.class, this);
        m_network.register(GetMigratedChunkIDRangesRequest.class, this);
    }

    // -----------------------------------------------------------------------------------

    /**
     * Handles an incoming GetRequest
     *
     * @param p_request
     *     the GetRequest
     */
    private void incomingGetRequest(final GetRequest p_request) {

        long[] chunkIDs = p_request.getChunkIDs();
        DataStructure[] chunks = new DataStructure[chunkIDs.length];
        int numChunksGot = 0;

        // #ifdef STATISTICS
        SOP_INCOMING_GET.enter(p_request.getChunkIDs().length);
        // #endif /* STATISTICS */

        m_memoryManager.lockAccess();
        for (int i = 0; i < chunks.length; i++) {
            // also does exist check
            int size = m_memoryManager.getSize(chunkIDs[i]);
            if (size < 0) {
                // #if LOGGER >= WARN
                LOGGER.warn("Getting size of chunk 0x%X failed, does not exist", chunkIDs[i]);
                // #endif /* LOGGER >= WARN */
                size = 0;
            } else {
                numChunksGot++;
            }

            // we have to use an instance of a data structure here in order to
            // handle the remote data locally
            chunks[i] = new Chunk(chunkIDs[i], size);
            m_memoryManager.get(chunks[i]);
        }
        m_memoryManager.unlockAccess();

        GetResponse response = new GetResponse(p_request, numChunksGot, chunks);

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending GetResponse for %d chunks failed: %s", numChunksGot, e);
            // #endif /* LOGGER >= ERROR */
        }

        // #ifdef STATISTICS
        SOP_INCOMING_GET.leave();
        // #endif /* STATISTICS */
    }

    /**
     * Handles an incoming PutRequest
     *
     * @param p_request
     *     the PutRequest
     */
    private void incomingPutRequest(final PutRequest p_request) {
        DataStructure[] chunks = p_request.getDataStructures();
        byte[] statusChunks = new byte[chunks.length];
        boolean allSuccessful = true;

        // #ifdef STATISTICS
        SOP_INCOMING_PUT.enter(chunks.length);
        // #endif /* STATISTICS */

        Map<Long, ArrayList<DataStructure>> remoteChunksByBackupPeers = new TreeMap<>();

        m_memoryManager.lockAccess();
        for (int i = 0; i < chunks.length; i++) {
            MemoryErrorCodes err = m_memoryManager.put(chunks[i]);
            if (err != MemoryErrorCodes.SUCCESS) {
                // does not exist (anymore)
                statusChunks[i] = -1;
                // #if LOGGER >= WARN
                LOGGER.warn("Putting chunk 0x%X failed, does not exist", chunks[i].getID());
                // #endif /* LOGGER >= WARN */
                allSuccessful = false;
            } else {
                // put successful
                statusChunks[i] = 0;
            }

            if (m_backup.isActive()) {
                // sort by backup peers
                long backupPeersAsLong = m_backup.getBackupPeersForLocalChunks(chunks[i].getID());
                ArrayList<DataStructure> remoteChunksOfBackupPeers = remoteChunksByBackupPeers.get(backupPeersAsLong);
                if (remoteChunksOfBackupPeers == null) {
                    remoteChunksOfBackupPeers = new ArrayList<>();
                    remoteChunksByBackupPeers.put(backupPeersAsLong, remoteChunksOfBackupPeers);
                }
                remoteChunksOfBackupPeers.add(chunks[i]);
            }
        }
        m_memoryManager.unlockAccess();

        // unlock chunks
        if (p_request.getUnlockOperation() != ChunkLockOperation.NO_LOCK_OPERATION) {
            boolean writeLock = false;
            if (p_request.getUnlockOperation() == ChunkLockOperation.WRITE_LOCK) {
                writeLock = true;
            }

            for (DataStructure dataStructure : chunks) {
                m_lock.unlock(dataStructure.getID(), m_boot.getNodeID(), writeLock);
            }
        }

        PutResponse response;
        // cut message length if all were successful
        if (allSuccessful) {
            response = new PutResponse(p_request, (byte) 1);
        } else {
            // we got errors, default message
            response = new PutResponse(p_request, statusChunks);
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending chunk put respond to request %s failed: %s", p_request, e);
            // #endif /* LOGGER >= ERROR */
        }

        // Send backups
        if (m_backup.isActive()) {
            long backupPeersAsLong;
            short[] backupPeers;
            DataStructure[] dataStructures;
            for (Entry<Long, ArrayList<DataStructure>> entry : remoteChunksByBackupPeers.entrySet()) {
                backupPeersAsLong = entry.getKey();
                dataStructures = entry.getValue().toArray(new DataStructure[entry.getValue().size()]);

                backupPeers = BackupRange.convert(backupPeersAsLong);
                for (int i = 0; i < backupPeers.length; i++) {
                    if (backupPeers[i] != m_boot.getNodeID() && backupPeers[i] != -1) {
                        // #if LOGGER == TRACE
                        LOGGER.trace("Logging %d chunks to 0x%X", dataStructures.length, backupPeers[i]);
                        // #endif /* LOGGER == TRACE */

                        try {
                            m_network.sendMessage(new LogMessage(backupPeers[i], dataStructures));
                        } catch (final NetworkException ignore) {

                        }
                    }
                }
            }
        }

        // #ifdef STATISTICS
        SOP_INCOMING_PUT.leave();
        // #endif /* STATISTICS */
    }

    /**
     * Handles an incoming RemoveRequest
     *
     * @param p_request
     *     the RemoveRequest
     */
    private void incomingRemoveRequest(final RemoveRequest p_request) {
        // #ifdef STATISTICS
        SOP_INCOMING_REMOVE.enter(p_request.getChunkIDs().length);
        // #endif /* STATISTICS */

        Long[] chunkIDs = p_request.getChunkIDs();
        byte[] chunkStatusCodes = new byte[chunkIDs.length];
        boolean allSuccessful = true;

        Map<Long, ArrayList<Long>> remoteChunksByBackupPeers = new TreeMap<>();

        // remove chunks from superpeer overlay first, so cannot be found before being deleted
        for (int i = 0; i < chunkIDs.length; i++) {
            m_lookup.removeChunkIDs(new long[] {chunkIDs[i]});

            if (m_backup.isActive()) {
                // sort by backup peers
                long backupPeersAsLong = m_backup.getBackupPeersForLocalChunks(chunkIDs[i]);
                ArrayList<Long> remoteChunkIDsOfBackupPeers = remoteChunksByBackupPeers.get(backupPeersAsLong);
                if (remoteChunkIDsOfBackupPeers == null) {
                    remoteChunkIDsOfBackupPeers = new ArrayList<>();
                    remoteChunksByBackupPeers.put(backupPeersAsLong, remoteChunkIDsOfBackupPeers);
                }
                remoteChunkIDsOfBackupPeers.add(chunkIDs[i]);
            }
        }

        // remove chunks first (local)
        m_memoryManager.lockManage();
        for (int i = 0; i < chunkIDs.length; i++) {
            MemoryErrorCodes err = m_memoryManager.remove(chunkIDs[i], false);
            if (err == MemoryErrorCodes.SUCCESS) {
                // remove successful
                chunkStatusCodes[i] = 0;
            } else {
                // remove failed, might be removed recently by someone else
                chunkStatusCodes[i] = -1;
                allSuccessful = false;
            }
        }
        m_memoryManager.unlockManage();

        RemoveResponse response;
        if (allSuccessful) {
            // use a short version to indicate everything is ok
            response = new RemoveResponse(p_request, (byte) 2);
        } else {
            // errors occured, send full status report
            response = new RemoveResponse(p_request, chunkStatusCodes);
        }

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending chunk remove respond to request %s failed: ", p_request, e);
            // #endif /* LOGGER >= ERROR */
        }

        // TODO for migrated chunks, send remove request to peer currently holding the chunk data
        // for (int i = 0; i < chunkIDs.length; i++) {
        // byte rangeID = m_backup.getBackupRange(chunkIDs[i]);
        // short[] backupPeers = m_backup.getBackupPeersForLocalChunks(chunkIDs[i]);
        // m_backup.removeChunk(chunkIDs[i]);
        //
        // if (m_memoryManager.dataWasMigrated(chunkIDs[i])) {
        // // Inform peer who got the migrated data about removal
        // RemoveRequest request = new RemoveRequest(ChunkID.getCreatorID(chunkIDs[i]), new Chunk(chunkIDs[i], 0));
        // try {
        // request.sendSync(m_network);
        // request.getResponse(RemoveResponse.class);
        // } catch (final NetworkException e) {
        // LOGGER.error("Informing creator about removal of chunk " + chunkIDs[i] + " failed.", e);
        // }
        // }
        // }

        // Inform backups
        if (m_backup.isActive()) {
            long backupPeersAsLong;
            short[] backupPeers;
            Long[] ids;
            for (Entry<Long, ArrayList<Long>> entry : remoteChunksByBackupPeers.entrySet()) {
                backupPeersAsLong = entry.getKey();
                ids = entry.getValue().toArray(new Long[entry.getValue().size()]);

                backupPeers = BackupRange.convert(backupPeersAsLong);
                for (int i = 0; i < backupPeers.length; i++) {
                    if (backupPeers[i] != m_boot.getNodeID() && backupPeers[i] != -1) {
                        try {
                            m_network.sendMessage(new RemoveMessage(backupPeers[i], ids));
                        } catch (final NetworkException ignore) {

                        }
                    }
                }
            }
        }

        // #ifdef STATISTICS
        SOP_INCOMING_REMOVE.leave();
        // #endif /* STATISTICS */
    }

    /**
     * Handle incoming create requests.
     *
     * @param p_request
     *     Request to handle
     */
    private void incomingCreateRequest(final CreateRequest p_request) {
        // #ifdef STATISTICS
        SOP_INCOMING_CREATE.enter(p_request.getSizes().length);
        // #endif /* STATISTICS */

        int[] sizes = p_request.getSizes();
        long[] chunkIDs = new long[sizes.length];

        if (sizes.length == 1) {
            m_memoryManager.lockManage();
            chunkIDs[0] = m_memoryManager.create(sizes[0]);
            m_memoryManager.unlockManage();

            // tell the superpeer overlay about our newly created chunks, otherwise they can not be found
            // by other peers (network traffic for backup range initialization only (e.g. every 256 MB))
            m_backup.initBackupRange(chunkIDs[0], sizes[0]);
        } else {
            m_memoryManager.lockManage();
            chunkIDs = m_memoryManager.createMultiSizes(sizes);
            m_memoryManager.unlockManage();

            if (chunkIDs == null) {
                // #if LOGGER >= ERROR
                LOGGER.error("Multi create chunks failed");
                // #endif /* LOGGER >= ERROR */

                for (int i = 0; i < chunkIDs.length; i++) {
                    chunkIDs[i] = ChunkID.INVALID_ID;
                }
            } else {
                // keep loop tight and execute everything
                // that we don't have to lock outside of this section
                for (int i = 0; i < sizes.length; i++) {
                    // tell the superpeer overlay about our newly created chunks, otherwise they can not be found
                    // by other peers (network traffic for backup range initialization only (e.g. every 256 MB))
                    m_backup.initBackupRange(chunkIDs[i], sizes[i]);
                }
            }
        }

        CreateResponse response = new CreateResponse(p_request, chunkIDs);
        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending chunk create respond to request %s failed: ", p_request, e);
            // #endif /* LOGGER >= ERROR */
        }

        // #ifdef STATISTICS
        SOP_INCOMING_CREATE.leave();
        // #endif /* STATISTICS */
    }

    /**
     * Handle incoming status requests.
     *
     * @param p_request
     *     Request to handle
     */
    private void incomingStatusRequest(final StatusRequest p_request) {
        Status status = getStatus();

        StatusResponse response = new StatusResponse(p_request, status);
        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending status respond to request %s failed: %s", p_request, e);
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Handle incoming get local chunk id ranges requests.
     *
     * @param p_request
     *     Request to handle
     */
    private void incomingGetLocalChunkIDRangesRequest(final GetLocalChunkIDRangesRequest p_request) {
        ArrayList<Long> cidRangesLocalChunks;

        m_memoryManager.lockAccess();
        cidRangesLocalChunks = m_memoryManager.getCIDRangesOfAllLocalChunks();
        m_memoryManager.unlockAccess();

        if (cidRangesLocalChunks == null) {
            cidRangesLocalChunks = new ArrayList<>(0);
            // #if LOGGER >= ERROR
            LOGGER.error("Getting local chunk id ranges failed, sending back empty range");
            // #endif /* LOGGER >= ERROR */
        }

        GetLocalChunkIDRangesResponse response = new GetLocalChunkIDRangesResponse(p_request, cidRangesLocalChunks);
        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Responding to local chunk id ranges request %s failed: %s", p_request, e);
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Handle incoming get migrated local chunk id ranges requests.
     *
     * @param p_request
     *     Request to handle
     */
    private void incomingGetMigratedChunkIDRangesRequest(final GetMigratedChunkIDRangesRequest p_request) {
        ArrayList<Long> cidRangesMigratedChunks;

        m_memoryManager.lockAccess();
        cidRangesMigratedChunks = m_memoryManager.getCIDOfAllMigratedChunks();
        m_memoryManager.unlockAccess();

        if (cidRangesMigratedChunks == null) {
            cidRangesMigratedChunks = new ArrayList<>(0);
            // #if LOGGER >= ERROR
            LOGGER.error("Getting migrated chunk id ranges failed, sending back empty range");
            // #endif /* LOGGER >= ERROR */
        }

        GetMigratedChunkIDRangesResponse response = new GetMigratedChunkIDRangesResponse(p_request, cidRangesMigratedChunks);

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Responding to migrated chunk id ranges request %s failed: %s", p_request, e);
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Status object for the chunk service containing various information
     * about it.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.03.2016
     */
    public static class Status implements Importable, Exportable {
        private long m_freeMemoryBytes = -1;
        private long m_totalMemoryBytes = -1;
        private long m_totalPayloadMemoryBytes = -1;
        private long m_numberOfActiveMemoryBlocks = -1;
        private long m_numberOfActiveChunks = -1;
        private long m_totalChunkPayloadMemory = -1;
        private long m_cidTableCount = -1;
        private long m_totalMemoryCIDTables = -1;

        /**
         * Default constructor
         */
        public Status() {

        }

        /**
         * Get the amount of free memory in bytes.
         *
         * @return Free memory in bytes.
         */
        public long getFreeMemory() {
            return m_freeMemoryBytes;
        }

        /**
         * Get the total amount of memory in bytes available.
         *
         * @return Total amount of memory in bytes.
         */
        public long getTotalMemory() {
            return m_totalMemoryBytes;
        }

        /**
         * Get the total number of active/allocated memory blocks.
         *
         * @return Number of allocated memory blocks.
         */
        public long getNumberOfActiveMemoryBlocks() {
            return m_numberOfActiveMemoryBlocks;
        }

        /**
         * Get the total number of currently active chunks.
         *
         * @return Number of active/allocated chunks.
         */
        public long getNumberOfActiveChunks() {
            return m_numberOfActiveChunks;
        }

        /**
         * Get the amount of memory used by chunk payload/data.
         *
         * @return Amount of memory used by chunk payload in bytes.
         */
        public long getTotalChunkPayloadMemory() {
            return m_totalChunkPayloadMemory;
        }

        /**
         * Get the number of currently allocated CID tables.
         *
         * @return Number of CID tables.
         */
        public long getCIDTableCount() {
            return m_cidTableCount;
        }

        /**
         * Get the total memory used by CID tables (payload only).
         *
         * @return Total memory used by CID tables in bytes.
         */
        public long getTotalMemoryCIDTables() {
            return m_totalMemoryCIDTables;
        }

        /**
         * Get the total amount of memory allocated and usable for actual payload/data.
         *
         * @return Total amount of memory usable for payload (in bytes).
         */
        public long getTotalPayloadMemory() {
            return m_totalPayloadMemoryBytes;
        }

        @Override
        public int sizeofObject() {
            return Long.BYTES * 8;
        }

        @Override
        public void exportObject(final Exporter p_exporter) {
            p_exporter.writeLong(m_freeMemoryBytes);
            p_exporter.writeLong(m_totalMemoryBytes);
            p_exporter.writeLong(m_totalPayloadMemoryBytes);
            p_exporter.writeLong(m_numberOfActiveMemoryBlocks);
            p_exporter.writeLong(m_numberOfActiveChunks);
            p_exporter.writeLong(m_totalChunkPayloadMemory);
            p_exporter.writeLong(m_cidTableCount);
            p_exporter.writeLong(m_totalMemoryCIDTables);
        }

        @Override
        public void importObject(final Importer p_importer) {
            m_freeMemoryBytes = p_importer.readLong();
            m_totalMemoryBytes = p_importer.readLong();
            m_totalPayloadMemoryBytes = p_importer.readLong();
            m_numberOfActiveMemoryBlocks = p_importer.readLong();
            m_numberOfActiveChunks = p_importer.readLong();
            m_totalChunkPayloadMemory = p_importer.readLong();
            m_cidTableCount = p_importer.readLong();
            m_totalMemoryCIDTables = p_importer.readLong();
        }

        @Override
        public String toString() {
            String str = "";
            str += "Free memory (bytes): " + m_freeMemoryBytes + '\n';
            str += "Total memory (bytes): " + m_totalMemoryBytes + '\n';
            str += "Total payload memory (bytes): " + m_totalPayloadMemoryBytes + '\n';
            str += "Num active memory blocks: " + m_numberOfActiveMemoryBlocks + '\n';
            str += "Num active chunks: " + m_numberOfActiveChunks + '\n';
            str += "Total chunk payload memory (bytes): " + m_totalChunkPayloadMemory + '\n';
            str += "Num CID tables: " + m_cidTableCount + '\n';
            str += "Total CID tables memory (bytes): " + m_totalMemoryCIDTables;
            return str;
        }
    }
}

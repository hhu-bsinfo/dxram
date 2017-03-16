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
import de.hhu.bsinfo.dxram.chunk.messages.RemoveMessage;
import de.hhu.bsinfo.dxram.chunk.messages.StatusRequest;
import de.hhu.bsinfo.dxram.chunk.messages.StatusResponse;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMRuntimeException;
import de.hhu.bsinfo.dxram.engine.InvalidNodeRoleException;
import de.hhu.bsinfo.dxram.lock.AbstractLockComponent;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.dxram.util.ArrayListLong;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.Pair;

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
     * Get the memory status
     *
     * @return Status object with current status of the key value store memory
     */
    public MemoryManagerComponent.Status getStatus() {
        return m_memoryManager.getStatus();
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
            return memManStatus.getTotalMemory().getBytes();
        } else {
            return -1;
        }
    }

    /**
     * Get the amount of free memory.
     *
     * @return Amount of free memory in bytes.
     */
    public long getFreeMemory() {
        MemoryManagerComponent.Status memManStatus;

        memManStatus = m_memoryManager.getStatus();

        if (memManStatus != null) {
            return memManStatus.getFreeMemory().getBytes();
        } else {
            return -1;
        }
    }

    /**
     * Get all chunk ID ranges of all migrated chunks stored on this node.
     *
     * @return ChunkIDRanges of all migrated chunks.
     */
    public ChunkIDRanges getAllMigratedChunkIDRanges() {
        ChunkIDRanges list;

        try {
            m_memoryManager.lockAccess();
            list = m_memoryManager.getCIDRangesOfAllMigratedChunks();
        } finally {
            m_memoryManager.unlockAccess();
        }

        return list;
    }

    /**
     * Get all chunk ID ranges of all locally stored chunks.
     *
     * @return Local ChunkIDRanges.
     */
    public ChunkIDRanges getAllLocalChunkIDRanges() {
        ChunkIDRanges list;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() != NodeRole.PEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        try {
            m_memoryManager.lockAccess();
            list = m_memoryManager.getCIDRangesOfAllLocalChunks();
        } finally {
            m_memoryManager.unlockAccess();
        }

        return list;
    }

    /**
     * Get the memory status of a remote node specified by a node id.
     *
     * @param p_nodeID
     *     Node id to get the status from.
     * @return Status object with status information of the remote node or null if getting status failed.
     */
    public MemoryManagerComponent.Status getStatus(final short p_nodeID) {
        MemoryManagerComponent.Status status = null;

        if (p_nodeID == NodeID.INVALID_ID) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid node id on get status");
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

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
     * Create new chunks.
     *
     * @param p_size
     *     Size of the new chunk.
     * @param p_count
     *     Number of chunks to create with the specified size.
     * @return ChunkIDs/Handles identifying the created chunks.
     */
    public long[] create(final int p_size, final int p_count) {
        return create(p_size, p_count, false);
    }

    /**
     * Create new chunks.
     *
     * @param p_size
     *     Size of the new chunk.
     * @param p_count
     *     Number of chunks to create with the specified size.
     * @param p_consecutive
     *     Whether the ChunkIDs must be consecutive or not.
     * @return ChunkIDs/Handles identifying the created chunks.
     */
    public long[] create(final int p_size, final int p_count, final boolean p_consecutive) {
        long[] chunkIDs = null;

        assert p_size > 0 && p_count > 0;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() != NodeRole.PEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        // #if LOGGER == TRACE
        LOGGER.trace("create[size %d, count %d]", p_size, p_count);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_CREATE.enter(p_count);
        // #endif /* STATISTICS */

        if (p_count == 1) {
            long chunkId = -1;
            try {
                m_memoryManager.lockManage();
                chunkId = m_memoryManager.create(p_size);

                // Initialize a new backup range every e.g. 256 MB and inform superpeer
                // Must be locked together with create call to memory manager
                m_backup.registerChunk(chunkId, p_size);
            } finally {
                m_memoryManager.unlockManage();
            }

            if (chunkId != ChunkID.INVALID_ID) {
                chunkIDs = new long[] {chunkId};
            }
        } else {
            try {
                m_memoryManager.lockManage();
                chunkIDs = m_memoryManager.createMulti(p_size, p_count, p_consecutive);

                // Initialize a new backup range every e.g. 256 MB and inform superpeer
                // Must be locked together with create call to memory manager
                m_backup.registerChunks(chunkIDs, p_size);
            } finally {
                m_memoryManager.unlockManage();
            }
        }

        if (chunkIDs == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Multi create for size %d, count %d failed", p_size, p_count);
            // #endif /* LOGGER >= ERROR */
            return null;
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
        return create(false, p_dataStructures);
    }

    /**
     * Create new chunks according to the data structures provided.
     * Important: This does NOT put/write the contents of the data structure provided.
     * It creates chunks with the sizes of the data structures and sets the IDs.
     *
     * @param p_consecutive
     *     Whether the ChunkIDs must be consecutive or not.
     * @param p_dataStructures
     *     Data structures to create chunks for.
     * @return Number of successfully created chunks.
     */
    public int create(final boolean p_consecutive, final DataStructure... p_dataStructures) {
        int count = 0;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() != NodeRole.PEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_dataStructures.length == 0) {
            return count;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("create[numDataStructures %d...]", p_dataStructures.length);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_CREATE.enter(p_dataStructures.length);
        // #endif /* STATISTICS */

        if (p_dataStructures.length == 1) {
            long chunkID = ChunkID.INVALID_ID;
            try {
                m_memoryManager.lockManage();
                chunkID = m_memoryManager.create(p_dataStructures[0].sizeofObject());

                // Initialize a new backup range every e.g. 256 MB and inform superpeer
                // Must be locked together with create call to memory manager
                m_backup.registerChunk(chunkID, p_dataStructures[0].sizeofObject());
            } finally {
                m_memoryManager.unlockManage();
            }

            if (chunkID != ChunkID.INVALID_ID) {
                p_dataStructures[0].setID(chunkID);
                count++;
            } else {
                p_dataStructures[0].setID(ChunkID.INVALID_ID);
                // #if LOGGER == ERROR
                LOGGER.error("Creating chunk for size %d failed", p_dataStructures[0].sizeofObject());
                // #endif /* LOGGER == ERROR */
            }
        } else {
            try {
                m_memoryManager.lockManage();
                m_memoryManager.createMulti(p_consecutive, p_dataStructures);

                // Initialize a new backup range every e.g. 256 MB and inform superpeer
                // Must be locked together with create call to memory manager
                m_backup.registerChunks(p_dataStructures);
            } finally {
                m_memoryManager.unlockManage();
            }

            count += p_dataStructures.length;
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
        return createSizes(false, p_sizes);
    }

    /**
     * Create chunks with different sizes.
     *
     * @param p_sizes
     *     List of sizes to create chunks for.
     * @return ChunkIDs/Handles identifying the created chunks.
     * @parm p_consecutive
     * Whether the ChunkIDs must be consecutive or not.
     */
    public long[] createSizes(final boolean p_consecutive, final int... p_sizes) {
        long[] chunkIDs;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() != NodeRole.PEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_sizes.length == 0) {
            return new long[0];
        }

        // #if LOGGER == TRACE
        LOGGER.trace("create[sizes(%d) %d, ...]", p_sizes.length, p_sizes[0]);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_CREATE.enter(p_sizes.length);
        // #endif /* STATISTICS */

        chunkIDs = new long[p_sizes.length];

        if (p_sizes.length == 1) {
            try {
                m_memoryManager.lockManage();
                chunkIDs[0] = m_memoryManager.create(p_sizes[0]);

                // Initialize a new backup range every e.g. 256 MB and inform superpeer
                // Must be locked together with create call to memory manager
                m_backup.registerChunk(chunkIDs[0], p_sizes[0]);
            } finally {
                m_memoryManager.unlockManage();
            }
        } else {
            try {
                m_memoryManager.lockManage();
                chunkIDs = m_memoryManager.createMultiSizes(p_consecutive, p_sizes);

                // Initialize a new backup range every e.g. 256 MB and inform superpeer
                // Must be locked together with create call to memory manager
                m_backup.registerChunks(chunkIDs, p_sizes);
            } finally {
                m_memoryManager.unlockManage();
            }

            if (chunkIDs == null) {
                // #if LOGGER >= ERROR
                LOGGER.error("Multi create chunks failed");
                // #endif /* LOGGER >= ERROR */
                return null;
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

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

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
        int size;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_chunkIDs.length == 0) {
            return chunksRemoved;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("remove[dataStructures(%d) %s, ...]", p_chunkIDs.length, ChunkID.toHexString(p_chunkIDs[0]));
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_REMOVE.enter(p_chunkIDs.length);
        // #endif /* STATISTICS */

        // sort by local and remote data first
        Map<Short, ArrayListLong> remoteChunksByPeers = new TreeMap<>();
        Map<Long, ArrayListLong> remoteChunksByBackupPeers = new TreeMap<>();
        ArrayListLong localChunks = new ArrayListLong();

        try {
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
                        ArrayListLong remoteChunkIDsOfBackupPeers = remoteChunksByBackupPeers.get(backupPeersAsLong);
                        if (remoteChunkIDsOfBackupPeers == null) {
                            remoteChunkIDsOfBackupPeers = new ArrayListLong();
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

                        ArrayListLong remoteChunksOfPeer = remoteChunksByPeers.get(peer);
                        if (remoteChunksOfPeer == null) {
                            remoteChunksOfPeer = new ArrayListLong();
                            remoteChunksByPeers.put(peer, remoteChunksOfPeer);
                        }
                        remoteChunksOfPeer.add(p_chunkIDs[i]);
                    }
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

        m_memoryManager.unlockAccess();

        // remove local chunks from superpeer overlay first, so cannot be found before being deleted
        m_lookup.removeChunkIDs(localChunks);

        try {
            // remove local chunkIDs
            m_memoryManager.lockManage();
            for (int i = 0; i < localChunks.getSize(); i++) {
                size = m_memoryManager.remove(localChunks.get(i), false);
                if (size > 0) {
                    chunksRemoved++;
                    m_backup.deregisterChunk(localChunks.get(i), size);
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Removing chunk ID 0x%X failed, does not exist", localChunks.get(i));
                    // #endif /* LOGGER >= ERROR */
                }
            }
        } finally {
            m_memoryManager.unlockManage();
        }

        // go for remote ones by each peer
        for (final Entry<Short, ArrayListLong> peerWithChunks : remoteChunksByPeers.entrySet()) {
            short peer = peerWithChunks.getKey();
            ArrayListLong remoteChunks = peerWithChunks.getValue();

            if (peer == m_boot.getNodeID()) {
                // local remove, migrated data to current node

                // FIXME kevin (refer to onIncomingRemoveRequest
                // remove local chunks from superpeer overlay first, so cannot be found before being deleted
                //m_lookup.removeChunkIDs(localChunks, m_boot.getNodeID());

                try {
                    m_memoryManager.lockManage();
                    for (int i = 0; i < remoteChunks.getSize(); i++) {
                        size = m_memoryManager.remove(localChunks.get(i), false);
                        if (size > 0) {
                            chunksRemoved++;
                            m_backup.deregisterChunk(localChunks.get(i), size);
                        } else {
                            // #if LOGGER >= ERROR
                            LOGGER.error("Removing chunk ID 0x%X failed, does not exist", remoteChunks.get(i));
                            // #endif /* LOGGER >= ERROR */
                        }
                    }
                } finally {
                    m_memoryManager.unlockManage();
                }
            } else {
                // Remote remove from specified peer
                RemoveMessage message = new RemoveMessage(peer, remoteChunks);
                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Sending chunk remove to peer 0x%X failed: %s", peer, e);
                    // #endif /* LOGGER >= ERROR */
                    continue;
                }

                chunksRemoved += remoteChunks.getSize();
            }
        }

        // Inform backups
        if (m_backup.isActive()) {
            long backupPeersAsLong;
            short[] backupPeers;
            ArrayListLong ids;
            for (Entry<Long, ArrayListLong> entry : remoteChunksByBackupPeers.entrySet()) {
                backupPeersAsLong = entry.getKey();
                ids = entry.getValue();

                backupPeers = BackupRange.convert(backupPeersAsLong);
                for (int i = 0; i < backupPeers.length; i++) {
                    if (backupPeers[i] != m_boot.getNodeID() && backupPeers[i] != NodeID.INVALID_ID) {
                        try {
                            m_network.sendMessage(new de.hhu.bsinfo.dxram.log.messages.RemoveMessage(backupPeers[i], ids));
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

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_dataStructures.length == 0) {
            return chunksPut;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("put[unlockOp %s, dataStructures(%d) ...]", p_chunkUnlockOperation, p_dataStructures.length);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_PUT.enter(p_count);
        // #endif /* STATISTICS */

        Map<Short, ArrayList<DataStructure>> remoteChunksByPeers = new TreeMap<>();
        Map<BackupRange, ArrayList<DataStructure>> remoteChunksByBackupRange = new TreeMap<>();

        // sort by local/remote chunks
        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < p_count; i++) {
                // filter null values
                if (p_dataStructures[i + p_offset] == null || p_dataStructures[i + p_offset].getID() == ChunkID.INVALID_ID) {
                    continue;
                }

                // try to put every chunk locally, returns false if it does not exist
                // and saves us an additional check
                if (m_memoryManager.put(p_dataStructures[i + p_offset])) {
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
                        BackupRange backupRange = m_backup.getBackupRange(p_dataStructures[i + p_offset].getID());
                        ArrayList<DataStructure> remoteChunksOfBackupRange = remoteChunksByBackupRange.get(backupRange);
                        if (remoteChunksOfBackupRange == null) {
                            remoteChunksOfBackupRange = new ArrayList<>();
                            remoteChunksByBackupRange.put(backupRange, remoteChunksOfBackupRange);
                        }
                        remoteChunksOfBackupRange.add(p_dataStructures[i + p_offset]);
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
                        LOGGER.error("Location of Chunk 0x%X is unknown, it cannot be put!", p_dataStructures[i + p_offset].getID());
                        // #endif /* LOGGER >= ERROR */
                    }
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

        // go for remote chunks
        for (Entry<Short, ArrayList<DataStructure>> entry : remoteChunksByPeers.entrySet()) {
            short peer = entry.getKey();

            if (peer == m_boot.getNodeID()) {
                // local put, migrated data to current node
                try {
                    m_memoryManager.lockAccess();
                    for (final DataStructure dataStructure : entry.getValue()) {
                        if (m_memoryManager.put(dataStructure)) {
                            chunksPut++;
                        } else {
                            // #if LOGGER >= ERROR
                            LOGGER.error("Putting local chunk 0x%X failed", dataStructure.getID());
                            // #endif /* LOGGER >= ERROR */
                        }
                    }
                } finally {
                    m_memoryManager.unlockAccess();
                }
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
            BackupRange backupRange;
            short[] backupPeers;
            DataStructure[] dataStructures;
            for (Entry<BackupRange, ArrayList<DataStructure>> entry : remoteChunksByBackupRange.entrySet()) {
                backupRange = entry.getKey();
                dataStructures = entry.getValue().toArray(new DataStructure[entry.getValue().size()]);

                backupPeers = backupRange.getBackupPeers();
                for (short backupPeer : backupPeers) {
                    if (backupPeer != NodeID.INVALID_ID) {
                        // #if LOGGER == TRACE
                        LOGGER.trace("Logging %d chunks to 0x%X", dataStructures.length, backupPeer);
                        // #endif /* LOGGER == TRACE */

                        try {
                            m_network.sendMessage(new LogMessage(backupPeer, backupRange.getRangeID(), dataStructures));
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

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_dataStructures.length == 0) {
            return totalChunksGot;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("get[dataStructures(%d) ...]", p_count);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_GET.enter(p_count);
        // #endif /* STATISTICS */

        // sort by local and remote data first
        Map<Short, ArrayList<DataStructure>> remoteChunksByPeers = new TreeMap<>();

        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < p_count; i++) {
                // filter null values
                if (p_dataStructures[i + p_offset] == null || p_dataStructures[i + p_offset].getID() == ChunkID.INVALID_ID) {
                    continue;
                }

                // try to get locally, will check first if it exists and
                // returns false if it doesn't exist
                if (m_memoryManager.get(p_dataStructures[i + p_offset])) {
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
        } finally {
            m_memoryManager.unlockAccess();
        }

        // go for remote ones by each peer
        for (final Entry<Short, ArrayList<DataStructure>> peerWithChunks : remoteChunksByPeers.entrySet()) {
            short peer = peerWithChunks.getKey();
            ArrayList<DataStructure> remoteChunks = peerWithChunks.getValue();

            if (peer == m_boot.getNodeID()) {
                // local get, migrated data to current node
                try {
                    m_memoryManager.lockAccess();
                    for (final DataStructure dataStructure : remoteChunks) {
                        if (m_memoryManager.get(dataStructure)) {
                            totalChunksGot++;
                        } else {
                            // #if LOGGER >= ERROR
                            LOGGER.error("Getting local chunk 0x%X failed, does not exist", dataStructure.getID());
                            // #endif /* LOGGER >= ERROR */
                        }
                    }
                } finally {
                    m_memoryManager.unlockAccess();
                }
            } else {
                // Remote get from specified peer
                GetRequest request = new GetRequest(peer, remoteChunks.toArray(new DataStructure[remoteChunks.size()]));

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
    public Chunk[] get(final long... p_chunkIDs) {
        Chunk[] ret;

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_chunkIDs.length == 0) {
            return null;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("get[chunkIDs(%d) ...]", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_GET.enter(p_chunkIDs.length);
        // #endif /* STATISTICS */

        // sort by local and remote data first
        Map<Short, ArrayList<Integer>> remoteChunkIDsByPeers = new TreeMap<>();

        ret = new Chunk[p_chunkIDs.length];
        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < p_chunkIDs.length; i++) {
                // try to get locally, will check first if it exists and
                // returns false if it doesn't exist
                byte[] data = m_memoryManager.get(p_chunkIDs[i]);
                if (data != null) {
                    ret[i] = new Chunk(p_chunkIDs[i], ByteBuffer.wrap(data));
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
        } finally {
            m_memoryManager.unlockAccess();
        }

        // go for remote ones by each peer
        for (final Entry<Short, ArrayList<Integer>> peerWithChunks : remoteChunkIDsByPeers.entrySet()) {
            short peer = peerWithChunks.getKey();
            ArrayList<Integer> remoteChunkIDIndexes = peerWithChunks.getValue();

            if (peer == m_boot.getNodeID()) {
                // local get, migrated data to current node
                try {
                    m_memoryManager.lockAccess();
                    for (final int index : remoteChunkIDIndexes) {
                        long chunkID = p_chunkIDs[index];
                        byte[] data = m_memoryManager.get(chunkID);
                        if (data != null) {
                            ret[index] = new Chunk(chunkID, ByteBuffer.wrap(data));
                        } else {
                            // #if LOGGER >= ERROR
                            LOGGER.error("Getting local chunk 0x%X failed", chunkID);
                            // #endif /* LOGGER >= ERROR */
                        }
                    }
                } finally {
                    m_memoryManager.unlockAccess();
                }
            } else {
                // Remote get from specified peer
                int i = 0;
                Chunk[] chunks = new Chunk[remoteChunkIDIndexes.size()];
                for (int index : remoteChunkIDIndexes) {
                    ret[index] = new Chunk(p_chunkIDs[index]);
                    chunks[i++] = ret[index];
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

                request.getResponse(GetResponse.class);
            }
        }

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

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_dataStructures.length == 0) {
            return totalChunksGot;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("getLocal[dataStructures(%d) ...]", p_count);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_GET.enter(p_count);
        // #endif /* STATISTICS */

        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < p_count; i++) {
                // filter null values
                if (p_dataStructures[i + p_offset] == null || p_dataStructures[i + p_offset].getID() == ChunkID.INVALID_ID) {
                    continue;
                }

                // try to get locally, will check first if it exists and
                // returns false if it doesn't exist
                if (m_memoryManager.get(p_dataStructures[i + p_offset])) {
                    totalChunksGot++;
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Getting local chunk 0x%X failed, not available.", p_dataStructures[i + p_offset].getID());
                    // #endif /* LOGGER >= ERROR */
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

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

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_chunkIDs.length == 0) {
            return null;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("getLocal[chunkIDs(%d) ...]", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_GET.enter(p_chunkIDs.length);
        // #endif /* STATISTICS */

        ret = new Pair<>(0, new Chunk[p_chunkIDs.length]);

        try {
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
        } finally {
            m_memoryManager.unlockAccess();
        }

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
     * @return Local ChunkIDRanges
     */
    public ChunkIDRanges getAllLocalChunkIDRanges(final short p_nodeID) {
        ChunkIDRanges list;

        // check if remote node is a peer
        NodeRole role = m_boot.getNodeRole(p_nodeID);
        if (role == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Remote node 0x%X does not exist for get local chunk id ranges", p_nodeID);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_nodeID == m_boot.getNodeID()) {
            list = getAllLocalChunkIDRanges();
        } else {
            GetLocalChunkIDRangesRequest request = new GetLocalChunkIDRangesRequest(p_nodeID);

            // important: the remote operation involves traversing the whole CIDTable which takes a while...
            // If the node fails during that process, the superpeer notifies all peers about that
            // and the NetworkService takes care of handling running requests that can't succeed anymore
            request.setIgnoreTimeout(true);

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
     * Get all migrated chunk IDs of all stored chunks from a specific node.
     *
     * @param p_nodeID
     *     NodeID of the node to get the IDs from.
     * @return Ranges of migrated chunk IDs
     */
    public ChunkIDRanges getAllMigratedChunkIDRanges(final short p_nodeID) {
        ChunkIDRanges list;

        // check if remote node is a peer
        NodeRole role = m_boot.getNodeRole(p_nodeID);
        if (role == null) {
            // #if LOGGER >= ERROR
            LOGGER.error("Remote node 0x%X does not exist for get migrated chunk id ranges", p_nodeID);
            // #endif /* LOGGER >= ERROR */
            return null;
        }

        // #ifdef ASSERT_NODE_ROLE
        if (m_boot.getNodeRole() == NodeRole.SUPERPEER) {
            throw new InvalidNodeRoleException(m_boot.getNodeRole());
        }
        // #endif /* ASSERT_NODE_ROLE */

        if (p_nodeID == m_boot.getNodeID()) {
            list = getAllMigratedChunkIDRanges();
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
                        incomingRemoveMessage((RemoveMessage) p_message);
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

        if (p_engineEngineSettings.getRole() == NodeRole.PEER && m_backup.isActive()) {
            if (m_memoryManager.getStatus().getMaxChunkSize().getBytes() > m_backup.getLogSegmentSizeBytes()) {
                LOGGER.fatal("Backup is active and segment size (%d bytes) of log is smaller than max chunk size (%d bytes). Fix your configuration");
                throw new DXRAMRuntimeException("Backup is active and segment size of log is smaller than max chunk size. Fix your configuration");
            }
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
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REMOVE_REQUEST, RemoveMessage.class);
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
        m_network.register(RemoveMessage.class, this);
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

        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < chunks.length; i++) {
                // also does exist check
                int size = m_memoryManager.getSize(chunkIDs[i]);
                if (size < 0) {
                    // #if LOGGER >= WARN
                    LOGGER.warn("Getting chunk 0x%X failed, does not exist", chunkIDs[i]);
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
        } finally {
            m_memoryManager.unlockAccess();
        }

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

        Map<BackupRange, ArrayList<DataStructure>> remoteChunksByBackupRange = new TreeMap<>();

        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < chunks.length; i++) {
                if (!m_memoryManager.put(chunks[i])) {
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
                    BackupRange backupRange = m_backup.getBackupRange(chunks[i].getID());
                    ArrayList<DataStructure> remoteChunksOfBackupRange = remoteChunksByBackupRange.get(backupRange);
                    if (remoteChunksOfBackupRange == null) {
                        remoteChunksOfBackupRange = new ArrayList<>();
                        remoteChunksByBackupRange.put(backupRange, remoteChunksOfBackupRange);
                    }
                    remoteChunksOfBackupRange.add(chunks[i]);
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

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
            BackupRange backupRange;
            short[] backupPeers;
            DataStructure[] dataStructures;
            for (Entry<BackupRange, ArrayList<DataStructure>> entry : remoteChunksByBackupRange.entrySet()) {
                backupRange = entry.getKey();
                dataStructures = entry.getValue().toArray(new DataStructure[entry.getValue().size()]);

                backupPeers = backupRange.getBackupPeers();
                for (short backupPeer : backupPeers) {
                    if (backupPeer != NodeID.INVALID_ID) {
                        // #if LOGGER == TRACE
                        LOGGER.trace("Logging %d chunks to 0x%X", dataStructures.length, backupPeer);
                        // #endif /* LOGGER == TRACE */

                        try {
                            m_network.sendMessage(new LogMessage(backupPeer, backupRange.getRangeID(), dataStructures));
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
     * Handles an incoming RemoveMessage
     *
     * @param p_message
     *     the RemoveMessage
     */

    private void incomingRemoveMessage(final RemoveMessage p_message) {
        int size;

        // #ifdef STATISTICS
        SOP_INCOMING_REMOVE.enter(p_message.getChunkIDs().length);
        // #endif /* STATISTICS */

        long[] chunkIDs = p_message.getChunkIDs();

        Map<Long, ArrayListLong> remoteChunksByBackupPeers = new TreeMap<>();

        // this call is sending requests a is waiting for a response, thus
        // blocking the message handler thread handling the current message.
        // run this in a separate thread to avoid blocking the handler thread
        // (full application lock if using one message handler thread, only)
        // FIXME this causes an out of memory exception very quickly (unable to create new thread)
        //new Thread(() -> {
        // remove chunks from superpeer overlay first, so cannot be found before being deleted
        m_lookup.removeChunkIDs(ArrayListLong.wrap(chunkIDs));
        // }).start();

        for (int i = 0; i < chunkIDs.length; i++) {
            if (m_backup.isActive()) {
                // sort by backup peers
                long backupPeersAsLong = m_backup.getBackupPeersForLocalChunks(chunkIDs[i]);
                ArrayListLong remoteChunkIDsOfBackupPeers = remoteChunksByBackupPeers.get(backupPeersAsLong);
                if (remoteChunkIDsOfBackupPeers == null) {
                    remoteChunkIDsOfBackupPeers = new ArrayListLong();
                    remoteChunksByBackupPeers.put(backupPeersAsLong, remoteChunkIDsOfBackupPeers);
                }
                remoteChunkIDsOfBackupPeers.add(chunkIDs[i]);
            }
        }

        // remove chunks first (local)
        try {
            m_memoryManager.lockManage();
            for (int i = 0; i < chunkIDs.length; i++) {
                size = m_memoryManager.remove(chunkIDs[i], false);
                m_backup.deregisterChunk(chunkIDs[i], size);
                if (size == -1) {
                    // #if LOGGER >= ERROR
                    LOGGER.warn("Removing chunk 0x%X failed, does not exist", chunkIDs[i]);
                    // #endif /* LOGGER >= ERROR */
                }
            }
        } finally {
            m_memoryManager.unlockManage();
        }

        // TODO for migrated chunks, send remove request to peer currently holding the chunk data
        // for (int i = 0; i < chunkIDs.length; i++) {
        // byte rangeID = m_backup.getBackupRange(chunkIDs[i]);
        // short[] backupPeers = m_backup.getBackupPeersForLocalChunks(chunkIDs[i]);
        // m_backup.removeChunk(chunkIDs[i]);
        //
        // if (m_memoryManager.dataWasMigrated(chunkIDs[i])) {
        // // Inform peer who got the migrated data about removal
        // RemoveMessage request = new RemoveMessage(ChunkID.getCreatorID(chunkIDs[i]), new Chunk(chunkIDs[i], 0));
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
            ArrayListLong ids;
            for (Entry<Long, ArrayListLong> entry : remoteChunksByBackupPeers.entrySet()) {
                backupPeersAsLong = entry.getKey();
                ids = entry.getValue();

                backupPeers = BackupRange.convert(backupPeersAsLong);
                for (int i = 0; i < backupPeers.length; i++) {
                    if (backupPeers[i] != m_boot.getNodeID() && backupPeers[i] != NodeID.INVALID_ID) {
                        try {
                            m_network.sendMessage(new de.hhu.bsinfo.dxram.log.messages.RemoveMessage(backupPeers[i], ids));
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
            try {
                m_memoryManager.lockManage();
                chunkIDs[0] = m_memoryManager.create(sizes[0]);

                // Initialize a new backup range every e.g. 256 MB and inform superpeer
                // Must be locked together with create call to memory manager
                m_backup.registerChunk(chunkIDs[0], sizes[0]);
            } finally {
                m_memoryManager.unlockManage();
            }
        } else {
            try {
                m_memoryManager.lockManage();
                chunkIDs = m_memoryManager.createMultiSizes(sizes);

                // Initialize a new backup range every e.g. 256 MB and inform superpeer
                // Must be locked together with create call to memory manager
                m_backup.registerChunks(chunkIDs, sizes);
            } finally {
                m_memoryManager.unlockManage();
            }

            if (chunkIDs == null) {
                // #if LOGGER >= ERROR
                LOGGER.error("Multi create chunks failed");
                // #endif /* LOGGER >= ERROR */

                chunkIDs = new long[sizes.length];
                for (int i = 0; i < chunkIDs.length; i++) {
                    chunkIDs[i] = ChunkID.INVALID_ID;
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
        MemoryManagerComponent.Status status = getStatus();

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
        ChunkIDRanges cidRangesLocalChunks;

        try {
            m_memoryManager.lockAccess();
            cidRangesLocalChunks = m_memoryManager.getCIDRangesOfAllLocalChunks();
        } finally {
            m_memoryManager.unlockAccess();
        }

        if (cidRangesLocalChunks == null) {
            cidRangesLocalChunks = new ChunkIDRanges();
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
        ChunkIDRanges cidRangesMigratedChunks;

        try {
            m_memoryManager.lockAccess();
            cidRangesMigratedChunks = m_memoryManager.getCIDRangesOfAllMigratedChunks();
        } finally {
            m_memoryManager.unlockAccess();
        }

        if (cidRangesMigratedChunks == null) {
            cidRangesMigratedChunks = new ChunkIDRanges();
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
}

/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
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
import de.hhu.bsinfo.dxram.chunk.messages.StatusRequest;
import de.hhu.bsinfo.dxram.chunk.messages.StatusResponse;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.dxram.data.DSByteArray;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMRuntimeException;
import de.hhu.bsinfo.dxram.lock.AbstractLockComponent;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupState;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.NodeID;

/**
 * This service provides access to the backend storage system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class ChunkService extends AbstractDXRAMService<ChunkServiceConfig> implements MessageReceiver {
    // statistics recording
    private static final StatisticsOperation SOP_CREATE = StatisticsRecorderManager.getOperation(ChunkService.class, "Create");
    private static final StatisticsOperation SOP_REMOTE_CREATE = StatisticsRecorderManager.getOperation(ChunkService.class, "RemoteCreate");
    private static final StatisticsOperation SOP_GET = StatisticsRecorderManager.getOperation(ChunkService.class, "Get");
    private static final StatisticsOperation SOP_PUT = StatisticsRecorderManager.getOperation(ChunkService.class, "Put");
    private static final StatisticsOperation SOP_INCOMING_CREATE = StatisticsRecorderManager.getOperation(ChunkService.class, "IncomingCreate");
    private static final StatisticsOperation SOP_INCOMING_GET = StatisticsRecorderManager.getOperation(ChunkService.class, "IncomingGet");
    private static final StatisticsOperation SOP_INCOMING_PUT = StatisticsRecorderManager.getOperation(ChunkService.class, "IncomingPut");

    // component dependencies
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
        super("chunk", ChunkServiceConfig.class);
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

        try {
            m_memoryManager.lockAccess();
            list = m_memoryManager.getCIDRangesOfAllLocalChunks();
        } finally {
            m_memoryManager.unlockAccess();
        }

        return list;
    }

    /**
     * Get the local memory status
     *
     * @return Status object with current status of the key value store memory
     */
    public MemoryManagerComponent.Status getStatus() {
        return m_memoryManager.getStatus();
    }

    /**
     * Get the memory status of a remote node specified by a node id.
     *
     * @param p_nodeID
     *         Node id to get the status from.
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
     *         Size of the new chunk.
     * @param p_count
     *         Number of chunks to create with the specified size.
     * @return ChunkIDs/Handles identifying the created chunks.
     */
    public long[] create(final int p_size, final int p_count) {
        return create(p_size, p_count, false);
    }

    /**
     * Create new chunks.
     *
     * @param p_size
     *         Size of the new chunk.
     * @param p_count
     *         Number of chunks to create with the specified size.
     * @param p_consecutive
     *         Whether the ChunkIDs must be consecutive or not.
     * @return ChunkIDs/Handles identifying the created chunks.
     */
    public long[] create(final int p_size, final int p_count, final boolean p_consecutive) {
        long[] chunkIDs = null;

        assert p_size > 0 && p_count > 0;

        // #if LOGGER == TRACE
        LOGGER.trace("create[size %d, count %d, consecutive %d]", p_size, p_count, p_consecutive);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_CREATE.enter(p_count);
        // #endif /* STATISTICS */

        if (p_count == 1) {
            long chunkId;
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

        // #ifdef STATISTICS
        SOP_CREATE.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("create[size %d, count %d, consecutive %d] -> %s, ...", p_size, p_count, p_consecutive, ChunkID.toHexString(chunkIDs[0]));
        // #endif /* LOGGER == TRACE */

        return chunkIDs;
    }

    /**
     * Create new chunks according to the data structures provided.
     * Important: This does NOT put/write the contents of the data structure provided.
     * It creates chunks with the sizes of the data structures and sets the IDs.
     *
     * @param p_dataStructures
     *         Data structures to create chunks for.
     * @return Number of successfully created chunks.
     */
    public void create(final DataStructure... p_dataStructures) {
        create(false, p_dataStructures);
    }

    /**
     * Create new chunks according to the data structures provided.
     * Important: This does NOT put/write the contents of the data structure provided.
     * It creates chunks with the sizes of the data structures and sets the IDs.
     *
     * @param p_consecutive
     *         Whether the ChunkIDs must be consecutive or not.
     * @param p_dataStructures
     *         Data structures to create chunks for.
     */
    public void create(final boolean p_consecutive, final DataStructure... p_dataStructures) {
        if (p_dataStructures.length == 0) {
            return;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("create[p_consecutive %d, numDataStructures %d...]", p_consecutive, p_dataStructures.length);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        SOP_CREATE.enter(p_dataStructures.length);
        // #endif /* STATISTICS */

        if (p_dataStructures.length == 1) {
            long chunkID;
            try {
                m_memoryManager.lockManage();
                chunkID = m_memoryManager.create(p_dataStructures[0].sizeofObject());

                // Initialize a new backup range every e.g. 256 MB and inform superpeer
                // Must be locked together with create call to memory manager
                m_backup.registerChunk(chunkID, p_dataStructures[0].sizeofObject());
            } finally {
                m_memoryManager.unlockManage();
            }

            p_dataStructures[0].setID(chunkID);
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
        }

        // #ifdef STATISTICS
        SOP_CREATE.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("create[numDataStructures(%d)]", p_dataStructures.length);
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Create chunks with different sizes.
     *
     * @param p_sizes
     *         List of sizes to create chunks for.
     * @return ChunkIDs/Handles identifying the created chunks.
     */
    public long[] createSizes(final int... p_sizes) {
        return createSizes(false, p_sizes);
    }

    /**
     * Create chunks with different sizes.
     *
     * @param p_sizes
     *         List of sizes to create chunks for.
     * @param p_consecutive
     *         Whether the ChunkIDs must be consecutive or not.
     * @return ChunkIDs/Handles identifying the created chunks.
     */
    public long[] createSizes(final boolean p_consecutive, final int... p_sizes) {
        long[] chunkIDs;

        if (p_sizes.length == 0) {
            return new long[0];
        }

        // #if LOGGER == TRACE
        LOGGER.trace("create[consecutive(%d), sizes(%d) %d, ...]", p_consecutive, p_sizes.length, p_sizes[0]);
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
        }

        // #ifdef STATISTICS
        SOP_CREATE.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("create[consecutive(%d), sizes(%d) %d, ...] -> %s, ...", p_consecutive, p_sizes.length, p_sizes[0], ChunkID.toHexString(chunkIDs[0]));
        // #endif /* LOGGER == TRACE */

        return chunkIDs;
    }

    /**
     * Create chunks on another node.
     *
     * @param p_peer
     *         NodeID of the peer to create the chunks on.
     * @param p_dataStructures
     *         Data structures to create chunks for.
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
                p_dataStructures[i].setState(ChunkState.OK);
            }
        }

        return count;
    }

    /**
     * Create chunks on another node.
     *
     * @param p_peer
     *         NodeID of the peer to create the chunks on.
     * @param p_sizes
     *         Sizes to create chunks of.
     * @return ChunkIDs/Handles identifying the created chunks. Null if remote is unreachable
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
     * Put/Update the contents of the key-value memory with the data of the provided chunks.
     *
     * @param p_chunks
     *         Chunks to put/update. Null values are ignored.
     * @return Number of successfully updated data structures.
     */
    public int put(final DataStructure... p_chunks) {
        return put(ChunkLockOperation.NO_LOCK_OPERATION, p_chunks);
    }

    /**
     * Put/Update the contents of the key-value memory with the data of the provided chunks.
     *
     * @param p_chunkUnlockOperation
     *         Unlock operation to execute right after the put operation.
     * @param p_chunks
     *         Chunks to put/update. Null values or chunks with invalid IDs are ignored.
     * @return Number of successfully updated data structures.
     */
    public int put(final ChunkLockOperation p_chunkUnlockOperation, final DataStructure... p_chunks) {
        return put(p_chunkUnlockOperation, p_chunks, 0, p_chunks.length);
    }

    /**
     * Put/Update the contents of the provided data structures in the backend storage.
     *
     * @param p_chunkUnlockOperation
     *         Unlock operation to execute right after the put operation.
     * @param p_chunks
     *         Chunks to put/update. Null values or chunks with invalid IDs are ignored.
     * @param p_offset
     *         Start offset within the array.
     * @param p_count
     *         Number of items to put.
     * @return Number of successfully updated data structures.
     */
    public int put(final ChunkLockOperation p_chunkUnlockOperation, final DataStructure[] p_chunks, final int p_offset, final int p_count) {
        int totalChunksPut = 0;

        if (p_chunks.length == 0) {
            return totalChunksPut;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("put[unlockOp %s, dataStructures(%d) ...]", p_chunkUnlockOperation, p_chunks.length);
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
                if (p_chunks[i + p_offset] == null || p_chunks[i + p_offset].getID() == ChunkID.INVALID_ID) {
                    continue;
                }

                // try to put every chunk locally, returns false if it does not exist
                // and saves us an additional check
                if (m_memoryManager.put(p_chunks[i + p_offset])) {
                    totalChunksPut++;
                    p_chunks[i + p_offset].setState(ChunkState.OK);

                    // unlock chunk as well
                    if (p_chunkUnlockOperation != ChunkLockOperation.NO_LOCK_OPERATION) {
                        boolean writeLock = false;
                        if (p_chunkUnlockOperation == ChunkLockOperation.WRITE_LOCK) {
                            writeLock = true;
                        }
                        m_lock.unlock(p_chunks[i + p_offset].getID(), m_boot.getNodeID(), writeLock);
                    }

                    if (m_backup.isActive()) {
                        // sort by backup peers
                        BackupRange backupRange = m_backup.getBackupRange(p_chunks[i + p_offset].getID());
                        ArrayList<DataStructure> remoteChunksOfBackupRange = remoteChunksByBackupRange.computeIfAbsent(backupRange, a -> new ArrayList<>());
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
                        short peer = location.getPrimaryPeer();

                        ArrayList<DataStructure> remoteChunksOfPeer = remoteChunksByPeers.computeIfAbsent(peer, a -> new ArrayList<>());
                        remoteChunksOfPeer.add(p_chunks[i + p_offset]);
                    } else if (location.getState() == LookupState.DOES_NOT_EXIST) {
                        p_chunks[i + p_offset].setState(ChunkState.DOES_NOT_EXIST);
                    } else if (location.getState() == LookupState.DATA_LOST) {
                        p_chunks[i + p_offset].setState(ChunkState.DATA_LOST);
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
                    for (DataStructure dataStructure : entry.getValue()) {
                        if (m_memoryManager.put(dataStructure)) {
                            totalChunksPut++;
                        }
                        // else: put failed, state for chunk set by memory manager
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
                    if (m_backup.isActive()) {
                        for (DataStructure ds : chunksToPut) {
                            ds.setState(ChunkState.DATA_TEMPORARY_UNAVAILABLE);
                            m_lookup.invalidate(ds.getID());
                        }
                    } else {
                        for (DataStructure ds : chunksToPut) {
                            ds.setState(ChunkState.DATA_LOST);
                            m_lookup.invalidate(ds.getID());
                        }
                    }
                    continue;
                }

                PutResponse response = request.getResponse(PutResponse.class);

                byte[] statusCodes = response.getStatusCodes();
                // try short cut, i.e. all puts successful
                if (statusCodes.length == 1 && statusCodes[0] == ChunkState.OK.ordinal()) {
                    totalChunksPut += chunksToPut.size();

                    for (DataStructure ds : chunksToPut) {
                        ds.setState(ChunkState.OK);
                    }
                } else {
                    for (int i = 0; i < statusCodes.length; i++) {
                        chunksToPut.get(i).setState(ChunkState.values()[statusCodes[i]]);
                        if (statusCodes[i] == ChunkState.OK.ordinal()) {
                            totalChunksPut++;
                        } else {
                            m_lookup.invalidateRange(chunksToPut.get(i).getID());
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
        LOGGER.trace("put[unlockOp %s, dataStructures(%d) ...] -> %d", p_chunkUnlockOperation, p_chunks.length, totalChunksPut);
        // #endif /* LOGGER == TRACE */

        return totalChunksPut;
    }

    /**
     * Get/Read the data stored in the backend storage into the provided chunk objects.
     *
     * @param p_chunks
     *         Chunks to read the stored data into. Null values or invalid IDs are ignored.
     * @return Number of successfully read data structures.
     */
    public int get(final DataStructure... p_chunks) {
        return get(p_chunks, 0, p_chunks.length);
    }

    /**
     * Get/Read the data stored in the backend storage into the provided chunk objects.
     *
     * @param p_chunks
     *         Array with chunk objects to read the stored data to. Null values or invalid IDs are ignored.
     * @param p_offset
     *         Start offset within the array.
     * @param p_count
     *         Number of elements to read.
     * @return Number of successfully read data structures.
     */
    public int get(final DataStructure[] p_chunks, final int p_offset, final int p_count) {
        int totalChunksGot = 0;

        assert p_offset >= 0 && p_count >= 0;

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
                if (p_chunks[i + p_offset] == null) {
                    continue;
                }

                if (p_chunks[i + p_offset].getID() == ChunkID.INVALID_ID) {
                    p_chunks[i + p_offset].setState(ChunkState.INVALID_ID);
                }

                // try to get locally, will check first if it exists
                if (m_memoryManager.get(p_chunks[i + p_offset])) {
                    totalChunksGot++;
                    p_chunks[i + p_offset].setState(ChunkState.OK);
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
                        short peer = location.getPrimaryPeer();

                        ArrayList<DataStructure> remoteChunksOfPeer = remoteChunksByPeers.computeIfAbsent(peer, a -> new ArrayList<>());
                        remoteChunksOfPeer.add(p_chunks[i + p_offset]);
                    } else if (location.getState() == LookupState.DOES_NOT_EXIST) {
                        p_chunks[i + p_offset].setState(ChunkState.DOES_NOT_EXIST);
                    } else if (location.getState() == LookupState.DATA_LOST) {
                        p_chunks[i + p_offset].setState(ChunkState.DATA_LOST);
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
                    for (DataStructure chunk : remoteChunks) {
                        if (m_memoryManager.get(chunk)) {
                            totalChunksGot++;
                            chunk.setState(ChunkState.OK);
                        } else {
                            chunk.setState(ChunkState.DOES_NOT_EXIST);
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
                    if (m_backup.isActive()) {
                        for (DataStructure chunk : remoteChunks) {
                            chunk.setState(ChunkState.DATA_TEMPORARY_UNAVAILABLE);
                            m_lookup.invalidate(chunk.getID());
                        }
                    } else {
                        for (DataStructure chunk : remoteChunks) {
                            chunk.setState(ChunkState.DATA_LOST);
                            m_lookup.invalidate(chunk.getID());
                        }
                    }
                    continue;
                }

                GetResponse response = request.getResponse(GetResponse.class);
                totalChunksGot += response.getTotalSuccessful();

                if (response.getTotalSuccessful() != remoteChunks.size()) {
                    for (DataStructure chunk : remoteChunks) {
                        if (chunk.getState() != ChunkState.OK) {
                            m_lookup.invalidateRange(chunk.getID());
                        }
                    }
                }

                // Chunk data is written directly to the provided data structure on receive
            }
        }



        // #ifdef STATISTICS
        SOP_GET.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("get[dataStructures(%d) ...] -> %d", p_chunks.length, totalChunksGot);
        // #endif /* LOGGER == TRACE */

        return totalChunksGot;
    }

    /**
     * Special local only get version. Use this if you already delegate tasks with non local
     * chunks to the remote owning them. This speeds up access to local only chunks a lot.
     * Get/Read the data stored in the backend storage into the provided chunks.
     *
     * @param p_chunks
     *         Chunks to read the stored data into. Null values or invalid IDs are ignored.
     * @return Number of successfully read data structures.
     */
    public int getLocal(final DataStructure... p_chunks) {
        return getLocal(p_chunks, 0, p_chunks.length);
    }

    /**
     * Special local only get version. Use this if you already delegate tasks with non local
     * chunks to the remote owning them. This speeds up access to local only chunks a lot.
     * Get/Read the data stored in the backend storage into the provided chunks.
     *
     * @param p_chunks
     *         Array with chunks to read the stored data to. Null values or invalid IDs are ignored.
     * @param p_offset
     *         Start offset within the array.
     * @param p_count
     *         Number of elements to read.
     * @return Number of successfully read data structures.
     */
    public int getLocal(final DataStructure[] p_chunks, final int p_offset, final int p_count) {
        int totalChunksGot = 0;

        assert p_offset >= 0 && p_count >= 0;

        if (p_chunks.length == 0) {
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
                if (p_chunks[i + p_offset] == null) {
                    continue;
                }

                if (p_chunks[i + p_offset].getID() == ChunkID.INVALID_ID) {
                    p_chunks[i + p_offset].setState(ChunkState.INVALID_ID);
                }

                // try to get locally, will check first if it exists and
                // returns false if it doesn't exist
                if (m_memoryManager.get(p_chunks[i + p_offset])) {
                    totalChunksGot++;
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

        // #ifdef STATISTICS
        SOP_GET.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        LOGGER.trace("getLocal[dataStructures(%d) ...] -> %d", p_chunks.length, totalChunksGot);
        // #endif /* LOGGER == TRACE */

        return totalChunksGot;
    }

    /**
     * Get all chunk ID ranges of all stored chunks from a specific node.
     * This does not include migrated chunks.
     *
     * @param p_nodeID
     *         NodeID of the node to get the ranges from.
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
     *         NodeID of the node to get the IDs from.
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
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
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
    protected boolean startService(final DXRAMContext.Config p_config) {
        registerNetworkMessages();
        registerNetworkMessageListener();

        if (m_backup.isActiveAndAvailableForBackup()) {
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
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_REQUEST, this);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_REQUEST, this);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_CREATE_REQUEST, this);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_STATUS_REQUEST, this);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST, this);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_REQUEST, this);
    }

    // -----------------------------------------------------------------------------------

    /**
     * Handles an incoming GetRequest
     *
     * @param p_request
     *         the GetRequest
     */
    private void incomingGetRequest(final GetRequest p_request) {
        long[] chunkIDs = p_request.getChunkIDs();
        byte[][] data = new byte[chunkIDs.length][];
        int numChunksGot = 0;

        // #ifdef STATISTICS
        SOP_INCOMING_GET.enter(p_request.getChunkIDs().length);
        // #endif /* STATISTICS */

        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < data.length; i++) {
                // also does exist check
                data[i] = m_memoryManager.get(chunkIDs[i]);

                if (data[i] != null) {
                    numChunksGot++;
                }
            }
        } finally {
            m_memoryManager.unlockAccess();
        }

        GetResponse response = new GetResponse(p_request, data, numChunksGot);

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
     *         the PutRequest
     */
    private void incomingPutRequest(final PutRequest p_request) {
        long[] chunkIDs = p_request.getChunkIDs();
        byte[][] data = p_request.getChunkData();

        byte[] statusChunks = new byte[chunkIDs.length];
        boolean allSuccessful = true;

        // #ifdef STATISTICS
        SOP_INCOMING_PUT.enter(chunkIDs.length);
        // #endif /* STATISTICS */

        Map<BackupRange, ArrayList<DataStructure>> remoteChunksByBackupRange = new TreeMap<>();

        try {
            m_memoryManager.lockAccess();
            for (int i = 0; i < chunkIDs.length; i++) {
                if (!m_memoryManager.put(chunkIDs[i], data[i])) {
                    // does not exist (anymore)
                    statusChunks[i] = (byte) ChunkState.DOES_NOT_EXIST.ordinal();

                    allSuccessful = false;
                } else {
                    // put successful
                    statusChunks[i] = (byte) ChunkState.OK.ordinal();
                }

                if (m_backup.isActive()) {
                    // sort by backup peers
                    BackupRange backupRange = m_backup.getBackupRange(chunkIDs[i]);
                    ArrayList<DataStructure> remoteChunksOfBackupRange = remoteChunksByBackupRange.computeIfAbsent(backupRange, k -> new ArrayList<>());
                    remoteChunksOfBackupRange.add(new DSByteArray(chunkIDs[i], data[i]));
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

            for (long chunkID : chunkIDs) {
                m_lock.unlock(chunkID, m_boot.getNodeID(), writeLock);
            }
        }

        PutResponse response;
        // cut message length if all were successful
        if (allSuccessful) {
            response = new PutResponse(p_request, (byte) ChunkState.OK.ordinal());
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
     * Handle incoming create requests.
     *
     * @param p_request
     *         Request to handle
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
     *         Request to handle
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
     *         Request to handle
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
     *         Request to handle
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

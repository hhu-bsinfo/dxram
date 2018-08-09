/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.chunk;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.PutMessage;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.ChunkState;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.lock.AbstractLockComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupState;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.stats.StatisticsManager;
import de.hhu.bsinfo.dxutils.stats.ThroughputPool;
import de.hhu.bsinfo.dxutils.stats.Value;

/**
 * This service provides access to the backend storage system.
 * It does not replace the normal ChunkService, but extends it capabilities with async operations.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.02.2016
 */
public class ChunkAsyncService extends AbstractDXRAMService<ChunkAsyncServiceConfig> implements MessageReceiver {
    private static final ThroughputPool SOP_PUT_ASYNC = new ThroughputPool(ChunkAnonService.class, "PutAsync",
            Value.Base.B_10);
    private static final ThroughputPool SOP_INCOMING_PUT_ASYNC = new ThroughputPool(ChunkAnonService.class,
            "IncomingPutAsync", Value.Base.B_10);

    static {
        StatisticsManager.get().registerOperation(ChunkAsyncService.class, SOP_PUT_ASYNC);
        StatisticsManager.get().registerOperation(ChunkAsyncService.class, SOP_INCOMING_PUT_ASYNC);
    }

    // component dependencies
    private AbstractBootComponent m_boot;
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;
    private LookupComponent m_lookup;
    private AbstractLockComponent m_lock;

    /**
     * Constructor
     */
    public ChunkAsyncService() {
        super("achunk", ChunkAsyncServiceConfig.class);
    }

    /**
     * Put/Update the contents of the provided data structures in the backend storage.
     *
     * @param p_dataStructres
     *         Data structures to put/update.
     */
    public void put(final DataStructure... p_dataStructres) {
        put(ChunkLockOperation.NO_LOCK_OPERATION, p_dataStructres);
    }

    /**
     * Put/Update the contents of the provided data structures in the backend storage.
     *
     * @param p_chunkUnlockOperation
     *         Unlock operation to execute right after the put operation.
     * @param p_dataStructures
     *         Data structures to put/update.
     */
    public int put(final ChunkLockOperation p_chunkUnlockOperation, final DataStructure... p_dataStructures) {
        int chunksPut = 0;

        if (p_dataStructures.length == 0) {
            return chunksPut;
        }

        if (p_dataStructures[0] == null) {

            LOGGER.trace("put[unlockOp %s, dataStructures(%d) ...]", p_chunkUnlockOperation, p_dataStructures.length);

        } else {

            LOGGER.trace("put[unlockOp %s, dataStructures(%d) %s, ...]", p_chunkUnlockOperation,
                    p_dataStructures.length, ChunkID.toHexString(p_dataStructures[0].getID()));

        }

        // #ifdef STATISTICS
        SOP_PUT_ASYNC.start(p_dataStructures.length);
        // #endif /* STATISTICS */

        Map<Short, ArrayList<DataStructure>> remoteChunksByPeers = new TreeMap<>();

        // sort by local/remote chunks
        m_memoryManager.lockAccess();
        for (DataStructure dataStructure : p_dataStructures) {
            // allowing nulls -> filter
            if (dataStructure == null) {
                continue;
            }

            if (m_memoryManager.put(dataStructure)) {
                chunksPut++;

                // unlock chunks
                if (p_chunkUnlockOperation != ChunkLockOperation.NO_LOCK_OPERATION) {
                    boolean writeLock = false;
                    if (p_chunkUnlockOperation == ChunkLockOperation.WRITE_LOCK) {
                        writeLock = true;
                    }

                    m_lock.unlock(dataStructure.getID(), m_boot.getNodeId(), writeLock);
                }
            } else {
                // remote or migrated, figure out location and sort by peers
                LookupRange location = m_lookup.getLookupRange(dataStructure.getID());
                while (location.getState() == LookupState.DATA_TEMPORARY_UNAVAILABLE) {
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException ignore) {
                    }
                    location = m_lookup.getLookupRange(dataStructure.getID());
                }

                if (location.getState() == LookupState.OK) {
                    // currently undefined because we still have to get it from remote
                    dataStructure.setState(ChunkState.UNDEFINED);
                    short peer = location.getPrimaryPeer();

                    ArrayList<DataStructure> remoteChunksOfPeer =
                            remoteChunksByPeers.computeIfAbsent(peer, a -> new ArrayList<>());
                    remoteChunksOfPeer.add(dataStructure);
                } else if (location.getState() == LookupState.DOES_NOT_EXIST) {
                    dataStructure.setState(ChunkState.DOES_NOT_EXIST);
                } else if (location.getState() == LookupState.DATA_LOST) {
                    dataStructure.setState(ChunkState.DATA_LOST);
                }
            }
        }

        m_memoryManager.unlockAccess();

        // go for remote chunks
        for (Entry<Short, ArrayList<DataStructure>> entry : remoteChunksByPeers.entrySet()) {
            short peer = entry.getKey();

            if (peer == m_boot.getNodeId()) {
                // local put, migrated data to current node
                m_memoryManager.lockAccess();
                for (final DataStructure dataStructure : entry.getValue()) {
                    if (!m_memoryManager.put(dataStructure)) {

                        LOGGER.error("Putting local chunk 0x%X failed, does not exist", dataStructure.getID());

                    }
                }
                m_memoryManager.unlockAccess();
            } else {
                // Remote put
                ArrayList<DataStructure> chunksToPut = entry.getValue();
                PutMessage message = new PutMessage(peer, p_chunkUnlockOperation,
                        chunksToPut.toArray(new DataStructure[chunksToPut.size()]));
                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {

                    LOGGER.error("Sending chunk put message to peer %s failed: %s", NodeID.toHexString(peer), e);

                    for (DataStructure ds : chunksToPut) {
                        m_lookup.invalidate(ds.getID());
                    }

                    continue;
                }

                chunksPut += chunksToPut.size();
            }
        }

        // #ifdef STATISTICS
        SOP_PUT_ASYNC.stop();
        // #endif /* STATISTICS */

        if (p_dataStructures[0] == null) {

            LOGGER.trace("put[unlockOp %s, dataStructures(%d) ...]", p_chunkUnlockOperation, p_dataStructures.length);

        } else {

            LOGGER.trace("put[unlockOp %s, dataStructures(%d) %s, ...]", p_chunkUnlockOperation,
                    p_dataStructures.length, ChunkID.toHexString(p_dataStructures[0].getID()));

        }

        return chunksPut;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {

        LOGGER.trace("Entering incomingMessage with: p_message=%s", p_message);

        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case ChunkMessages.SUBTYPE_PUT_MESSAGE:
                        incomingPutMessage((PutMessage) p_message);
                        break;
                    default:
                        break;
                }
            }
        }

        LOGGER.trace("Exiting incomingMessage");

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
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_lock = p_componentAccessor.getComponent(AbstractLockComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        registerNetworkMessages();
        registerNetworkMessageListener();

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
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_MESSAGE,
                PutMessage.class);
    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_MESSAGE, this);
    }

    // -----------------------------------------------------------------------------------

    /**
     * Handles an incoming PutRequest
     *
     * @param p_request
     *         the PutRequest
     */
    private void incomingPutMessage(final PutMessage p_request) {
        long[] chunkIDs = p_request.getChunkIDs();
        byte[][] data = p_request.getChunkData();

        // #ifdef STATISTICS
        SOP_INCOMING_PUT_ASYNC.start(chunkIDs.length);
        // #endif /* STATISTICS */

        m_memoryManager.lockAccess();
        try {
            for (int i = 0; i < chunkIDs.length; i++) {

                if (!m_memoryManager.put(chunkIDs[i], data[i])) {
                    LOGGER.error("Putting chunk 0x%X failed, does not exist", chunkIDs[i]);
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

            for (int i = 0; i < chunkIDs.length; i++) {
                m_lock.unlock(chunkIDs[i], m_boot.getNodeId(), writeLock);
            }
        }

        // #ifdef STATISTICS
        SOP_INCOMING_PUT_ASYNC.stop();
        // #endif /* STATISTICS */
    }
}

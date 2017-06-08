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
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.PutMessage;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.lock.AbstractLockComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.NodeID;

/**
 * This service provides access to the backend storage system.
 * It does not replace the normal ChunkService, but extends it capabilities with async operations.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.02.2016
 */
public class ChunkAsyncService extends AbstractDXRAMService<ChunkAsyncServiceConfig> implements MessageReceiver {
    // statistics recording
    private static final StatisticsOperation SOP_PUT_ASYNC = StatisticsRecorderManager.getOperation(ChunkAsyncService.class, "PutAsync");
    private static final StatisticsOperation SOP_INCOMING_PUT_ASYNC = StatisticsRecorderManager.getOperation(ChunkAsyncService.class, "IncomingPutAsync");

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
            // #if LOGGER == TRACE
            LOGGER.trace("put[unlockOp %s, dataStructures(%d) ...]", p_chunkUnlockOperation, p_dataStructures.length);
            // #endif /* LOGGER == TRACE */
        } else {
            // #if LOGGER == TRACE
            LOGGER.trace("put[unlockOp %s, dataStructures(%d) %s, ...]", p_chunkUnlockOperation, p_dataStructures.length,
                    ChunkID.toHexString(p_dataStructures[0].getID()));
            // #endif /* LOGGER == TRACE */
        }

        // #ifdef STATISTICS
        SOP_PUT_ASYNC.enter(p_dataStructures.length);
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

                    m_lock.unlock(dataStructure.getID(), m_boot.getNodeID(), writeLock);
                }
            } else {
                // remote or migrated, figure out location and sort by peers
                LookupRange lookupRange = m_lookup.getLookupRange(dataStructure.getID());
                if (lookupRange == null) {
                    continue;
                }

                short peer = lookupRange.getPrimaryPeer();

                ArrayList<DataStructure> remoteChunksOfPeer = remoteChunksByPeers.computeIfAbsent(peer, a -> new ArrayList<>());
                remoteChunksOfPeer.add(dataStructure);
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
                    if (!m_memoryManager.put(dataStructure)) {
                        // #if LOGGER >= ERROR
                        LOGGER.error("Putting local chunk 0x%X failed, does not exist", dataStructure.getID());
                        // #endif /* LOGGER >= ERROR */
                    }
                }
                m_memoryManager.unlockAccess();
            } else {
                // Remote put
                ArrayList<DataStructure> chunksToPut = entry.getValue();
                PutMessage message = new PutMessage(peer, p_chunkUnlockOperation, chunksToPut.toArray(new DataStructure[chunksToPut.size()]));
                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {
                    // #if LOGGER >= ERROR
                    LOGGER.error("Sending chunk put message to peer %s failed: %s", NodeID.toHexString(peer), e);
                    // #endif /* LOGGER >= ERROR */

                    for (DataStructure ds : chunksToPut) {
                        m_lookup.invalidate(ds.getID());
                    }

                    continue;
                }

                chunksPut += chunksToPut.size();
            }
        }

        // #ifdef STATISTICS
        SOP_PUT_ASYNC.leave();
        // #endif /* STATISTICS */

        if (p_dataStructures[0] == null) {
            // #if LOGGER == TRACE
            LOGGER.trace("put[unlockOp %s, dataStructures(%d) ...]", p_chunkUnlockOperation, p_dataStructures.length);
            // #endif /* LOGGER == TRACE */
        } else {
            // #if LOGGER == TRACE
            LOGGER.trace("put[unlockOp %s, dataStructures(%d) %s, ...]", p_chunkUnlockOperation, p_dataStructures.length,
                    ChunkID.toHexString(p_dataStructures[0].getID()));
            // #endif /* LOGGER == TRACE */
        }

        return chunksPut;
    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {
        // #if LOGGER == TRACE
        LOGGER.trace("Entering incomingMessage with: p_message=%s", p_message);
        // #endif /* LOGGER == TRACE */

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
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_MESSAGE, PutMessage.class);
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
        SOP_INCOMING_PUT_ASYNC.enter(chunkIDs.length);
        // #endif /* STATISTICS */

        m_memoryManager.lockAccess();
        try {
            for (int i = 0; i < chunkIDs.length; i++) {
                // #if LOGGER >= WARN
                if (!m_memoryManager.put(chunkIDs[i], data[i])) {
                    LOGGER.error("Putting chunk 0x%X failed, does not exist", chunkIDs[i]);
                }
                // #endif /* LOGGER >= WARN */
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
                m_lock.unlock(chunkIDs[i], m_boot.getNodeID(), writeLock);
            }
        }

        // #ifdef STATISTICS
        SOP_INCOMING_PUT_ASYNC.leave();
        // #endif /* STATISTICS */
    }
}

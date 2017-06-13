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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.DumpMemoryMessage;
import de.hhu.bsinfo.dxram.chunk.messages.ResetMemoryMessage;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.net.MessageReceiver;
import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.NetworkException;

/**
 * Special and separate service for debug/benchmark only ChunkService related calls
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2017
 */
public class ChunkDebugService extends AbstractDXRAMService<ChunkDebugServiceConfig> implements MessageReceiver {
    private MemoryManagerComponent m_memoryManager;
    private NetworkComponent m_network;
    private NameserviceComponent m_nameservice;

    /**
     * Constructor
     */
    protected ChunkDebugService() {
        super("chunkdebug", ChunkDebugServiceConfig.class);
    }

    /**
     * Dump the chunk memory to a file
     *
     * @param p_fileName
     *         File to dump memory to
     */
    public void dumpChunkMemory(final String p_fileName) {
        // #if LOGGER >= INFO
        LOGGER.info("Dumping chunk memory to %s, wait", p_fileName);
        // #endif /* LOGGER >= INFO */

        try {
            m_memoryManager.lockManage();

            // #if LOGGER >= INFO
            LOGGER.info("Dumping chunk memory to %s...", p_fileName);
            // #endif /* LOGGER >= INFO */

            m_memoryManager.dumpMemory(p_fileName);
        } finally {
            m_memoryManager.unlockManage();
        }

        // #if LOGGER >= INFO
        LOGGER.info("Dumping chunk memory to %s, done", p_fileName);
        // #endif /* LOGGER >= INFO */
    }

    /**
     * Dump the chunk memory of a remote peer to a file
     *
     * @param p_fileName
     *         File to dump memory to
     * @return True if dumping memory of remote peer successful, false on failure
     */
    public boolean dumpChunkMemory(final String p_fileName, final short p_remoteNodeId) {
        // #if LOGGER >= INFO
        LOGGER.info("Dumping remote chunk memory of 0x%X to %s...", p_remoteNodeId, p_fileName);
        // #endif /* LOGGER >= INFO */

        DumpMemoryMessage message = new DumpMemoryMessage(p_remoteNodeId, p_fileName);

        try {
            m_network.sendMessage(message);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending request to dump memory of node 0x%X failed: %s", p_remoteNodeId, e);
            // #endif /* LOGGER >= ERROR */
            return false;
        }

        // #if LOGGER >= INFO
        LOGGER.info("Triggered async chunk memory dump to %s", p_fileName);
        // #endif /* LOGGER >= INFO */

        return true;
    }

    /**
     * Reset the complete key value memory.
     * NOTE: This is used for testing and benchmarks of the memory and does
     * not properly reset anything involved in the backup or nameservice
     */
    public void resetMemory() {
        // #if LOGGER >= WARN
        LOGGER.warn("FULL chunk memory reset/wipe...");
        // #endif /* LOGGER >= WARN */

        m_memoryManager.lockManage();
        m_memoryManager.reset();

        // re-init nameservice
        m_nameservice.reinit();

        // don't unlock after the reset because the lock is also reset'd

        // #if LOGGER >= WARN
        LOGGER.warn("Resetting chunk memory finished. Backup and nameservice are NOW BROKEN because they were not involved in this reset process");
        // #endif /* LOGGER >= WARN */
    }

    /**
     * Reset the complete key value memory.
     * NOTE: This is used for testing and benchmarks of the memory and does
     * not properly reset anything involved in the backup or nameservice
     *
     * @param p_nodeId
     *         Remote peer to reset the memory of
     */
    public void resetMemory(final short p_nodeId) {
        // #if LOGGER >= INFO
        LOGGER.info("Resetting remote chunk memory of 0x%X ", p_nodeId);
        // #endif /* LOGGER >= INFO */

        ResetMemoryMessage message = new ResetMemoryMessage(p_nodeId);

        try {
            m_network.sendMessage(message);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending request to reset memory of node 0x%X failed: %s", p_nodeId, e);
            // #endif /* LOGGER >= ERROR */
        }

        // #if LOGGER >= INFO
        LOGGER.info("Triggered async chunk memory reset");
        // #endif /* LOGGER >= INFO */
    }

    @Override
    public void onIncomingMessage(final AbstractMessage p_message) {
        // #if LOGGER == TRACE
        LOGGER.trace("Entering incomingMessage with: p_message=%s", p_message);
        // #endif /* LOGGER == TRACE */

        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case ChunkMessages.SUBTYPE_DUMP_MEMORY_MESSAGE:
                        incomingDumpMemoryMessage((DumpMemoryMessage) p_message);
                        break;
                    case ChunkMessages.SUBTYPE_RESET_MEMORY_MESSAGE:
                        incomingResetMemoryMessage((ResetMemoryMessage) p_message);
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
    protected void resolveComponentDependencies(DXRAMComponentAccessor p_componentAccessor) {
        m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_nameservice = p_componentAccessor.getComponent(NameserviceComponent.class);
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
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_DUMP_MEMORY_MESSAGE, DumpMemoryMessage.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_RESET_MEMORY_MESSAGE, ResetMemoryMessage.class);
    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_DUMP_MEMORY_MESSAGE, this);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_RESET_MEMORY_MESSAGE, this);
    }

    // -----------------------------------------------------------------------------------

    /**
     * Handle incoming dump memory messages
     *
     * @param p_message
     *         Message to handle
     */
    private void incomingDumpMemoryMessage(final DumpMemoryMessage p_message) {
        // #if LOGGER >= INFO
        LOGGER.info("Async dumping chunk memory to %s, (remote req 0x%X)", p_message.getFileName(), p_message.getSource());
        // #endif /* LOGGER >= INFO */

        // don't block message handler, this might take a few seconds depending on the memory size
        new Thread(() -> {
            try {
                m_memoryManager.lockManage();

                // #if LOGGER >= INFO
                LOGGER.info("Dumping chunk memory to %s...", p_message.getFileName());
                // #endif /* LOGGER >= INFO */

                m_memoryManager.dumpMemory(p_message.getFileName());
            } finally {
                m_memoryManager.unlockManage();
            }

            // #if LOGGER >= INFO
            LOGGER.info("Dumping chunk memory to %s, done", p_message.getFileName());
            // #endif /* LOGGER >= INFO */
        }).start();
    }

    /**
     * Handle incoming reset memory messages
     *
     * @param p_message
     *         Message to handle
     */
    private void incomingResetMemoryMessage(final ResetMemoryMessage p_message) {
        // #if LOGGER >= WARN
        LOGGER.warn("Remote memory reset from 0x%X...", p_message.getSource());
        // #endif /* LOGGER >= WARN */

        // don't block message handler, this might take a few seconds depending on the memory size
        new Thread(this::resetMemory).start();
    }
}

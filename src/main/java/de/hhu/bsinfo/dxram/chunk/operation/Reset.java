package de.hhu.bsinfo.dxram.chunk.operation;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.ResetMemoryMessage;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Reset the key-value store memory, i.e. delete everything (for debugging and testing)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Reset extends Operation implements MessageReceiver {
    public Reset(final Class<? extends Service> p_parentService,
            final BootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_RESET_MEMORY_MESSAGE,
                ResetMemoryMessage.class);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_RESET_MEMORY_MESSAGE, this);
    }

    /**
     * Reset the complete key value memory.
     * NOTE: This is used for testing and benchmarks of the memory and does
     * not properly reset anything involved in the backup or nameservice
     */
    public void resetMemory() {
        m_logger.warn("FULL chunk memory reset/wipe (this might take a while)...");

        m_chunk.getMemory().reset();

        // re-init nameservice
        m_nameservice.reinit();

        m_logger.warn("Resetting chunk memory finished. Backup and nameservice are NOW BROKEN because they were " +
                "not involved in this reset process");

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
        m_logger.info("Resetting remote chunk memory of 0x%X ", p_nodeId);

        ResetMemoryMessage message = new ResetMemoryMessage(p_nodeId);

        try {
            m_network.sendMessage(message);
        } catch (final NetworkException e) {
            m_logger.error("Sending request to reset memory of node 0x%X failed: %s", p_nodeId, e);
        }

        m_logger.info("Triggered async chunk memory reset");
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE &&
                p_message.getSubtype() == ChunkMessages.SUBTYPE_RESET_MEMORY_MESSAGE) {
            m_logger.warn("Remote memory reset from 0x%X...", p_message.getSource());

            // don't block message handler, this might take a few seconds depending on the memory size
            new Thread(this::resetMemory).start();
        }
    }
}

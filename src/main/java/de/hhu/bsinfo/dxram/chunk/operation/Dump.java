package de.hhu.bsinfo.dxram.chunk.operation;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.DumpMemoryMessage;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Dump the local key-value memory (for debugging/analysis)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Dump extends Operation implements MessageReceiver {
    public Dump(final Class<? extends Service> p_parentService,
            final BootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_DUMP_MEMORY_MESSAGE,
                DumpMemoryMessage.class);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_DUMP_MEMORY_MESSAGE, this);
    }

    /**
     * Debug the chunk memory to a file
     *
     * @param p_fileName
     *         File to dump memory to
     */
    public void dumpChunkMemory(final String p_fileName) {
        m_logger.info("Dumping chunk memory to %s (this might take a while)...", p_fileName);

        m_chunk.getMemory().dump().dump(p_fileName);

        m_logger.info("Dumping chunk memory to %s, done", p_fileName);
    }

    /**
     * Debug the chunk memory of a remote peer to a file
     *
     * @param p_fileName
     *         File to dump memory to
     * @return True if dumping memory of remote peer successful, false on failure
     */
    public boolean dumpChunkMemory(final String p_fileName, final short p_remoteNodeId) {
        m_logger.info("Dumping remote chunk memory of 0x%X to %s...", p_remoteNodeId, p_fileName);

        DumpMemoryMessage message = new DumpMemoryMessage(p_remoteNodeId, p_fileName);

        try {
            m_network.sendMessage(message);
        } catch (final NetworkException e) {
            m_logger.error("Sending request to dump memory of node 0x%X failed: %s", p_remoteNodeId, e);
            return false;
        }

        m_logger.info("Triggered async chunk memory dump to %s", p_fileName);

        return true;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE &&
                p_message.getSubtype() == ChunkMessages.SUBTYPE_DUMP_MEMORY_MESSAGE) {
            DumpMemoryMessage message = (DumpMemoryMessage) p_message;

            m_logger.info("Async dumping chunk memory to %s, (remote req 0x%X)", message.getFileName(),
                    p_message.getSource());

            // don't block message handler, this might take a few seconds depending on the memory size
            new Thread(() -> {
                m_chunk.getMemory().dump().dump(message.getFileName());
            }).start();

        }
    }
}

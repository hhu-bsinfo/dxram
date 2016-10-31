
package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxgraph.DXGraphMessageTypes;
import de.hhu.bsinfo.dxgraph.data.BFSResult;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Message to send bfs result to other node(s).
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.05.2016
 */
public class BFSResultMessage extends AbstractMessage {
    private BFSResult m_bfsResult;

    /**
     * Creates an instance of BFSResultMessage.
     * This constructor is used when receiving this message.
     */
    public BFSResultMessage() {
        super();
    }

    /**
     * Creates an instance of BFSResultMessage
     * @param p_destination
     *            the destination
     * @param p_bfsResult
     *            Results of a bfs iteration
     */
    public BFSResultMessage(final short p_destination, final BFSResult p_bfsResult) {
        super(p_destination, DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_RESULT_MESSAGE);

        m_bfsResult = p_bfsResult;
    }

    /**
     * Get the bfs result attached to this message.
     * @return BFS result.
     */
    public BFSResult getBFSResult() {
        return m_bfsResult;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);

        exporter.exportObject(m_bfsResult);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);

        m_bfsResult = new BFSResult();
        exporter.importObject(m_bfsResult);
    }

    @Override
    protected final int getPayloadLength() {
        return m_bfsResult.sizeofObject();
    }
}

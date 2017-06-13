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

package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxgraph.DXGraphMessageTypes;
import de.hhu.bsinfo.dxgraph.data.BFSResult;
import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.ethnet.core.AbstractMessage;

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
        ByteBufferImExporter exporter = new ByteBufferImExporter(p_buffer);

        exporter.exportObject(m_bfsResult);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        ByteBufferImExporter exporter = new ByteBufferImExporter(p_buffer);

        m_bfsResult = new BFSResult();
        exporter.importObject(m_bfsResult);
    }

    @Override
    protected final int getPayloadLength() {
        return m_bfsResult.sizeofObject();
    }
}

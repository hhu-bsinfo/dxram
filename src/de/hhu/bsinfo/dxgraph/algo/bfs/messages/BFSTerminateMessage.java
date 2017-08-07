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

import de.hhu.bsinfo.dxgraph.DXGraphMessageTypes;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;

/**
 * Message to determine if BFS has to terminate after the iteration is finished.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.05.2016
 */
public class BFSTerminateMessage extends Message {
    private long m_frontierNextVerices;
    private long m_frontierNextEdges;

    /**
     * Creates an instance of BFSTerminateMessage.
     * This constructor is used when receiving this message.
     */
    public BFSTerminateMessage() {
        super();
    }

    /**
     * Creates an instance of BFSTerminateMessage
     *
     * @param p_destination
     *         the destination
     * @param p_frontierNextVertices
     *         Total number of vertices in the next frontier.
     * @param p_frontierNextEdges
     *         Total number of edges in the next frontier
     */
    public BFSTerminateMessage(final short p_destination, final long p_frontierNextVertices, final long p_frontierNextEdges) {
        super(p_destination, DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_TERMINATE_MESSAGE);

        m_frontierNextVerices = p_frontierNextVertices;
        m_frontierNextEdges = p_frontierNextEdges;
    }

    /**
     * Get the number of vertices in the next frontier of the remote peer.
     *
     * @return Number of vertices in next frontier.
     */
    public long getFrontierNextVertices() {
        return m_frontierNextVerices;
    }

    /**
     * Get the total number of edges of all vertices in the next frontier of the remote peer.
     *
     * @return Total number of edges in the next frontier.
     */
    public long getFrontierNextEdges() {
        return m_frontierNextEdges;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeLong(m_frontierNextVerices);
        p_exporter.writeLong(m_frontierNextEdges);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_frontierNextVerices = p_importer.readLong(m_frontierNextVerices);
        m_frontierNextEdges = p_importer.readLong(m_frontierNextEdges);
    }

    @Override
    protected final int getPayloadLength() {
        return 2 * Long.BYTES;
    }
}

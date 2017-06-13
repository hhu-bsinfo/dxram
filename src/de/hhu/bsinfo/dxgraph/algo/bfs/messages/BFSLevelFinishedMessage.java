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
import de.hhu.bsinfo.net.core.AbstractMessage;

/**
 * Message broadcasted by one bfs peer to all other participating peers when
 * the current peer has finished his iteration.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.05.2016
 */
public class BFSLevelFinishedMessage extends AbstractMessage {

    private int m_token;
    private long m_sentMsgCount;
    private long m_receivedMsgCount;

    /**
     * Creates an instance of BFSLevelFinishedMessage.
     * This constructor is used when receiving this message.
     */
    public BFSLevelFinishedMessage() {
        super();
    }

    /**
     * Creates an instance of BFSLevelFinishedMessage
     * @param p_destination
     *            the destination
     */
    public BFSLevelFinishedMessage(final short p_destination, final int p_token, final long p_sentMsgCount, final long p_receivedMsgCount) {
        super(p_destination, DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE);

        m_token = p_token;
        m_sentMsgCount = p_sentMsgCount;
        m_receivedMsgCount = p_receivedMsgCount;
    }

    public int getToken() {
        return m_token;
    }

    public long getSentMessageCount() {
        return m_sentMsgCount;
    }

    public long getReceivedMessageCount() {
        return m_receivedMsgCount;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_token);
        p_buffer.putLong(m_sentMsgCount);
        p_buffer.putLong(m_receivedMsgCount);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_token = p_buffer.getInt();
        m_sentMsgCount = p_buffer.getLong();
        m_receivedMsgCount = p_buffer.getLong();
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + 2 * Long.BYTES;
    }
}

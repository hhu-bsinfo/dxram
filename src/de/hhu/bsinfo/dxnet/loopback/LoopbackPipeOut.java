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

package de.hhu.bsinfo.dxnet.loopback;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.NodeMap;
import de.hhu.bsinfo.dxnet.core.AbstractFlowControl;
import de.hhu.bsinfo.dxnet.core.AbstractPipeOut;
import de.hhu.bsinfo.dxnet.core.OutgoingRingBuffer;

/**
 * Created by nothaas on 6/9/17.
 */
public class LoopbackPipeOut extends AbstractPipeOut {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LoopbackPipeOut.class.getSimpleName());

    private final LoopbackSendThread m_loopbackSendThread;
    private LoopbackConnection m_connection;

    LoopbackPipeOut(final short p_ownNodeId, final short p_destinationNodeId, final int p_bufferSize, final AbstractFlowControl p_flowControl,
            final OutgoingRingBuffer p_outgoingBuffer, final LoopbackSendThread p_loopbackSendThread, final NodeMap p_nodeMap,
            final LoopbackConnection p_parentConnection) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_outgoingBuffer);

        m_loopbackSendThread = p_loopbackSendThread;
        m_connection = p_parentConnection;

        setConnected(true);
    }

    @Override
    protected boolean isOpen() {
        return true;
    }

    boolean write() throws IOException {
        ByteBuffer buffer;
        buffer = ((LoopbackOutgoingRingBuffer) getOutgoingQueue()).popFront();

        if (buffer == null) {
            return false;
        }

        int length = m_connection.getPipeIn().read(buffer);

        getOutgoingQueue().shiftBack(length);

        return true;
    }

    @Override
    protected void bufferPosted(final int p_size) {
        m_loopbackSendThread.trigger(m_connection);
    }
}

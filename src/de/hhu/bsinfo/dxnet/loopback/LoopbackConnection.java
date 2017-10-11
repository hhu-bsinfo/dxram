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

import de.hhu.bsinfo.dxnet.MessageHandlers;
import de.hhu.bsinfo.dxnet.NodeMap;
import de.hhu.bsinfo.dxnet.core.AbstractConnection;
import de.hhu.bsinfo.dxnet.core.AbstractExporterPool;
import de.hhu.bsinfo.dxnet.core.BufferPool;
import de.hhu.bsinfo.dxnet.core.IncomingBufferQueue;
import de.hhu.bsinfo.dxnet.core.MessageDirectory;
import de.hhu.bsinfo.dxnet.core.MessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.RequestMap;

/**
 * Represents a network connection
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 */
public class LoopbackConnection extends AbstractConnection<LoopbackPipeIn, LoopbackPipeOut> {

    private LoopbackSendThread m_loopbackSendThread;

    LoopbackConnection(final short p_ownNodeId, final short p_destination, final int p_bufferSize, final int p_flowControlWindowSize,
            final float p_flowControlWindowThreshold, final IncomingBufferQueue p_incomingBufferQueue, final MessageHeaderPool p_messageHeaderPool,
            final MessageDirectory p_messageDirectory, final RequestMap p_requestMap, final MessageHandlers p_messageHandlers, final BufferPool p_bufferPool,
            final AbstractExporterPool p_exporterPool, final LoopbackSendThread p_loopbackSendThread, final NodeMap p_nodeMap) {
        super(p_ownNodeId);

        LoopbackFlowControl flowControl =
                new LoopbackFlowControl(p_destination, p_flowControlWindowSize, p_flowControlWindowThreshold, p_loopbackSendThread, this);
        LoopbackOutgoingRingBuffer outgoingBuffer = new LoopbackOutgoingRingBuffer(p_bufferSize, p_exporterPool);
        LoopbackPipeIn pipeIn =
                new LoopbackPipeIn(p_ownNodeId, p_destination, p_messageHeaderPool, flowControl, p_messageDirectory, p_requestMap, p_messageHandlers,
                        p_bufferPool, p_incomingBufferQueue, this);
        LoopbackPipeOut pipeOut =
                new LoopbackPipeOut(p_ownNodeId, p_destination, p_bufferSize, flowControl, outgoingBuffer, p_loopbackSendThread, p_nodeMap, this);

        setPipes(pipeIn, pipeOut);

        m_loopbackSendThread = p_loopbackSendThread;
    }

    @Override
    public void close(boolean p_force) {
        setClosingTimestamp();
    }

    @Override
    public void wakeup() {
        m_loopbackSendThread.trigger(this);
    }

}

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

package de.hhu.bsinfo.net.ib;

public final class JNIIbdxnet {
    public interface SendHandler {
        long getNextDataToSend(final short p_prevNodeIdWritten, final int p_prevDataWrittenLen);
    }

    public interface RecvHandler {
        // note: byte buffer must be allocated as direct buffer
        void receivedBuffer(final short p_sourceNodeId, final long p_bufferHandle, final long p_addr, final int p_length);

        void receivedFlowControlData(final short p_sourceNodeId, final int p_bytes);
    }

    public interface DiscoveryHandler {
        void nodeDiscovered(final short p_nodeId);

        void nodeInvalidated(final short p_nodeId);
    }

    public interface ConnectionHandler {
        void nodeConnected(final short p_nodeId);

        void nodeDisconnected(final short p_nodeId);
    }

    private JNIIbdxnet() {

    }

    public static native boolean init(final short p_ownNodeId, final int p_inBufferSize, final int p_outBufferSize, final int p_recvPoolSizeBytes,
            final int p_maxRecvReqs, final int p_maxSendReqs, final int p_flowControlMaxRecvReqs, final int p_maxNumConnections,
            final SendHandler p_sendHandler, final RecvHandler p_recvHandler, final DiscoveryHandler p_discoveryHandler,
            final ConnectionHandler p_connectionHandler, final boolean p_enableSignalHandler, final boolean p_enableDebugThread);

    public static native boolean shutdown();

    public static native void addNode(final int p_ipv4);

    public static native long getSendBufferAddress(final short p_targetNodeId);

    public static native void returnRecvBuffer(final long p_addr);
}


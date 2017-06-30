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

import java.nio.ByteBuffer;

public final class JNIIbnet {

    public interface Callbacks {
        void nodeDiscovered(final short p_nodeId);

        void nodeInvalidated(final short p_nodeId);

        void nodeConnected(final short p_nodeId);

        void nodeDisconnected(final short p_nodeId);

        // get a buffer for the receive call (from a pool)
        // note: returning byte buffer must be a direct buffer and non null
        ByteBuffer getReceiveBuffer(final int p_size);

        // note: byte buffer must be allocated as direct buffer
        void receivedBuffer(final short p_sourceNodeId, final ByteBuffer p_buffer, final int p_length);

        void receivedFlowControlData(final short p_sourceNodeId, final int p_bytes);
    }

    private JNIIbnet() {

    }

    public static native boolean init(final short p_ownNodeId, final int p_maxRecvReqs, final int p_maxSendReqs, final int p_inOutBufferSize,
            final int p_flowControlMaxRecvReqs, final int p_flowControlMaxSendReqs, final int p_outgoingJobPoolSize, final int p_sendThreads,
            final int p_recvThreads, final int p_maxNumConnections, final Callbacks p_callbacks, final boolean p_enableSignalHandler,
            final boolean p_enableDebugThread);

    public static native boolean shutdown();

    public static native void addNode(final int p_ipv4);

    // note: byte buffer must be allocated as direct buffer
    public static native boolean postBuffer(final short p_nodeId, final ByteBuffer p_buffer, final int p_length);

    public static native boolean postFlowControlData(final short p_nodeId, final int p_bytes);
}


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

package de.hhu.bsinfo.dxnet.ib;

/**
 * Class/interface to access the Ibdxnet C/C++ subsystem
 * (ensure that the respected library is loaded before using this class)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.06.2017
 */
final class JNIIbdxnet {
    /**
     * Interface for callbacks from the subsystem to the java space (send operations)
     */
    public interface SendHandler {
        /**
         * The send thread calls this to get the next data to send
         *
         * @param p_prevNodeIdWritten
         *         Remote node id data was sent to on the previous call
         * @param p_prevDataWrittenLen
         *         Number of bytes written on the previous call
         * @return Unsafe address with data stored as a struct determining the next node and buffer to write
         * (see implementation for details)
         */
        long getNextDataToSend(final short p_prevNodeIdWritten, final int p_prevDataWrittenLen);
    }

    /**
     * Interface for callbacks from the subsystem to the java space (recv operations)
     */
    public interface RecvHandler {
        /**
         * The receive thread calls this to pass buffers with incoming data to the java space
         *
         * @param p_sourceNodeId
         *         Node id of the source (remote sender)
         * @param p_bufferHandle
         *         Handle to identify the buffer (important when returning it)
         * @param p_addr
         *         Unsafe address of the buffer
         * @param p_length
         *         Length of the incoming data
         */
        void receivedBuffer(final short p_sourceNodeId, final long p_bufferHandle, final long p_addr, final int p_length);

        /**
         * The receive thread calls this to pass received flow control data to the java space
         *
         * @param p_sourceNodeId
         *         Node id of the source (remote sender)
         * @param p_bytes
         *         FC bytes received from the remote
         */
        void receivedFlowControlData(final short p_sourceNodeId, final int p_bytes);
    }

    /**
     * Interface for callbacks from the subsystem to the java space (node discovery operations)
     */
    public interface DiscoveryHandler {
        /**
         * Called when a new node was discovered (but not connection established, yet)
         *
         * @param p_nodeId
         *         Node id discovered
         */
        void nodeDiscovered(final short p_nodeId);

        /**
         * Called when a node is invalidated
         *
         * @param p_nodeId
         *         Node id of invalidated node
         */
        void nodeInvalidated(final short p_nodeId);
    }

    /**
     * Interface for callbacks from the subsystem to the java space (node connect operations)
     */
    public interface ConnectionHandler {
        /**
         * Called when a node is connected, i.e. an actual infiniband connection was established
         *
         * @param p_nodeId
         *         Node id of connected node
         */
        void nodeConnected(final short p_nodeId);

        /**
         * Called when a node disconnected, e.g. the infiniband connection is no longer available
         *
         * @param p_nodeId
         *         Node id of disconnected node
         */
        void nodeDisconnected(final short p_nodeId);
    }

    /**
     * Private constructor (static only class)
     */
    private JNIIbdxnet() {

    }

    /**
     * Initialize the underlying subsystem
     *
     * @param p_ownNodeId
     *         Node id of the current node
     * @param p_inBufferSize
     *         Size of a single buffer for incoming data (for pool)
     * @param p_outBufferSize
     *         Size of the outgoing (ring) buffer (per connection)
     * @param p_recvPoolSizeBytes
     *         Total size of the recv pool filled with buffers for
     *         incoming data (shared across all connections)
     * @param p_maxRecvReqs
     *         Max number of WRQs in the (shared) recv queue
     * @param p_maxSendReqs
     *         Max number of WRQs in the (per connection) send queue
     * @param p_flowControlMaxRecvReqs
     *         Max number of WRQs in the (shared) FC recv queue
     * @param p_maxNumConnections
     *         Max number of connections
     * @param p_sendHandler
     *         Reference to a send handler
     * @param p_recvHandler
     *         Reference to a recv handler
     * @param p_discoveryHandler
     *         Reference to a discovery handler
     * @param p_connectionHandler
     *         Reference to a connection handler
     * @param p_enableSignalHandler
     *         True to overwrite java's signal handler with Ibdxnet's. Useful for
     *         debugging segmentation faults in the subsystem
     * @param p_enableDebugThread
     *         True to enable a thread printing additional runtime information periodically
     *         (runtime statistics, current load, throughput) of the subsystem
     * @return True if init was successful, false on failure
     */
    public static native boolean init(final short p_ownNodeId, final int p_inBufferSize, final int p_outBufferSize, final long p_recvPoolSizeBytes,
            final int p_maxRecvReqs, final int p_maxSendReqs, final int p_flowControlMaxRecvReqs, final int p_maxNumConnections,
            final SendHandler p_sendHandler, final RecvHandler p_recvHandler, final DiscoveryHandler p_discoveryHandler,
            final ConnectionHandler p_connectionHandler, final boolean p_enableSignalHandler, final boolean p_enableDebugThread);

    /**
     * Shut down the subsystem
     *
     * @return True on success, false on failure
     */
    public static native boolean shutdown();

    /**
     * Add a node to allow the subsystem to discover it and set up infiniband connections
     *
     * @param p_ipv4
     *         IPV4 of the node
     */
    public static native void addNode(final int p_ipv4);

    /**
     * Get the (unsafe) address for the outgoing ring buffer to use. This allows us to serialize
     * messages directly into the send buffer without further copying
     *
     * @param p_targetNodeId
     *         Node id of the connection
     * @return Unsafe address of the send buffer (size of the buffer as set on init)
     */
    public static native long getSendBufferAddress(final short p_targetNodeId);

    /**
     * Return a used receive buffer after finished processing its contents
     *
     * @param p_addr
     *         Unsafe address of the receive buffer to return
     */
    public static native void returnRecvBuffer(final long p_addr);
}


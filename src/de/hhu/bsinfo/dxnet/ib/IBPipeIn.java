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

import de.hhu.bsinfo.dxnet.MessageHandlers;
import de.hhu.bsinfo.dxnet.core.AbstractFlowControl;
import de.hhu.bsinfo.dxnet.core.AbstractPipeIn;
import de.hhu.bsinfo.dxnet.core.MessageDirectory;
import de.hhu.bsinfo.dxnet.core.RequestMap;

/**
 * Pipe in implementation (remote -> current node write) for IB
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.06.2017
 */
class IBPipeIn extends AbstractPipeIn {

    /**
     * Constructor
     *
     * @param p_ownNodeId
     *         Node id of the current node
     * @param p_destinationNodeId
     *         Node id of the destination this pipe is connected to
     * @param p_flowControl
     *         Flow control instance
     * @param p_messageDirectory
     *         Message directory instance
     * @param p_requestMap
     *         Request map instance
     * @param p_messageHandlers
     *         Message handlers instance
     */
    IBPipeIn(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl, final MessageDirectory p_messageDirectory,
            final RequestMap p_requestMap, final MessageHandlers p_messageHandlers) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_messageDirectory, p_requestMap, p_messageHandlers);
    }

    /**
     * Handle incoming flow control data from the remote connection
     *
     * @param p_confirmedBytes
     *         Bytes confirmed/processed and sent by the remote to use to handle
     */
    void handleFlowControlData(final int p_confirmedBytes) {
        getFlowControl().handleFlowControlData(p_confirmedBytes);
    }

    @Override
    public void returnProcessedBuffer(final Object p_obj, final long p_bufferHandle) {
        // p_obj unused
        JNIIbdxnet.returnRecvBuffer(p_bufferHandle);
    }

    @Override
    public boolean isOpen() {
        return true;
    }
}

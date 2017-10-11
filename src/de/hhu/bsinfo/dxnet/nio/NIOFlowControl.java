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

package de.hhu.bsinfo.dxnet.nio;

import de.hhu.bsinfo.dxnet.core.AbstractFlowControl;
import de.hhu.bsinfo.dxnet.core.NetworkException;

/**
 * Created by nothaas on 6/12/17.
 */
public class NIOFlowControl extends AbstractFlowControl {

    private final NIOSelector m_nioSelector;
    private final ChangeOperationsRequest m_flowControlOperation;

    NIOFlowControl(final short p_destinationNodeId, final int p_flowControlWindowSize, final float p_flowControlWindowThreshold,
            final NIOSelector p_nioSelector, final NIOConnection p_connection) {
        super(p_destinationNodeId, p_flowControlWindowSize, p_flowControlWindowThreshold);

        m_nioSelector = p_nioSelector;
        m_flowControlOperation = new ChangeOperationsRequest(p_connection, NIOSelector.FLOW_CONTROL);
    }

    @Override
    public void flowControlWrite() throws NetworkException {
        m_nioSelector.changeOperationInterestAsync(m_flowControlOperation);
    }
}

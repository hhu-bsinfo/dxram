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

/**
 * Created by nothaas on 6/10/16.
 */
public class PingMessage extends Message {
    /**
     * Creates an instance of VerticesForNextFrontierRequest.
     * This constructor is used when receiving this message.
     */
    public PingMessage() {
        super();
    }

    /**
     * Creates an instance of VerticesForNextFrontierRequest
     *
     * @param p_destination
     *         the destination
     */
    public PingMessage(final short p_destination) {
        super(p_destination, DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_PING_MESSAGE);

    }
}

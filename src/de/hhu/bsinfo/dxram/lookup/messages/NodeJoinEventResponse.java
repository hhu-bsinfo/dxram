/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxnet.core.Response;

/**
 * Response to a NodeJoinEventRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.04.2017
 */
public class NodeJoinEventResponse extends Response {

    // Constructors

    /**
     * Creates an instance of NodeJoinEventResponse
     */
    public NodeJoinEventResponse() {
        super();
    }

    /**
     * Creates an instance of NodeJoinEventResponse
     *
     * @param p_request
     *     the corresponding NodeJoinEventRequest
     */
    public NodeJoinEventResponse(final NodeJoinEventRequest p_request) {
        super(p_request, LookupMessages.SUBTYPE_NODE_JOIN_EVENT_RESPONSE);
    }
}

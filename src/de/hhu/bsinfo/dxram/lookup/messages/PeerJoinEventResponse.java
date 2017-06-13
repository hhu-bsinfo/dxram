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

import de.hhu.bsinfo.net.core.AbstractResponse;

/**
 * Response to a PeerJoinEventRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.04.2017
 */
public class PeerJoinEventResponse extends AbstractResponse {

    // Constructors

    /**
     * Creates an instance of PeerJoinEventResponse
     */
    public PeerJoinEventResponse() {
        super();
    }

    /**
     * Creates an instance of PeerJoinEventResponse
     *
     * @param p_request
     *     the corresponding PeerJoinEventRequest
     */
    public PeerJoinEventResponse(final PeerJoinEventRequest p_request) {
        super(p_request, LookupMessages.SUBTYPE_PEER_JOIN_EVENT_RESPONSE);
    }
}

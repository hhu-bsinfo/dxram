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

package de.hhu.bsinfo.dxram.lock.messages;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request to get a list of locked chunks from another node.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.04.2016
 */
public class GetLockedListRequest extends AbstractRequest {

    /**
     * Creates an instance of GetLockedListRequest as a receiver.
     */
    public GetLockedListRequest() {
        super();
    }

    /**
     * Creates an instance of GetLockedListRequest as a sender
     *
     * @param p_destination
     *     the destination node ID.
     */
    public GetLockedListRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.LOCK_MESSAGES_TYPE, LockMessages.SUBTYPE_GET_LOCKED_LIST_REQUEST);
    }

}

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

package de.hhu.bsinfo.dxcompute.ms.messages;

import de.hhu.bsinfo.dxcompute.DXComputeMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request of the slave to a master to join a compute group.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class SlaveJoinRequest extends AbstractRequest {
    /**
     * Creates an instance of SlaveJoinRequest.
     * This constructor is used when receiving this message.
     */
    public SlaveJoinRequest() {
        super();
    }

    /**
     * Creates an instance of SlaveJoinRequest.
     * This constructor is used when sending this message.
     * @param p_destination
     *            the destination node id.
     */
    public SlaveJoinRequest(final short p_destination) {
        super(p_destination, DXComputeMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_REQUEST);
    }
}

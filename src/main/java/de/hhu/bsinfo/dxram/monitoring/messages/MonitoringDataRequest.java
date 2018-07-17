/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.monitoring.messages;

import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;

/**
 * Monitoring Request message (only used by terminal)
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class MonitoringDataRequest extends Request {
    /**
     * Creates an instance of MonitoringRequest.
     * This constructor is used when receiving this message.
     */
    public MonitoringDataRequest() {
        super();
    }

    /**
     * Creates an instance of MonitoringRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination The destination nid.
     */
    public MonitoringDataRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.MONITORING_MESSAGES_TYPE,
                MonitoringMessages.SUBTYPE_MONITORING_DATA_REQUEST);
    }
}

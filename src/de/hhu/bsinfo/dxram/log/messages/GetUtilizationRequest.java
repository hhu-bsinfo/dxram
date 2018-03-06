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

package de.hhu.bsinfo.dxram.log.messages;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.Request;

/**
 * Get Utilization Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.04.2016
 */
public class GetUtilizationRequest extends Request {

    // Constructors

    /**
     * Creates an instance of GetUtilizationRequest
     */
    public GetUtilizationRequest() {
        super();
    }

    /**
     * Creates an instance of GetUtilizationRequest
     *
     * @param p_destination
     *     the destination
     */
    public GetUtilizationRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_GET_UTILIZATION_REQUEST);
    }

}

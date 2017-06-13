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

package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.ethnet.core.AbstractResponse;

/**
 * Response to free request.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.05.2016
 */
public class BarrierFreeResponse extends AbstractResponse {
    /**
     * Creates an instance of BarrierFreeRequest
     */
    public BarrierFreeResponse() {
        super();
    }

    /**
     * Creates an instance of BarrierFreeRequest
     *
     * @param p_request
     *     The request to respond to
     */
    public BarrierFreeResponse(final BarrierFreeRequest p_request) {
        super(p_request, LookupMessages.SUBTYPE_BARRIER_FREE_RESPONSE);
    }
}

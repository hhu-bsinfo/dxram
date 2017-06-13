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

package de.hhu.bsinfo.dxram.failure.messages;

import de.hhu.bsinfo.ethnet.core.AbstractResponse;

/**
 * Response to a FailureRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.10.2016
 */
public class FailureResponse extends AbstractResponse {

    // Constructors

    /**
     * Creates an instance of FailureResponse
     */
    public FailureResponse() {
        super();
    }

    /**
     * Creates an instance of FailureResponse
     *
     * @param p_request
     *     the corresponding FailureRequest
     */
    public FailureResponse(final FailureRequest p_request) {
        super(p_request, FailureMessages.SUBTYPE_FAILURE_RESPONSE);
    }

}

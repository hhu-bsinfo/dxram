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
 * Response to the put request.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2015
 */
public class SuperpeerStoragePutResponse extends AbstractResponse {
    /**
     * Creates an instance of SuperpeerStoragePutResponse.
     * This constructor is used when receiving this message.
     */
    public SuperpeerStoragePutResponse() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStoragePutResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *     the request
     */
    public SuperpeerStoragePutResponse(final SuperpeerStoragePutRequest p_request) {
        super(p_request, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_RESPONSE);
    }
}

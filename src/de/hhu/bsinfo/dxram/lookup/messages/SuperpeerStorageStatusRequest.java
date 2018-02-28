/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.Request;

/**
 * Requesting the status of the superpeer storage.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 18.05.2015
 */
public class SuperpeerStorageStatusRequest extends Request {
    /**
     * Creates an instance of SuperpeerStorageCreateRequest
     */
    public SuperpeerStorageStatusRequest() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStorageCreateRequest
     *
     * @param p_destination
     *     the destination
     */
    public SuperpeerStorageStatusRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_STATUS_REQUEST);
    }
}

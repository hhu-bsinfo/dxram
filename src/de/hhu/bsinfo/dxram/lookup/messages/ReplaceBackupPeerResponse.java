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

import de.hhu.bsinfo.dxnet.core.Response;

/**
 * Response to a ReplaceBackupPeerRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 21.10.2016
 */
public class ReplaceBackupPeerResponse extends Response {

    // Constructors

    /**
     * Creates an instance of ReplaceBackupPeerResponse
     */
    public ReplaceBackupPeerResponse() {
        super();
    }

    /**
     * Creates an instance of ReplaceBackupPeerResponse
     *
     * @param p_request
     *     the corresponding ReplaceBackupPeerRequest
     */
    public ReplaceBackupPeerResponse(final ReplaceBackupPeerRequest p_request) {
        super(p_request, LookupMessages.SUBTYPE_REPLACE_BACKUP_PEER_RESPONSE);
    }

}

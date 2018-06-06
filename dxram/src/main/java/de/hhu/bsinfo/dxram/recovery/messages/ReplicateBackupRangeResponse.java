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

package de.hhu.bsinfo.dxram.recovery.messages;

import de.hhu.bsinfo.dxnet.core.Response;

/**
 * Response to a ReplicateBackupRangeRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.06.2017
 */
public class ReplicateBackupRangeResponse extends Response {

    // Constructors

    /**
     * Creates an instance of ReplicateBackupRangeResponse
     */
    public ReplicateBackupRangeResponse() {
        super();
    }

    /**
     * Creates an instance of ReplicateBackupRangeResponse
     *
     * @param p_request
     *     the corresponding ReplicateBackupRangeResponse
     */
    public ReplicateBackupRangeResponse(final ReplicateBackupRangeRequest p_request) {
        super(p_request, RecoveryMessages.SUBTYPE_REPLICATE_BACKUP_RANGE_RESPONSE);
    }

}

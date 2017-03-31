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

package de.hhu.bsinfo.dxram.chunk.messages;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request for getting the chunk id ranges of migrated locally stored chunk ids from another node.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class GetMigratedChunkIDRangesRequest extends AbstractRequest {
    /**
     * Creates an instance of GetMigratedChunkIDRangesRequest.
     * This constructor is used when receiving this message.
     */
    public GetMigratedChunkIDRangesRequest() {
        super();
    }

    /**
     * Creates an instance of GetMigratedChunkIDRangesRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *     the destination node id.
     */
    public GetMigratedChunkIDRangesRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_REQUEST);
    }
}

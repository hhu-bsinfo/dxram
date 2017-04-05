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

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Reset the memory on a peer
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2017
 */
public class ResetMemoryMessage extends AbstractMessage {

    /**
     * Creates an instance of ResetMemoryMessage.
     * This constructor is used when receiving this message.
     */
    public ResetMemoryMessage() {
        super();
    }

    /**
     * Creates an instance of ResetMemoryMessage
     *
     * @param p_destination
     *     the destination
     */
    public ResetMemoryMessage(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_RESET_MEMORY_MESSAGE);
    }

    @Override
    protected final int getPayloadLength() {
        return 0;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {

    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {

    }

}

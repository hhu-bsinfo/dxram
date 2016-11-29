/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.net.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * This is a default message which is never processed on the receiver.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.10.2016
 */
public class DefaultMessage extends AbstractMessage {

    /**
     * Creates an instance of DefaultMessage.
     */
    public DefaultMessage() {
        super();
    }

    /**
     * Creates an instance of DefaultMessage
     *
     * @param p_destination
     *     the destination nodeID
     */
    public DefaultMessage(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.DEFAULT_MESSAGES_TYPE, DefaultMessages.SUBTYPE_DEFAULT_MESSAGE);
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

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

package de.hhu.bsinfo.dxram.boot.messages;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Message to trigger a soft reboot of DXRAM
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 02.05.2016
 */
public class RebootMessage extends AbstractMessage {
    /**
     * Creates an instance of RebootMessage.
     * This constructor is used when receiving this message.
     */
    public RebootMessage() {
        super();
    }

    /**
     * Creates an instance of RebootMessage
     *
     * @param p_destination
     *     the destination
     */
    public RebootMessage(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.BOOT_MESSAGES_TYPE, BootMessages.SUBTYPE_REBOOT_MESSAGE);
    }
}

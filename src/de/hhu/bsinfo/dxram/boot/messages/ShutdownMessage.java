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

package de.hhu.bsinfo.dxram.boot.messages;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Message to trigger a soft shutdown of DXRAM
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 02.05.2016
 */
public class ShutdownMessage extends AbstractMessage {
    /**
     * Creates an instance of ShutdownMessage.
     * This constructor is used when receiving this message.
     */
    public ShutdownMessage() {
        super();
    }

    /**
     * Creates an instance of ShutdownMessage
     *
     * @param p_destination
     *     the destination
     * @param p_hardShutdown
     *     True if the whole application running DXRAM has to exit, false for DXRAM only shutdown
     */
    public ShutdownMessage(final short p_destination, final boolean p_hardShutdown) {
        super(p_destination, DXRAMMessageTypes.BOOT_MESSAGES_TYPE, BootMessages.SUBTYPE_SHUTDOWN_MESSAGE);

        setStatusCode((byte) (p_hardShutdown ? 1 : 0));
    }

    /**
     * Check if the shutdown is a hard shutdown (full application).
     *
     * @return True for hard shutdown, false for soft (DXRAM only) shutdown.
     */
    public boolean isHardShutdown() {
        return getStatusCode() > 0;
    }
}

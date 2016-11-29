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

/**
 * Type and list of subtypes for all boot messages
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 02.05.2016
 */
public final class BootMessages {
    public static final byte SUBTYPE_REBOOT_MESSAGE = 1;
    public static final byte SUBTYPE_SHUTDOWN_MESSAGE = 2;

    /**
     * Static class
     */
    private BootMessages() {
    }
}

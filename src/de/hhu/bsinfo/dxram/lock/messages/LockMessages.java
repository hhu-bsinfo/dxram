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

package de.hhu.bsinfo.dxram.lock.messages;

/**
 * Network message types for the lock package
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.01.2016
 */
public final class LockMessages {
    public static final byte SUBTYPE_LOCK_REQUEST = 1;
    public static final byte SUBTYPE_LOCK_RESPONSE = 2;
    public static final byte SUBTYPE_UNLOCK_MESSAGE = 3;
    public static final byte SUBTYPE_GET_LOCKED_LIST_REQUEST = 4;
    public static final byte SUBTYPE_GET_LOCKED_LIST_RESPONSE = 5;

    /**
     * Static class
     */
    private LockMessages() {
    }
}

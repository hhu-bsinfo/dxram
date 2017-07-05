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

package de.hhu.bsinfo.dxram.log.messages;

/**
 * Type and list of subtypes for all log messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 30.03.2016
 */
public final class LogMessages {
    public static final byte SUBTYPE_LOG_MESSAGE = 1;
    public static final byte SUBTYPE_LOG_ANON_MESSAGE = 2;
    public static final byte SUBTYPE_LOG_BUFFER_MESSAGE = 3;
    public static final byte SUBTYPE_REMOVE_MESSAGE = 4;
    public static final byte SUBTYPE_INIT_BACKUP_RANGE_REQUEST = 5;
    public static final byte SUBTYPE_INIT_BACKUP_RANGE_RESPONSE = 6;
    public static final byte SUBTYPE_INIT_RECOVERED_BACKUP_RANGE_REQUEST = 7;
    public static final byte SUBTYPE_INIT_RECOVERED_BACKUP_RANGE_RESPONSE = 8;

    public static final byte SUBTYPE_GET_UTILIZATION_REQUEST = 9;
    public static final byte SUBTYPE_GET_UTILIZATION_RESPONSE = 10;

    /**
     * Hidden constructor
     */
    private LogMessages() {
    }
}

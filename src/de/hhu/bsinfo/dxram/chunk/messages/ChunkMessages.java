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

package de.hhu.bsinfo.dxram.chunk.messages;

/**
 * Type and list of subtypes for all chunk messages
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public final class ChunkMessages {
    public static final byte SUBTYPE_GET_REQUEST = 1;
    public static final byte SUBTYPE_GET_RESPONSE = 2;
    public static final byte SUBTYPE_PUT_REQUEST = 3;
    public static final byte SUBTYPE_PUT_RESPONSE = 4;
    public static final byte SUBTYPE_REMOVE_REQUEST = 5;
    public static final byte SUBTYPE_REMOVE_RESPONSE = 6;
    public static final byte SUBTYPE_CREATE_REQUEST = 7;
    public static final byte SUBTYPE_CREATE_RESPONSE = 8;
    public static final byte SUBTYPE_STATUS_REQUEST = 9;
    public static final byte SUBTYPE_STATUS_RESPONSE = 10;
    public static final byte SUBTYPE_PUT_MESSAGE = 11;
    public static final byte SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST = 12;
    public static final byte SUBTYPE_GET_LOCAL_CHUNKID_RANGES_RESPONSE = 13;
    public static final byte SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_REQUEST = 14;
    public static final byte SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_RESPONSE = 15;

    /**
     * Static class
     */
    private ChunkMessages() {
    }
}

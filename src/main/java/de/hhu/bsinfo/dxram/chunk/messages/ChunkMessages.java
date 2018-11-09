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

package de.hhu.bsinfo.dxram.chunk.messages;

/**
 * Type and list of subtypes for all chunk messages
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public final class ChunkMessages {
    public static final byte SUBTYPE_GET_REQUEST = 1;
    public static final byte SUBTYPE_GET_RESPONSE = 2;
    public static final byte SUBTYPE_GET_MULTI_REQUEST = 3;
    public static final byte SUBTYPE_GET_MULTI_RESPONSE = 4;
    public static final byte SUBTYPE_GET_ANON_REQUEST = 5;
    public static final byte SUBTYPE_GET_ANON_RESPONSE = 6;
    public static final byte SUBTYPE_PUT_REQUEST = 7;
    public static final byte SUBTYPE_PUT_RESPONSE = 8;
    public static final byte SUBTYPE_PUT_ANON_REQUEST = 9;
    public static final byte SUBTYPE_PUT_ANON_RESPONSE = 10;
    public static final byte SUBTYPE_PUT_MESSAGE = 11;
    public static final byte SUBTYPE_PUT_ANON_MESSAGE = 12;
    public static final byte SUBTYPE_REMOVE_MESSAGE = 13;
    public static final byte SUBTYPE_REUSE_ID_MESSAGE = 14;
    public static final byte SUBTYPE_CREATE_REQUEST = 15;
    public static final byte SUBTYPE_CREATE_RESPONSE = 16;
    public static final byte SUBTYPE_STATUS_REQUEST = 17;
    public static final byte SUBTYPE_STATUS_RESPONSE = 18;
    public static final byte SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST = 19;
    public static final byte SUBTYPE_GET_LOCAL_CHUNKID_RANGES_RESPONSE = 20;
    public static final byte SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_REQUEST = 21;
    public static final byte SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_RESPONSE = 22;
    public static final byte SUBTYPE_DUMP_MEMORY_MESSAGE = 23;
    public static final byte SUBTYPE_RESET_MEMORY_MESSAGE = 24;
    public static final byte SUBTYPE_RESIZE_REQUEST = 25;
    public static final byte SUBTYPE_RESIZE_RESPONSE = 26;
    public static final byte SUBTYPE_LOCK_REQUEST = 27;
    public static final byte SUBTYPE_LOCK_RESPONSE = 28;

    /**
     * Static class
     */
    private ChunkMessages() {
    }
}

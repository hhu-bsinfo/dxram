/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
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

package de.hhu.bsinfo.dxram.loader.messages;

/**
 * @author Julien Bernhart, julien.bernhart@hhu.de, 2019-04-17
 */
public class LoaderMessages {
    public static final byte SUBTYPE_CLASS_REQUEST = 1;
    public static final byte SUBTYPE_CLASS_RESPONSE = 2;
    public static final byte SUBTYPE_CLASS_REGISTER = 3;
    public static final byte SUBTYPE_CLASS_DISTRIBUTE = 4;
    public static final byte SUBTYPE_SYNC_REQUEST = 5;
    public static final byte SUBTYPE_SYNC_RESPONSE = 6;
    public static final byte SUBTYPE_SYNC_INVITATION = 7;
    public static final byte SUBTYPE_UPDATE = 8;

    /**
     * Static class
     */
    private LoaderMessages() {
    }
}
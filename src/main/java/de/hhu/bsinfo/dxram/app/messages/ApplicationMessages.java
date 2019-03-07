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

package de.hhu.bsinfo.dxram.app.messages;

/**
 * Different message types used by the application package.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.10.2018
 */
public final class ApplicationMessages {
    public static final byte SUBTYPE_START_APPLICATION_REQUEST = 1;
    public static final byte SUBTYPE_START_APPLICATION_RESPONSE = 2;
    public static final byte SUBTYPE_LIST_APPLICATIONS_REQUEST = 3;
    public static final byte SUBTYPE_LIST_APPLICATIONS_RESPONSE = 4;
    public static final byte SUBTYPE_LIST_ACTIVE_APPLICATIONS_REQUEST = 5;
    public static final byte SUBTYPE_LIST_ACTIVE_APPLICATIONS_RESPONSE = 6;
    public static final byte SUBTYPE_SUBMIT_APPLICATION = 7;

    /**
     * Static class
     */
    private ApplicationMessages() {
    }
}

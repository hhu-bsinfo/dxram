/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.failure.messages;

/**
 * Type and list of subtypes for all failure messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 18.10.2016
 */
public final class FailureMessages {
    public static final byte SUBTYPE_FAILURE_REQUEST = 1;
    public static final byte SUBTYPE_FAILURE_RESPONSE = 2;

    /**
     * Hidden constructor
     */
    private FailureMessages() {
    }
}

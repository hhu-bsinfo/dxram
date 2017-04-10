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

package de.hhu.bsinfo.soh;

/**
 * Runtime memory exception for the small object heap.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public class MemoryRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1547073091176402925L;

    /**
     * Constructor
     *
     * @param p_message
     *     Exception message.
     */
    MemoryRuntimeException(final String p_message) {
        super(p_message);
    }

    /**
     * Constructor
     *
     * @param p_message
     *     Exception message
     * @param p_cause
     *     Other exception causing this
     */
    MemoryRuntimeException(final String p_message, final Throwable p_cause) {
        super(p_message, p_cause);
    }
}

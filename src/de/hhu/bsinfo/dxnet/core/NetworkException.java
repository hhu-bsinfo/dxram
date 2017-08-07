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

package de.hhu.bsinfo.dxnet.core;

/**
 * Exception for failed network accesses
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 */
public class NetworkException extends Exception {

    /**
     * Creates an instance of NetworkException
     *
     * @param p_message
     *         the message
     */
    public NetworkException(final String p_message) {
        super(p_message);
    }

    /**
     * Creates an instance of NetworkException
     *
     * @param p_message
     *         the message
     * @param p_cause
     *         the cause
     */
    public NetworkException(final String p_message, final Throwable p_cause) {
        super(p_message, p_cause);
    }

}

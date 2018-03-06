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

package de.hhu.bsinfo.dxterm;

/**
 * Exception for terminal related errors
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class TerminalException extends Exception {
    /**
     * Constructor
     *
     * @param p_msg
     *         Exception message
     */
    public TerminalException(final String p_msg) {
        super(p_msg);
    }

    /**
     * Constructor
     *
     * @param p_e
     *         Exception to wrap
     */
    public TerminalException(final Exception p_e) {
        super(p_e);
    }

    /**
     * Constructor
     *
     * @param p_msg
     *         Exception message
     * @param p_e
     *         Exception to wrap
     */
    public TerminalException(final String p_msg, final Exception p_e) {
        super(p_msg, p_e);
    }
}

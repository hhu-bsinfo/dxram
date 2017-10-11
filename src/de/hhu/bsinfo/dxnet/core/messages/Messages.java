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

package de.hhu.bsinfo.dxnet.core.messages;

/**
 * Message types reserved for the network subsystem
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.07.2017
 */
public final class Messages {
    public static final byte DEFAULT_MESSAGES_TYPE = 0;

    public static final byte SUBTYPE_INVALID_MESSAGE = 0;
    public static final byte SUBTYPE_DEFAULT_MESSAGE = 1;
    public static final byte SUBTYPE_BENCHMARK_MESSAGE = 2;

    /**
     * Hidden constructor
     */
    private Messages() {
    }
}

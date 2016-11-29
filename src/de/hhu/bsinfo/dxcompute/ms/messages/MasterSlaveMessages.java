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

package de.hhu.bsinfo.dxcompute.ms.messages;

/**
 * Different message types used for the master slave framework
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 18.02.2016
 */
public final class MasterSlaveMessages {
    public static final byte SUBTYPE_SLAVE_JOIN_REQUEST = 1;
    public static final byte SUBTYPE_SLAVE_JOIN_RESPONSE = 2;
    public static final byte SUBTYPE_EXECUTE_TASK_REQUEST = 3;
    public static final byte SUBTYPE_EXECUTE_TASK_RESPONSE = 4;
    public static final byte SUBTYPE_SUBMIT_TASK_REQUEST = 5;
    public static final byte SUBTYPE_SUBMIT_TASK_RESPONSE = 6;
    public static final byte SUBTYPE_GET_MASTER_STATUS_REQUEST = 7;
    public static final byte SUBTYPE_GET_MASTER_STATUS_RESPONSE = 8;
    public static final byte SUBTYPE_TASK_EXECUTION_FINISHED_MESSAGE = 9;
    public static final byte SUBTYPE_TASK_EXECUTION_STARTED_MESSAGE = 10;
    public static final byte SUBTYPE_SIGNAL = 11;

    /**
     * Static class
     */
    private MasterSlaveMessages() {
    }
}

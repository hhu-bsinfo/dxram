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

package de.hhu.bsinfo.dxcompute.ms.tasks;

/**
 * List of task payloads provided by the master slave framework
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public final class MasterSlaveTaskPayloads {
    public static final short TYPE = 0;
    public static final short SUBTYPE_NULL_TASK = 0;
    public static final short SUBTYPE_SLAVE_PRINT_INFO_TASK = 1;
    public static final short SUBTYPE_WAIT_TASK = 2;
    public static final short SUBTYPE_PRINT_TASK = 3;
    public static final short SUBTYPE_PRINT_MEMORY_STATUS_CONSOLE_TASK = 4;
    public static final short SUBTYPE_PRINT_MEMORY_STATUS_FILE_TASK = 5;
    public static final short SUBTYPE_PRINT_STATISTICS_CONSOLE_TASK = 6;
    public static final short SUBTYPE_PRINT_STATISTICS_FILE_TASK = 7;

    /**
     * Static class
     */
    private MasterSlaveTaskPayloads() {
    }
}

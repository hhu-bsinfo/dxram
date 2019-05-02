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

package de.hhu.bsinfo.dxram;

/**
 * Type and list of types for all dxram messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 19.10.2016
 */
public final class DXRAMMessageTypes {
    // 0 is reserved in dxnet subsystem
    public static final byte BOOT_MESSAGES_TYPE = 1;
    public static final byte LOOKUP_MESSAGES_TYPE = 2;
    public static final byte CHUNK_MESSAGES_TYPE = 3;
    public static final byte MIGRATION_MESSAGES_TYPE = 4;
    public static final byte LOG_MESSAGES_TYPE = 5;
    public static final byte FAILURE_MESSAGES_TYPE = 6;
    public static final byte RECOVERY_MESSAGES_TYPE = 7;
    public static final byte LOGGER_MESSAGES_TYPE = 8;
    public static final byte JOB_MESSAGES_TYPE = 9;
    public static final byte MASTERSLAVE_MESSAGES_TYPE = 10;
    public static final byte NETWORK_MESSAGES_TYPE = 11;
    public static final byte MONITORING_MESSAGES_TYPE = 12;
    public static final byte APPLICATION_MESSAGE_TYPE = 13;
    public static final byte FUNCTION_MESSAGE_TYPE = 14;
    public static final byte LOADER_MESSAGE_TYPE = 15;

    /**
     * Static class
     */
    private DXRAMMessageTypes() {
    }
}

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
 * Default ordering for component initialization and shutdown for the DXRAM engine
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public final class DXRAMComponentOrder {

    /**
     * Static class
     */
    private DXRAMComponentOrder() {

    }

    /**
     * Init order of components
     */
    public static final class Init {
        public static final short NULL = 1;
        public static final short LOGGER = 2;
        public static final short EVENT = 3;
        public static final short BOOT = 4;
        public static final short NETWORK = 5;
        public static final short LOOKUP = 7;
        public static final short FAILURE = 8;
        public static final short CHUNK = 9;
        public static final short LOG = 10;
        public static final short BACKUP = 11;
        public static final short CHUNK_INDEX = 12;
        public static final short NAMESERVICE = 13;
        public static final short JOB = 14;
        public static final short MONITORING = 15;
        public static final short APPLICATION = 16;

        /**
         * Static class
         */
        private Init() {

        }
    }

    /**
     * Shutdown order of components
     */
    public static final class Shutdown {
        public static final short BOOT = Short.MIN_VALUE;
        public static final short APPLICATION = -15;
        public static final short MONITORING = -14;
        public static final short JOB = -13;
        public static final short NAMESERVICE = -12;
        public static final short CHUNK_INDEX = -11;
        public static final short BACKUP = -10;
        public static final short LOG = -9;
        public static final short CHUNK = -8;
        public static final short FAILURE = -7;
        public static final short LOOKUP = -6;
        public static final short NETWORK = -4;
        public static final short EVENT = -3;
        public static final short LOGGER = -2;
        public static final short NULL = -1;

        /**
         * Static class
         */
        private Shutdown() {

        }
    }
}

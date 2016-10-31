package de.hhu.bsinfo.dxram;

/**
 * Default ordering for component initialization and shutdown for the DXRAM engine
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public final class DXRAMComponentOrder {

    /**
     * Init order of components
     */
    public final class Init {
        public static final short NULL = 1;
        public static final short LOGGER = 2;
        public static final short EVENT = 3;
        public static final short MEMORY = 4;
        public static final short BOOT = 5;
        public static final short NETWORK = 6;
        public static final short LOOKUP = 7;
        public static final short FAILURE = 8;
        public static final short PEER_LOCK = 9;
        public static final short LOG = 10;
        public static final short BACKUP = 11;
        public static final short CHUNK = 12;
        public static final short NAMESERVICE = 13;
        public static final short SCRIPT = 14;
        public static final short TERMINAL = 15;

        /**
         * Static class
         */
        private Init() {

        }
    }

    /**
     * Shutdown order of components
     */
    public final class Shutdown {
        public static final short BOOT = Short.MIN_VALUE;
        public static final short TERMINAL = -14;
        public static final short SCRIPT = -13;
        public static final short NAMESERVICE = -12;
        public static final short CHUNK = -11;
        public static final short BACKUP = -10;
        public static final short LOG = -9;
        public static final short PEER_LOCK = -8;
        public static final short FAILURE = -7;
        public static final short LOOKUP = -6;
        public static final short NETWORK = -5;
        public static final short MEMORY = -4;
        public static final short EVENT = -3;
        public static final short LOGGER = -2;
        public static final short NULL = -1;

        /**
         * Static class
         */
        private Shutdown() {

        }
    }

    /**
     * Static class
     */
    private DXRAMComponentOrder() {

    }
}

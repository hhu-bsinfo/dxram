package de.hhu.bsinfo.dxram;

/**
 * Default ordering for component initialization and shutdown for the DXRAM engine
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public final class DXRAMComponentOrder {

    /**
     * Init order of components
     */
    public final class Init {
        public static final short NULL = 1;
        public static final short LOGGER = 2;
        public static final short EVENT = 3;
        public static final short STATISTICS = 4;
        public static final short MEMORY = 5;
        public static final short BOOT = 6;
        public static final short NETWORK = 7;
        public static final short LOOKUP = 8;
        public static final short FAILURE = 9;
        public static final short PEER_LOCK = 10;
        public static final short LOG = 11;
        public static final short BACKUP = 12;
        public static final short CHUNK = 13;
        public static final short NAMESERVICE = 14;
        public static final short SCRIPT = 15;
        public static final short TERMINAL = 16;

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
        public static final short TERMINAL = -16;
        public static final short SCRIPT = -15;
        public static final short NAMESERVICE = -14;
        public static final short CHUNK = -13;
        public static final short BACKUP = -12;
        public static final short LOG = -11;
        public static final short PEER_LOCK = -10;
        public static final short FAILURE = -9;
        public static final short LOOKUP = -8;
        public static final short NETWORK = -7;
        public static final short MEMORY = -5;
        public static final short STATISTICS = -4;
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

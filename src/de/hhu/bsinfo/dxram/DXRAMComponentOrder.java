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
        public static final int NULL = 1;
        public static final int LOGGER = 2;
        public static final int EVENT = 3;
        public static final int STATISTICS = 4;
        public static final int MEMORY = 5;
        public static final int BOOT = 6;
        public static final int NETWORK = 7;
        public static final int LOOKUP = 8;
        public static final int FAILURE = 9;
        public static final int PEER_LOCK = 10;
        public static final int LOG = 11;
        public static final int BACKUP = 12;
        public static final int CHUNK = 13;
        public static final int NAMESERVICE = 14;
        public static final int SCRIPT = 15;
        public static final int TERMINAL = 16;

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
        public static final int BOOT = -1000;
        public static final int TERMINAL = -16;
        public static final int SCRIPT = -15;
        public static final int NAMESERVICE = -14;
        public static final int CHUNK = -13;
        public static final int BACKUP = -12;
        public static final int LOG = -11;
        public static final int PEER_LOCK = -10;
        public static final int FAILURE = -9;
        public static final int LOOKUP = -8;
        public static final int NETWORK = -7;
        public static final int MEMORY = -5;
        public static final int STATISTICS = -4;
        public static final int EVENT = -3;
        public static final int LOGGER = -2;
        public static final int NULL = -1;

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

package de.hhu.bsinfo.dxram;

/**
 * Default ordering for component initialization and shutdown for the engine
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
        public static final int FAILURE = 10;
        public static final int PEER_LOCK = 11;
        public static final int LOG = 12;
        public static final int BACKUP = 13;
        public static final int CHUNK = 14;
        public static final int NAMESERVICE = 15;
        public static final int SCRIPT = 17;
        public static final int TERMINAL = 18;

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
        public static final int BOOT = 50;
        public static final int TERMINAL = 82;
        public static final int SCRIPT = 83;
        public static final int NAMESERVICE = 85;
        public static final int CHUNK = 86;
        public static final int BACKUP = 87;
        public static final int LOG = 88;
        public static final int PEER_LOCK = 89;
        public static final int FAILURE = 90;
        public static final int LOOKUP = 92;
        public static final int NETWORK = 93;
        public static final int MEMORY = 95;
        public static final int STATISTICS = 96;
        public static final int EVENT = 97;
        public static final int LOGGER = 98;
        public static final int NULL = 99;

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

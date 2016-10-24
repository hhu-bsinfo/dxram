package de.hhu.bsinfo.dxcompute;

/**
 * Created by rubbinnexx on 10/24/16.
 */
public class DXComputeComponentOrder {
    /**
     * Init order of components
     */
    public final class Init {
        public static final short JOB_WORK_STEALING = 100;

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
        public static final short JOB_WORK_STEALING = -100;


        /**
         * Static class
         */
        private Shutdown() {

        }
    }


    /**
     * Static class
     */
    private DXComputeComponentOrder() {

    }
}

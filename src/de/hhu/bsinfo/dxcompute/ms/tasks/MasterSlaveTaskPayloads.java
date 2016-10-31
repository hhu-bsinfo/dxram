
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

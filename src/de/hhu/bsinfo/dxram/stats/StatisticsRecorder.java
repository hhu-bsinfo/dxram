package de.hhu.bsinfo.dxram.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Records statistics for a number of operations. Create one recorder
 * for a subset of operations, for example: category memory management,
 * operations alloc, free, ...
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public class StatisticsRecorder {

    private String m_recorderName;

    private Lock m_mapLock = new ReentrantLock(false);
    private Map<String, StatisticsOperation> m_operations = new HashMap<>();

    /**
     * Constructor
     *
     * @param p_recorderName
     *         Recorder name
     */
    public StatisticsRecorder(final String p_recorderName) {
        m_recorderName = p_recorderName;
    }

    /**
     * Get the name of the recorder.
     *
     * @return Name of the recorder.
     */
    public String getName() {
        return m_recorderName;
    }

    /**
     * Get an operation from the recorder (non existing operations are created on demand)
     *
     * @param p_name
     *         Name of the operation
     * @return StatisticsOperation
     */
    public StatisticsOperation getOperation(final String p_name) {

        StatisticsOperation operation = m_operations.get(p_name);
        if (operation == null) {
            m_mapLock.lock();
            operation = m_operations.get(p_name);
            if (operation == null) {
                operation = new StatisticsOperation(p_name);
                m_operations.put(p_name, operation);
            }
            m_mapLock.unlock();
        }

        return operation;
    }

    /**
     * Enable/disable all operations
     *
     * @param p_val
     *         True to enable, false to disable
     */
    public void setAllOperationsEnabled(final boolean p_val) {
        for (StatisticsOperation op : m_operations.values()) {
            op.setEnabled(p_val);
        }
    }

    @Override public String toString() {
        String str = "[" + m_recorderName + "]";
        for (StatisticsOperation op : m_operations.values()) {
            str += "\n\t" + op;
        }
        return str;
    }
}

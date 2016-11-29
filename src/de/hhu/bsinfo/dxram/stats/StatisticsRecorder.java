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
     *     Recorder name
     */
    StatisticsRecorder(final String p_recorderName) {
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
     * Enable/disable all operations
     *
     * @param p_val
     *     True to enable, false to disable
     */
    public void setAllOperationsEnabled(final boolean p_val) {
        for (StatisticsOperation op : m_operations.values()) {
            op.setEnabled(p_val);
        }
    }

    /**
     * Reset all operations
     */
    public void reset() {
        for (StatisticsOperation op : m_operations.values()) {
            op.reset();
        }
    }

    @Override
    public String toString() {
        String str = '[' + m_recorderName + ']';
        for (StatisticsOperation op : m_operations.values()) {
            str += "\n\t" + op;
        }
        return str;
    }

    /**
     * Get an operation from the recorder (non existing operations are created on demand)
     *
     * @param p_name
     *     Name of the operation
     * @return StatisticsOperation
     */
    StatisticsOperation getOperation(final String p_name) {

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
}

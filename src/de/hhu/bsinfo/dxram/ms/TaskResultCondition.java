/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.ms;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Condition to allow execution of a task script taking different branches
 * depending on the return code of the previous task
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 15.01.2017
 */
final class TaskResultCondition implements TaskScriptNode {

    private static final Map<String, ConditionFunction> CONDITIONS = new HashMap<>();

    static {
        CONDITIONS.put("==", (v1, v2) -> v1 == v2);
        CONDITIONS.put("!=", (v1, v2) -> v1 != v2);
        CONDITIONS.put("<", (v1, v2) -> v1 < v2);
        CONDITIONS.put(">", (v1, v2) -> v1 > v2);
        CONDITIONS.put("<=", (v1, v2) -> v1 <= v2);
        CONDITIONS.put(">=", (v1, v2) -> v1 >= v2);
    }

    @FunctionalInterface
    interface ConditionFunction {
        boolean evaluate(int p_a, int p_b);
    }

    @Expose
    private String m_cond = "";
    @Expose
    private int m_param = 0;
    @Expose
    private TaskScript m_true = new TaskScript();
    @Expose
    private TaskScript m_false = new TaskScript();

    /**
     * Default constructor
     */
    public TaskResultCondition() {

    }

    /**
     * Evaluate the condition based on the previous task's return code
     *
     * @param p_prevTaskReturnCode
     *         Return code of the previous task
     * @return TaskScript which is the result of the evaluated condition for further execution
     */
    TaskScript evaluate(final int p_prevTaskReturnCode) {
        ConditionFunction func = CONDITIONS.get(m_cond);
        if (func == null) {
            return new TaskScript();
        }

        if (func.evaluate(p_prevTaskReturnCode, m_param)) {
            return m_true;
        } else {
            return m_false;
        }
    }

    @Override
    public String toString() {
        return "TaskResultCondition[res " + m_cond + ' ' + m_param + "] ? " + m_true.getTasks().length + " : " + m_false.getTasks().length;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_cond);
        p_exporter.writeInt(m_param);

        p_exporter.exportObject(m_true);
        p_exporter.exportObject(m_false);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_cond = p_importer.readString(m_cond);
        m_param = p_importer.readInt(m_param);

        if (m_true == null) {
            m_true = new TaskScript();
        }
        p_importer.importObject(m_true);

        if (m_false == null) {
            m_false = new TaskScript();
        }
        p_importer.importObject(m_false);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_cond) + Integer.BYTES + m_true.sizeofObject() + m_false.sizeofObject();
    }
}

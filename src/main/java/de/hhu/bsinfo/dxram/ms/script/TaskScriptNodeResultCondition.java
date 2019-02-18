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

package de.hhu.bsinfo.dxram.ms.script;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Condition to allow execution of a task script taking different branches
 * depending on the return code of the previous task.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 15.01.2017
 */
public final class TaskScriptNodeResultCondition implements TaskScriptNode {
    public static final String CONDITION_EQUALS = "==";
    public static final String CONDITION_NOT_EQUALS = "!=";
    public static final String CONDITION_LESS = "<";
    public static final String CONDITION_GREATER = ">";
    public static final String CONDITION_LESS_EQUALS = "<=";
    public static final String CONDITION_GREATER_EQUALS = ">=";

    private static final Map<String, ConditionFunction> CONDITIONS = new HashMap<>();

    static {
        CONDITIONS.put(CONDITION_EQUALS, (v1, v2) -> v1 == v2);
        CONDITIONS.put(CONDITION_NOT_EQUALS, (v1, v2) -> v1 != v2);
        CONDITIONS.put(CONDITION_LESS, (v1, v2) -> v1 < v2);
        CONDITIONS.put(CONDITION_GREATER, (v1, v2) -> v1 > v2);
        CONDITIONS.put(CONDITION_LESS_EQUALS, (v1, v2) -> v1 <= v2);
        CONDITIONS.put(CONDITION_GREATER_EQUALS, (v1, v2) -> v1 >= v2);
    }

    @Expose
    private String m_cond = "";
    @Expose
    private int m_param;
    @Expose
    private TaskScript m_true = new TaskScript();
    @Expose
    private TaskScript m_false = new TaskScript();

    /**
     * Default constructor.
     */
    public TaskScriptNodeResultCondition() {

    }

    /**
     * Constructor.
     *
     * @param p_condition
     *         Condition to evaluate.
     * @param p_param
     *         Parameter to compare result to.
     * @param p_true
     *         Script to execute if condition is true.
     * @param p_false
     *         Script to execute if condition is false.
     */
    public TaskScriptNodeResultCondition(final String p_condition, final int p_param, final TaskScript p_true,
            final TaskScript p_false) {
        m_cond = p_condition;
        m_param = p_param;
        m_true = p_true;
        m_false = p_false;
    }

    /**
     * Get the task script associated with the true case.
     *
     * @return TaskScript
     */
    public TaskScript getScriptTrueCase() {
        return m_true;
    }

    /**
     * Get the task script associated with the false case.
     *
     * @return TaskScript
     */
    public TaskScript getScriptFalseCase() {
        return m_false;
    }

    /**
     * Evaluate the condition based on the previous task's return code.
     *
     * @param p_prevTaskReturnCode
     *         Return code of the previous task
     * @return TaskScript which is the result of the evaluated condition for further execution
     */
    public TaskScript evaluate(final int p_prevTaskReturnCode) {
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
        return "TaskResultCondition[res " + m_cond + ' ' + m_param + "] ? " + m_true.getTasks().length + " : " +
                m_false.getTasks().length;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(m_cond);
        p_exporter.writeInt(m_param);

        p_exporter.exportObject(m_true);
        p_exporter.exportObject(m_false);
    }

    @Override
    public void importObject(final Importer p_importer) {
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

    @FunctionalInterface
    interface ConditionFunction {
        /**
         * Evaluate the condition.
         *
         * @param p_a
         *         First input parameter.
         * @param p_b
         *         Second input parameter.
         * @return Result of the condition.
         */
        boolean evaluate(int p_a, int p_b);
    }
}

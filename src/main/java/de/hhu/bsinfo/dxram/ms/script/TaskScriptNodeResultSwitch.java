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

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.ms.tasks.EmptyTask;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Switch state to allow execution of a task script taking different paths
 * depending on the return code of the previous task.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 15.01.2017
 */
public final class TaskScriptNodeResultSwitch implements TaskScriptNode {
    @Expose
    private Case[] m_switchCases = new Case[0];
    @Expose
    private Case m_switchCaseDefault = new Case();

    private int m_tmpLen;

    /**
     * Default constructor.
     */
    public TaskScriptNodeResultSwitch() {

    }

    /**
     * Constructor.
     *
     * @param p_default
     *         Default case to execute if no other case matches.
     * @param p_cases
     *         Cases of the switch structure.
     */
    public TaskScriptNodeResultSwitch(final Case p_default, final Case... p_cases) {
        m_switchCases = p_cases;
        m_switchCaseDefault = p_default;
    }

    /**
     * Get the non-default switch cases.
     *
     * @return Cases
     */
    public Case[] getSwitchCases() {
        return m_switchCases;
    }

    /**
     * Get the default case.
     *
     * @return Case
     */
    public Case getDefaultSwitchCase() {
        return m_switchCaseDefault;
    }

    /**
     * Evaluate the switch state based on the previous task's return code.
     *
     * @param p_prevTaskReturnCode
     *         Return code of the previous task
     * @return TaskScript which is the result of the evaluated switch state for further execution
     */
    public TaskScript evaluate(final int p_prevTaskReturnCode) {
        for (Case esac : m_switchCases) {
            if (esac.getCaseValue() == p_prevTaskReturnCode) {
                return esac.getCase();
            }
        }

        return m_switchCaseDefault.getCase();
    }

    @Override
    public String toString() {
        return "TaskScriptNodeResultSwitch[" + m_switchCases.length + " cases]";
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_switchCases.length);

        for (Case esac : m_switchCases) {
            p_exporter.exportObject(esac);
        }

        p_exporter.exportObject(m_switchCaseDefault);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_tmpLen = p_importer.readInt(m_tmpLen);

        if (m_switchCases.length == 0) {
            m_switchCases = new Case[m_tmpLen];
        }

        for (int i = 0; i < m_switchCases.length; i++) {
            if (m_switchCases[i] == null) {
                m_switchCases[i] = new Case();
            }

            p_importer.importObject(m_switchCases[i]);
        }

        p_importer.importObject(m_switchCaseDefault);
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        size += Integer.BYTES;

        for (Case esac : m_switchCases) {
            size += esac.sizeofObject();
        }

        size += m_switchCaseDefault.sizeofObject();

        return size;
    }

    /**
     * Class for a switch case
     */
    public static class Case implements Importable, Exportable {
        @Expose
        private int m_caseValue = 0;
        @Expose
        private TaskScript m_case = new TaskScript(new EmptyTask(0));

        /**
         * Default constructor
         */
        Case() {

        }

        /**
         * Get the task script associated with the case.
         *
         * @return TaskScript
         */
        public TaskScript getScriptCase() {
            return m_case;
        }

        /**
         * Get the case value
         *
         * @return Case value
         */
        int getCaseValue() {
            return m_caseValue;
        }

        /**
         * Get the task script attached to this case
         *
         * @return TaskScript
         */
        TaskScript getCase() {
            return m_case;
        }

        @Override
        public void exportObject(final Exporter p_exporter) {
            p_exporter.writeInt(m_caseValue);
            p_exporter.exportObject(m_case);
        }

        @Override
        public void importObject(final Importer p_importer) {
            m_caseValue = p_importer.readInt(m_caseValue);

            if (m_case.getTasks()[0] instanceof EmptyTask) {
                m_case = new TaskScript();
            }

            p_importer.importObject(m_case);
        }

        @Override
        public int sizeofObject() {
            return Integer.BYTES + m_case.sizeofObject();
        }
    }
}

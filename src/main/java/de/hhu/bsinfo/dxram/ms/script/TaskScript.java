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

import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * A task script, containing a sequence of tasks, can be submitted to a master-slave compute group.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public final class TaskScript implements Importable, Exportable {
    public static final int NUM_SLAVES_ARBITRARY = 0;

    @Expose
    private int m_minSlaves = NUM_SLAVES_ARBITRARY;
    @Expose
    private int m_maxSlaves = NUM_SLAVES_ARBITRARY;
    @Expose
    private String m_name = "";
    @Expose
    private TaskScriptNode[] m_tasks = new TaskScriptNode[0];

    private int m_tmpLen;

    /**
     * Constructor.
     * Empty task script.
     */
    public TaskScript() {

    }

    /**
     * Constructor.
     *
     * @param p_tasks
     *         List of tasks forming the script
     */
    public TaskScript(final TaskScriptNode... p_tasks) {
        m_tasks = p_tasks;
    }

    /**
     * Constructor.
     *
     * @param p_minSlaves
     *         Minimum number of slaves required to run this task script
     * @param p_maxSlaves
     *         Max number of slaves for this task script
     * @param p_tasks
     *         List of tasks forming the script
     */
    public TaskScript(final short p_minSlaves, final short p_maxSlaves, final TaskScriptNode... p_tasks) {
        m_minSlaves = p_minSlaves;
        m_maxSlaves = p_maxSlaves;
        m_tasks = p_tasks;
    }

    /**
     * Constructor.
     *
     * @param p_minSlaves
     *         Minimum number of slaves required to run this task script
     * @param p_maxSlaves
     *         Max number of slaves for this task script
     * @param p_name
     *         Name for the task script (for debugging only)
     * @param p_tasks
     *         List of tasks forming the script
     */
    public TaskScript(final short p_minSlaves, final short p_maxSlaves, final String p_name,
            final TaskScriptNode... p_tasks) {
        m_minSlaves = p_minSlaves;
        m_maxSlaves = p_maxSlaves;
        m_name = p_name;
        m_tasks = p_tasks;
    }

    /**
     * Get the minimum number of slaves required to run this task script.
     *
     * @return Minimum number of slaves this task script requires.
     */
    public int getMinSlaves() {
        return m_minSlaves;
    }

    /**
     * Get the maximum number of slaves for this task script.
     *
     * @return Maximum number of slaves for this task script.
     */
    public int getMaxSlaves() {
        return m_maxSlaves;
    }

    /**
     * Get the tasks of this task script.
     *
     * @return List of tasks
     */
    public TaskScriptNode[] getTasks() {
        return m_tasks;
    }

    @Override
    public String toString() {
        return "TaskScript[" + m_name + ", " + m_minSlaves + ", " + m_maxSlaves + ", " + m_tasks.length + ']';
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_minSlaves);
        p_exporter.writeInt(m_maxSlaves);
        p_exporter.writeString(m_name);
        p_exporter.writeInt(m_tasks.length);

        // export tasks as TaskScriptNodeData manually to not depend on reflection when importing them in importObject
        for (int i = 0; i < m_tasks.length; i++) {
            // string
            p_exporter.writeString(m_tasks[i].getClass().getName());

            // as "byte array"
            p_exporter.writeCompactNumber(m_tasks[i].sizeofObject());
            p_exporter.exportObject(m_tasks[i]);
        }
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_minSlaves = p_importer.readInt(m_minSlaves);
        m_maxSlaves = p_importer.readInt(m_maxSlaves);
        m_name = p_importer.readString(m_name);

        m_tmpLen = p_importer.readInt(m_tmpLen);

        if (m_tasks.length == 0) {
            m_tasks = new TaskScriptNode[m_tmpLen];
        }

        // import as TaskScriptNodeData and use reflection to create proper objects later (not possible here)
        for (int i = 0; i < m_tasks.length; i++) {
            if (m_tasks[i] == null) {
                m_tasks[i] = new TaskScriptNodeData();
            }

            p_importer.importObject(m_tasks[i]);
        }
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        size += 2 * Integer.BYTES + ObjectSizeUtil.sizeofString(m_name) + Integer.BYTES;

        for (int i = 0; i < m_tasks.length; i++) {
            if (m_tasks[i] instanceof TaskScriptNodeData) {
                size += m_tasks[i].sizeofObject();
            } else {
                int objSize = m_tasks[i].sizeofObject();

                size += ObjectSizeUtil.sizeofString(m_tasks[i].getClass().getName()) +
                        ObjectSizeUtil.sizeofCompactedNumber(objSize) + objSize;
            }
        }

        return size;
    }
}

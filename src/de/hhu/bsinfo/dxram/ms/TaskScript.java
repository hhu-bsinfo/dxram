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

import java.lang.reflect.InvocationTargetException;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * A task script, containing a sequence of tasks, can be submitted to a master-slave compute group.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public final class TaskScript implements Importable, Exportable {
    static final int NUM_SLAVES_ARBITRARY = 0;

    @Expose
    private int m_minSlaves = NUM_SLAVES_ARBITRARY;
    @Expose
    private int m_maxSlaves = NUM_SLAVES_ARBITRARY;
    @Expose
    private String m_name = "";
    @Expose
    private TaskScriptNode[] m_tasks = new TaskScriptNode[0];

    /**
     * Constructor
     * Empty task script
     */
    public TaskScript() {

    }

    /**
     * Constructor
     *
     * @param p_tasks
     *     List of tasks forming the script
     */
    public TaskScript(final TaskScriptNode... p_tasks) {
        m_tasks = p_tasks;
    }

    /**
     * Constructor
     *
     * @param p_minSlaves
     *     Minimum number of slaves required to run this task script
     * @param p_maxSlaves
     *     Max number of slaves for this task script
     * @param p_tasks
     *     List of tasks forming the script
     */
    public TaskScript(final short p_minSlaves, final short p_maxSlaves, final TaskScriptNode... p_tasks) {
        m_minSlaves = p_minSlaves;
        m_maxSlaves = p_maxSlaves;
        m_tasks = p_tasks;
    }

    /**
     * Constructor
     *
     * @param p_minSlaves
     *     Minimum number of slaves required to run this task script
     * @param p_maxSlaves
     *     Max number of slaves for this task script
     * @param p_name
     *     Name for the task script (for debugging only)
     * @param p_tasks
     *     List of tasks forming the script
     */
    public TaskScript(final short p_minSlaves, final short p_maxSlaves, final String p_name, final TaskScriptNode... p_tasks) {
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

        for (int i = 0; i < m_tasks.length; i++) {
            p_exporter.writeString(m_tasks[i].getClass().getName());
            p_exporter.exportObject(m_tasks[i]);
        }
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_minSlaves = p_importer.readInt();
        m_maxSlaves = p_importer.readInt();
        m_name = p_importer.readString();
        m_tasks = new TaskScriptNode[p_importer.readInt()];

        for (int i = 0; i < m_tasks.length; i++) {
            String taskName = p_importer.readString();

            Class<?> clazz;
            try {
                clazz = Class.forName(taskName);
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException("Cannot find task class " + taskName);
            }

            try {
                m_tasks[i] = (TaskScriptNode) clazz.getConstructor().newInstance();
            } catch (final NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new RuntimeException("Cannot create instance of Task, maybe missing default constructor", e);
            }

            p_importer.importObject(m_tasks[i]);
        }
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        size += 2 * Integer.BYTES + ObjectSizeUtil.sizeofString(m_name) + Integer.BYTES;

        for (int i = 0; i < m_tasks.length; i++) {
            size += ObjectSizeUtil.sizeofString(m_tasks[i].getClass().getName()) + m_tasks[i].sizeofObject();
        }

        return size;
    }
}

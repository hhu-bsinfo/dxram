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

package de.hhu.bsinfo.dxram.ms.tasks;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Empty task for testing for filling up as a no-op
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class EmptyTask implements Task {

    @Expose
    private int m_result = 0;

    /**
     * Constructor
     */
    public EmptyTask() {

    }

    /**
     * Constructor
     *
     * @param p_result
     *         Return code for this task
     */
    public EmptyTask(final int p_result) {
        m_result = p_result;
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        return m_result;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_result);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_result = p_importer.readInt(m_result);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES;
    }
}

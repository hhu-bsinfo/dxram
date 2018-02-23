/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Empty task that returns a random return value within the specified range
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.01.2017
 */
public class RandomReturnValueTask implements Task {

    @Expose
    private int m_begin = 0;
    @Expose
    private int m_end = 0;

    /**
     * Constructor
     */
    public RandomReturnValueTask() {

    }

    /**
     * Constructor
     *
     * @param p_begin
     *         Begin of the random range (including)
     * @param p_end
     *         End of the random range (including)
     */
    public RandomReturnValueTask(final int p_begin, final int p_end) {
        m_begin = p_begin;
        m_end = p_end;
    }

    @Override
    public int execute(TaskContext p_ctx) {
        return (int) (Math.random() * (m_end - m_begin + 1) + m_begin);
    }

    @Override
    public void handleSignal(Signal p_signal) {
        // nothing to handle
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_begin);
        p_exporter.writeInt(m_end);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_begin = p_importer.readInt(m_begin);
        m_end = p_importer.readInt(m_end);
    }

    @Override
    public int sizeofObject() {
        return 2 * Integer.BYTES;
    }
}

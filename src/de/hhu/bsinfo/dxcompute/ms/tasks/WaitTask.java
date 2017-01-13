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

package de.hhu.bsinfo.dxcompute.ms.tasks;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Wait for specified amount of time.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class WaitTask implements Task {

    @Expose
    private int m_waitMs = 0;

    /**
     * Constructor
     */
    public WaitTask() {

    }

    /**
     * Constructor
     *
     * @param p_timeMs
     *     Amount of time to wait in ms.
     */
    public WaitTask(final int p_timeMs) {
        m_waitMs = p_timeMs;
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        try {
            Thread.sleep(m_waitMs);
        } catch (final InterruptedException e) {
            return -1;
        }

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_waitMs);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_waitMs = p_importer.readInt();
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES;
    }
}

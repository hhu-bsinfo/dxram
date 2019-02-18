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

package de.hhu.bsinfo.dxram.ms.tasks;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Print a message to the console.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class PrintTask implements Task {

    @Expose
    private String m_msg = "";

    /**
     * Constructor
     */
    public PrintTask() {

    }

    /**
     * Constructor
     *
     * @param p_msg
     *         Message to print
     */
    public PrintTask(final String p_msg) {
        m_msg = p_msg;
    }

    @Override
    public int execute(final TaskContext p_ctx) {
        System.out.println(m_msg);
        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(m_msg);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_msg = p_importer.readString(m_msg);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_msg);
    }
}

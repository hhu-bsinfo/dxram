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

import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Dummy task which is used to add comments/visual separators
 * to a task script.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.01.2017
 */
public class DummyTask implements Task {

    /**
     * Constructor
     */
    public DummyTask() {

    }

    @Override
    public int execute(final TaskContext p_ctx) {
        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
    }

    @Override
    public void exportObject(final Exporter p_exporter) {

    }

    @Override
    public void importObject(final Importer p_importer) {

    }

    @Override
    public int sizeofObject() {
        return 0;
    }
}

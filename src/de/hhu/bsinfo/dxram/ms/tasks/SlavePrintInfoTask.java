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

import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Print information about the current slave to the console.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class SlavePrintInfoTask implements Task {

    /**
     * Constructor
     */
    public SlavePrintInfoTask() {

    }

    @Override
    public int execute(final TaskContext p_ctx) {

        System.out.println("TaskScript " + getClass().getSimpleName() + ": ");
        System.out.println("OwnSlaveId: " + p_ctx.getCtxData().getSlaveId());
        System.out.println("List of slaves in current compute group " + p_ctx.getCtxData().getComputeGroupId() + ": ");
        short[] slaves = p_ctx.getCtxData().getSlaveNodeIds();
        for (int i = 0; i < slaves.length; i++) {
            System.out.println(i + ": " + NodeID.toHexString(slaves[i]));
        }

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        // ignore signals
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        // nothing to export
    }

    @Override
    public void importObject(final Importer p_importer) {
        // nothing to import
    }

    @Override
    public int sizeofObject() {
        return 0;
    }
}
